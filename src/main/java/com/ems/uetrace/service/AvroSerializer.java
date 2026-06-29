package com.ems.uetrace.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AvroSerializer {
    private static final ThreadLocal<ByteArrayOutputStream> TL_BAOS =
            ThreadLocal.withInitial(() -> new ByteArrayOutputStream(512));

    private static final ThreadLocal<BinaryEncoder[]> TL_ENCODER =
            ThreadLocal.withInitial(() -> new BinaryEncoder[1]);

    private static final class SystemCtx {
        final Schema schema;
        final GenericDatumWriter<GenericRecord> writer;
        final ThreadLocal<GenericRecord> tlRecord;
        final Map<String, Integer> fieldIndex;

        final int[] extraFieldPositions;        // nullable-int extra fields → reset to null
        final int[] requiredStringPositions;    // required-string extra fields → reset to ""
        final Set<Integer> nullableIntPositions;
        final Set<Integer> requiredIntPositions;
        final int[] counterFieldPositions;      // reset to 0L before each record
        final int[] kpiFieldPositions;          // reset to 0.0 before each record (required double)

        final String subject;
        volatile int schemaId = -1;

        SystemCtx(Schema schema,
                  String subject,
                  Map<String, Integer> fieldIndex,
                  int[] extraFieldPositions,
                  int[] requiredStringPositions,
                  Set<Integer> nullableIntPositions,
                  Set<Integer> requiredIntPositions,
                  int[] counterFieldPositions,
                  int[] kpiFieldPositions) {

            this.schema = schema;
            this.subject = subject;
            this.writer = new GenericDatumWriter<>(schema);
            this.tlRecord = ThreadLocal.withInitial(() -> new GenericData.Record(schema));
            this.fieldIndex = fieldIndex;
            this.extraFieldPositions = extraFieldPositions;
            this.requiredStringPositions = requiredStringPositions;
            this.nullableIntPositions = nullableIntPositions;
            this.requiredIntPositions = requiredIntPositions;
            this.counterFieldPositions = counterFieldPositions;
            this.kpiFieldPositions = kpiFieldPositions;
        }
    }

    private volatile Map<SystemType, SystemCtx> ctxBySystem;
    // Parsed from comma-separated config; tried left-to-right for HA cluster.
    private final List<String> schemaRegistryUrls;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public AvroStreamSerializer(KafkaProducerProperties props, NeConfig neConfig) {
        this.schemaRegistryUrls = Arrays.stream(props.getSchemaRegistryUrl().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        Map<SystemType, SystemCtx> m = new EnumMap<>(SystemType.class);

        this.ctxBySystem = Collections.unmodifiableMap(m);
        log.info("[AVRO] Initialized schemas for systems: {}", ctxBySystem.keySet());
    }

    public synchronized void reloadSystem(SystemType system,
                                          List<NeConfig.ExtraFieldDef> extraDefs,
                                          List<NeConfig.CounterDef> counterDefs,
                                          List<NeConfig.KpiDef> kpiDefs,
                                          String subject) {

        SystemCtx ctx = buildCtx(extraDefs, counterDefs, kpiDefs, subject);
        Map<SystemType, SystemCtx> next = new EnumMap<>(ctxBySystem);
        next.put(system, ctx);
        ctxBySystem = Collections.unmodifiableMap(next);

        try {
            doRegister(ctx);
        } catch (Exception e) {
            log.warn("[AVRO] Schema Registry registration failed after reload for {} ({}): {} - will retry on serialize",
                    system, subject, e.getMessage());
        }
    }

    private static SystemCtx buildCtx(List<NeConfig.ExtraFieldDef> extraDefs,
                                      List<NeConfig.CounterDef> counterDefs,
                                      List<NeConfig.KpiDef> kpiDefs,
                                      String subject) {

        Schema schema = buildSchema(extraDefs, counterDefs, kpiDefs);

        Map<String, Integer> idx = new HashMap<>(schema.getFields().size() * 2);
        for (Schema.Field f : schema.getFields()) {
            idx.put(f.name(), f.pos());
        }

        Map<String, Integer> fieldIndex = Collections.unmodifiableMap(idx);

        Set<String> baseNames = Set.of(
                "ne_id", "record_time", "duration", "location", "cell_index"
        );

        // Nullable-int extra fields → reset to null
        int[] extraPos = extraDefs.stream()
                .filter(d -> !baseNames.contains(d.getColumnName()))
                .filter(d -> "int".equalsIgnoreCase(d.getColumnType()) && d.isNullable())
                .map(d -> idx.get(d.getColumnName()))
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .toArray();

        // Required-string extra fields → reset to ""
        int[] requiredStringPos = extraDefs.stream()
                .filter(d -> !baseNames.contains(d.getColumnName()))
                .filter(d -> !"int".equalsIgnoreCase(d.getColumnType()))
                .map(d -> idx.get(d.getColumnName()))
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .toArray();

        Set<Integer> nullableIntPos = new HashSet<>();
        Set<Integer> requiredIntPos = new HashSet<>();

        for (NeConfig.ExtraFieldDef def : extraDefs) {
            if ("int".equalsIgnoreCase(def.getColumnType())) {
                Integer pos = idx.get(def.getColumnName());
                if (pos != null) {
                    if (def.isNullable()) {
                        nullableIntPos.add(pos);
                    } else {
                        requiredIntPos.add(pos);
                    }
                }
            }
        }

        int[] counterPos = counterDefs.stream()
                .map(d -> idx.get(d.resolvedColumn()))
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .toArray();

        int[] kpiPos = kpiDefs.stream()
                .map(d -> idx.get(d.resolvedColumn()))
                .filter(Objects::nonNull)
                .mapToInt(Integer::intValue)
                .toArray();

        log.info("[AVRO] subject={} fields={}",
                subject,
                schema.getFields().stream()
                        .map(Schema.Field::name)
                        .collect(Collectors.toList()));

        return new SystemCtx(
                schema,
                subject,
                fieldIndex,
                extraPos,
                requiredStringPos,
                nullableIntPos,
                requiredIntPos,
                counterPos,
                kpiPos
        );
    }

    private static Schema buildSchema(List<NeConfig.ExtraFieldDef> extraDefs,
                                      List<NeConfig.CounterDef> counterDefs,
                                      List<NeConfig.KpiDef> kpiDefs) {

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("StreamRecord")
                .namespace("com.viettel.ems.mspstream")
                .fields()
                .requiredLong("ne_id")
                .requiredLong("record_time")
                .requiredInt("duration")
                .requiredString("location")
                .requiredInt("cell_index");

        Set<String> seen = new LinkedHashSet<>(
                Set.of("ne_id", "record_time", "duration", "location", "cell_index")
        );

        for (NeConfig.ExtraFieldDef def : extraDefs) {
            if (!seen.add(def.getColumnName())) {
                continue;
            }

            boolean isInt = "int".equalsIgnoreCase(def.getColumnType());

            if (isInt) {
                if (def.isNullable()) {
                    fields.optionalInt(def.getColumnName());
                } else {
                    fields.requiredInt(def.getColumnName());
                }
            } else {
                fields.name(def.getColumnName())
                        .type()
                        .stringType()
                        .stringDefault("");
            }
        }

        Set<String> counterSeen = new LinkedHashSet<>();
        for (NeConfig.CounterDef def : counterDefs) {
            String col = def.resolvedColumn();
            if (counterSeen.add(col)) {
                fields.name(col)
                        .type()
                        .longType()
                        .longDefault(0L);
            }
        }

        Set<String> kpiSeen = new LinkedHashSet<>();
        for (NeConfig.KpiDef def : kpiDefs) {
            String col = def.resolvedColumn();
            if (kpiSeen.add(col)) {
                fields.optionalDouble(col);
            }
        }

        return fields.endRecord();
    }

    @PostConstruct
    public void registerSchema() {
        ctxBySystem.forEach((system, ctx) -> {
            try {
                doRegister(ctx);
            } catch (Exception e) {
                log.warn("[AVRO] Schema Registry not reachable for {} ({}): {} - will retry on first serialize",
                        system, schemaRegistryUrls, e.getMessage());
            }
        });
    }

    private void doRegister(SystemCtx ctx) throws Exception {
        String schemaPayload = jsonMapper.writeValueAsString(
                Map.of("schema", ctx.schema.toString()));

        Exception lastEx = null;
        for (String baseUrl : schemaRegistryUrls) {
            try {
                // Set BACKWARD compatibility so SR accepts new nullable/defaulted fields
                // and rejects genuinely breaking changes. Idempotent on restart.
                setCompatibility(baseUrl, ctx.subject, "BACKWARD");

                postSchema(ctx, baseUrl + "/subjects/" + ctx.subject + "/versions", schemaPayload);

                if (ctx.schemaId != -1) return;

                throw new IllegalStateException(
                        "Schema Registry did not return a schemaId");
            } catch (Exception e) {
                log.warn("[AVRO] Schema Registry {} failed: {}",
                        baseUrl, e.getMessage());
                lastEx = e;
            }
        }

        throw new IllegalStateException(
                "All Schema Registry nodes failed", lastEx);
    }

    private int postSchema(SystemCtx ctx, String url, String body) throws Exception {
        HttpURLConnection conn =
                (HttpURLConnection) URI.create(url).toURL().openConnection();

        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);
        conn.setRequestMethod("POST");
        conn.setRequestProperty(
                "Content-Type",
                "application/vnd.schemaregistry.v1+json");

        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();

        if (status == 200 || status == 201) {
            byte[] resp = conn.getInputStream().readAllBytes();

            ctx.schemaId = jsonMapper.readTree(resp).get("id").asInt();

            log.info(
                    "[AVRO] Schema registered: subject={} schemaId={}",
                    ctx.subject,
                    ctx.schemaId);
        }

        return status;
    }

    private void setCompatibility(String baseUrl,
                                  String subject,
                                  String level) throws Exception {

        String body = jsonMapper.writeValueAsString(
                Map.of("compatibility", level));

        HttpURLConnection conn =
                (HttpURLConnection)
                        URI.create(baseUrl + "/config/" + subject)
                                .toURL()
                                .openConnection();

        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);

        conn.setRequestMethod("PUT");
        conn.setRequestProperty(
                "Content-Type",
                "application/vnd.schemaregistry.v1+json");

        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        conn.getResponseCode();
        conn.disconnect();
    }
    // --- Serialization ---------------------------------------------------------

    public byte[] serialize(StreamRecord record, SystemType system)
            throws IOException {

        SystemCtx ctx = ctxBySystem.get(system);

        if (ctx == null)
            throw new IOException("No Avro schema for system: " + system);

        if (ctx.schemaId == -1) {
            try {
                doRegister(ctx);
            } catch (Exception e) {
                throw new IOException(
                        "Schema Registry unavailable: " + e.getMessage(), e);
            }
        }

        ByteArrayOutputStream baos = TL_BAOS.get();
        baos.reset();

        baos.write(0x00);

        int id = ctx.schemaId;
        baos.write((id >>> 24) & 0xFF);
        baos.write((id >>> 16) & 0xFF);
        baos.write((id >>> 8) & 0xFF);
        baos.write(id & 0xFF);

        BinaryEncoder[] enc = TL_ENCODER.get();
        enc[0] = EncoderFactory.get().binaryEncoder(baos, enc[0]);

        GenericRecord avroRecord = ctx.tlRecord.get();

        fillGeneric(avroRecord, ctx, record);

        ctx.writer.write(avroRecord, enc[0]);
        enc[0].flush();

        return baos.toByteArray();
    }

    private void fillGeneric(GenericRecord rec,
                             SystemCtx ctx,
                             StreamRecord r) {

        rec.put(0, r.getNetId());
        rec.put(1, r.getRecordTime());
        rec.put(2, r.getDuration());
        rec.put(3, r.getLocation());
        rec.put(4, r.getCellIndex());

        for (int pos : ctx.extraFieldPositions)
            rec.put(pos, null);          // nullable-int only

        for (int pos : ctx.requiredStringPositions)
            rec.put(pos, "");            // reset required strings to ""

        for (int pos : ctx.counterFieldPositions)
            rec.put(pos, 0L);

        for (int pos : ctx.kpiFieldPositions)
            rec.put(pos, null);

        if (r.getExtraFields() != null) {
            r.getExtraFields().forEach((String k, String v) -> {

                Integer pos = ctx.fieldIndex.get(k);

                if (pos == null)
                    return;

                if (ctx.nullableIntPositions.contains(pos)) {
                    try {
                        rec.put(pos, Integer.parseInt(v));
                    } catch (NumberFormatException ignored) {
                        /* leave null */
                    }

                } else if (ctx.requiredIntPositions.contains(pos)) {

                    rec.put(pos, Integer.parseInt(v));
                    // propagates -> caught by sendRecord -> record dropped

                } else {
                    rec.put(pos, v);
                }
            });
        }

        if (r.getMetrics() != null) {
            r.getMetrics().forEach((String k, Long v) -> {
                Integer pos = ctx.fieldIndex.get(k);
                if (pos != null)
                    rec.put(pos, v);
            });
        }

        if (r.getKpis() != null) {
            r.getKpis().forEach((String k, Double v) -> {
                Integer pos = ctx.fieldIndex.get(k);
                if (pos != null)
                    rec.put(pos, v);   // null v is valid for optionalDouble
            });
        }
    }
}
