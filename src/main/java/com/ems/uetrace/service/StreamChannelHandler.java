package com.ems.uetrace.service;

import com.ems.uetrace.contains.SystemType;
import com.ems.uetrace.model.StreamRecord;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Service
public class StreamChannelHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private AvroSerializer avroSerializer;

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {

    }

    private void sendRecord(ChannelHandlerContext ctx, StreamRecord record, SystemType system) {
        byte[] avroBytes;
        try {
            avroBytes = avroSerializer.serialize(record, system);
        } catch (Exception e) {
            log.error("[STREAM] Avro serialize error: {}", e.getMessage());
            return;
        }

        int current = inflight.incrementAndGet();
        checkBackpressureOn(ctx, current);

        boolean queued = kafkaProducer.sendBytes(system, String.valueOf(record.getNeId()), avroBytes, (meta, ex) -> {
            if (ex != null) {
                metrics.kafkaSentFail.incrementAndGet();
                log.error("[KAFKA] Send failed (system={} ne={} : {}", system, record.getNeId(), ex.getMessage());
            } else {
                metrics.kafkaSentOk.incrementAndGet();
            }
            checkBackpressureOff(ctx, inflight.decrementAndGet());
        });

        if (!queued) {
            metrics.kafkaSentFail.incrementAndGet();
            log.warn("[KAFKA] Send not queued (buffer full?) system={} neId={}", system, record.getNeId());
            forceBackpressureOn(ctx, inflight.decrementAndGet());
        }
    }

    private void checkBackpressureOn(ChannelHandlerContext ctx, int current) {
        if (current >= inflightLimit) {
            ctx.channel().eventLoop().execute(() -> {
                if (ctx.channel().config().isAutoRead()) {
                    ctx.channel().config().setAutoRead(false);
                    metrics.backpressureOn.incrementAndGet();
                    log.debug("[BP] ON inflight={} ch={}", current, ctx.channel().remoteAddress());
                }
            });
        }
    }

    private void checkBackpressureOff(ChannelHandlerContext ctx, int remaining) {
        if (remaining < inflightLimit / 2) {
            ctx.channel().eventLoop().execute(() -> {
                if (!ctx.channel().config().isAutoRead()) {
                    ctx.channel().config().setAutoRead(true);
                    log.debug("[BP] OFF inflight={} ch={}", remaining, ctx.channel().remoteAddress());
                }
            });
        }
    }

    private void forceBackpressureOn(ChannelHandlerContext ctx, int current) {
        ctx.channel().eventLoop().execute(() -> {
            if (ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(false);
                metrics.backpressureOn.incrementAndGet();
                log.debug("[BP] ON (Kafka buffer exhausted) inflight={} ch={}", current, ctx.channel().remoteAddress());
            }
            if (recoveryScheduled.compareAndSet(false, true)){
                ctx.channel().eventLoop().schedule(() -> {
                    recoveryScheduled.set(false);
                    int now = inflight.get();
                    if (now < inflightLimit / 2 && !ctx.channel().config().isAutoRead()){
                        ctx.channel().config().setAutoRead(true);
                        log.debug("[BP] OFF (recovery time) inflight={} ch={}", now, ctx.channel().remoteAddress());
                    }
                }, 200, TimeUnit.MILLISECONDS);
            }
        });
    }

}
