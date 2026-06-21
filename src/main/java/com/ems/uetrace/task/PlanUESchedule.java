package com.ems.uetrace.task;

import com.ems.uetrace.model.PlanUETrace;
import com.ems.uetrace.repository.PlanUETraceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class PlanUESchedule {
//
//    private final LeaderElectionService  leaderElectionService;
//    private final PlanUETraceRepository repository;
//    private final UETraceApiClient       ueTraceApiClient;
//    private final TaskScheduler          taskScheduler;
//    private final ObjectMapper objectMapper;
//
//    // Track các plan đã được schedule để tránh tạo lại
//    private final Set<Integer> scheduledPlanIds = ConcurrentHashMap.newKeySet();
//
//    @Scheduled(fixedDelay = 30_000)
//    public void scanSchedulePlan() {
//        log.info("___________________________ Start polling plan ___________________________");
//        if (!leaderElectionService.isLeader()) {
//            log.debug("___________________________ Current instance is not leader, end polling plan ___________________________");
//            return;
//        }
//
//        try {
//            // Lấy các plan INIT chưa có traceId và chưa được schedule
//            List<PlanUETrace> plans = repository.findAll().stream()
//                    .filter(p -> "INIT".equalsIgnoreCase(p.getStatus())
//                            && p.getTraceId() == null
//                            && !scheduledPlanIds.contains(p.getId()))
//                    .collect(Collectors.toList());
//
//            log.info("Found {} plan(s) to schedule", plans.size());
//
//            for (PlanUETrace plan : plans) {
//                try {
//                    processPlan(plan);
//                } catch (Exception e) {
//                    log.error("Failed to process plan id={}: {}", plan.getId(), e.getMessage(), e);
//                }
//            }
//        } catch (Exception e) {
//            log.error("Error during scanSchedulePlan: {}", e.getMessage(), e);
//        }
//
//        log.info("___________________________ End polling plan ___________________________");
//    }
//
//    // ── Xử lý 1 plan ────────────────────────────────────────────
//
//    private void processPlan(PlanUETrace plan) throws Exception {
//        // 1. Tạo traceId cho mỗi UE
//        Map<String, String> traceIdMap = generateTraceIds(plan);
//
//        // 2. Lưu traceId vào DB
//        plan.setTraceId(objectMapper.writeValueAsString(traceIdMap));
//        repository.save(plan);
//
//        // 3. Schedule start
//        scheduleStart(plan, traceIdMap);
//
//        // 4. Schedule stop
//        scheduleStop(plan, traceIdMap);
//
//        // 5. Đánh dấu đã schedule
//        scheduledPlanIds.add(plan.getId());
//
//        log.info("Scheduled plan id={} name={} | start={} stop={}",
//                plan.getId(), plan.getName(), plan.getStartTime(), plan.getEndTime());
//    }
//
//    // ── Generate traceId unique cho từng UE ─────────────────────
//
//    private Map<String, String> generateTraceIds(PlanUETrace plan) {
//        Map<String, String> traceIdMap = new LinkedHashMap<>();
//        List<String> ues = parseUes(plan.getUes());
//        for (String ue : ues) {
//            // Format: planId_ue_timestamp_uuid → đảm bảo unique
//            String traceId = plan.getId() + "_"
//                    + ue.replaceAll("\\s+", "") + "_"
//                    + System.currentTimeMillis() + "_"
//                    + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
//            traceIdMap.put(ue, traceId);
//        }
//        return traceIdMap;
//    }
//
//    // ── Schedule START ───────────────────────────────────────────
//
//    private void scheduleStart(PlanUETrace plan, Map<String, String> traceIdMap) {
//        Date now       = new Date();
//        Date startTime = plan.getStartTime();
//
//        if (startTime == null) {
//            log.warn("Plan id={} has no startTime, skip schedule start", plan.getId());
//            return;
//        }
//
//        // Nếu startTime đã qua → chạy ngay
//        Date triggerTime = startTime.before(now) ? now : startTime;
//        long delayMs     = triggerTime.getTime() - now.getTime();
//
//        taskScheduler.schedule(
//                () -> executeStart(plan.getId(), traceIdMap),
//                new Date(System.currentTimeMillis() + Math.max(delayMs, 0))
//        );
//
//        log.info("Scheduled START for plan id={} at {} (delay={}ms)",
//                plan.getId(), triggerTime, delayMs);
//    }
//
//    // ── Schedule STOP ────────────────────────────────────────────
//
//    private void scheduleStop(PlanUETrace plan, Map<String, String> traceIdMap) {
//        Date now     = new Date();
//        Date endTime = plan.getEndTime();
//
//        if (endTime == null) {
//            log.warn("Plan id={} has no endTime, skip schedule stop", plan.getId());
//            return;
//        }
//
//        if (endTime.before(now)) {
//            log.warn("Plan id={} endTime already passed, skip schedule stop", plan.getId());
//            return;
//        }
//
//        long delayMs = endTime.getTime() - now.getTime();
//
//        taskScheduler.schedule(
//                () -> executeStop(plan.getId(), traceIdMap),
//                new Date(System.currentTimeMillis() + delayMs)
//        );
//
//        log.info("Scheduled STOP for plan id={} at {} (delay={}ms)",
//                plan.getId(), endTime, delayMs);
//    }
//
//    // ── Execute START ────────────────────────────────────────────
//
//    private void executeStart(Integer planId, Map<String, String> traceIdMap) {
//        log.info("Executing START for plan id={}", planId);
//        try {
//            PlanUETrace plan = repository.findById(planId).orElse(null);
//            if (plan == null) {
//                log.warn("Plan id={} not found when executing start", planId);
//                return;
//            }
//
//            // Call API start cho từng UE
//            boolean allSuccess = true;
//            for (Map.Entry<String, String> entry : traceIdMap.entrySet()) {
//                String ue      = entry.getKey();
//                String traceId = entry.getValue();
//                try {
//                    ueTraceApiClient.startTrace(ue, traceId, plan.getMode(), plan.getModeRun());
//                    log.info("Started trace UE={} traceId={}", ue, traceId);
//                } catch (Exception e) {
//                    allSuccess = false;
//                    log.error("Failed to start trace UE={} traceId={}: {}", ue, traceId, e.getMessage());
//                }
//            }
//
//            // Cập nhật status RUNNING
//            plan.setStatus("RUNNING");
//            repository.save(plan);
//            log.info("Plan id={} status updated to RUNNING (allSuccess={})", planId, allSuccess);
//
//        } catch (Exception e) {
//            log.error("Error executing START for plan id={}: {}", planId, e.getMessage(), e);
//        }
//    }
//
//    // ── Execute STOP ─────────────────────────────────────────────
//
//    private void executeStop(Integer planId, Map<String, String> traceIdMap) {
//        log.info("Executing STOP for plan id={}", planId);
//        try {
//            PlanUETrace plan = repository.findById(planId).orElse(null);
//            if (plan == null) {
//                log.warn("Plan id={} not found when executing stop", planId);
//                return;
//            }
//
//            // Call API stop cho từng UE
//            boolean allSuccess = true;
//            for (Map.Entry<String, String> entry : traceIdMap.entrySet()) {
//                String ue      = entry.getKey();
//                String traceId = entry.getValue();
//                try {
//                    ueTraceApiClient.stopTrace(ue, traceId, plan.getMode());
//                    log.info("Stopped trace UE={} traceId={}", ue, traceId);
//                } catch (Exception e) {
//                    allSuccess = false;
//                    log.error("Failed to stop trace UE={} traceId={}: {}", ue, traceId, e.getMessage());
//                }
//            }
//
//            // Cập nhật status FINISH
//            plan.setStatus("FINISH");
//            repository.save(plan);
//            log.info("Plan id={} status updated to FINISH (allSuccess={})", planId, allSuccess);
//
//        } catch (Exception e) {
//            log.error("Error executing STOP for plan id={}: {}", planId, e.getMessage(), e);
//        }
//    }
//
//    // ── Helper ───────────────────────────────────────────────────
//
//    private List<String> parseUes(String ues) {
//        if (!StringUtils.hasText(ues)) return Collections.emptyList();
//        return Arrays.stream(ues.split(","))
//                .map(String::trim)
//                .filter(StringUtils::hasText)
//                .collect(Collectors.toList());
//    }
}