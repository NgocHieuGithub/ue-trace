package com.ems.uetrace.repository;

import com.ems.uetrace.model.PlanUETrace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface PlanUETraceRepository extends JpaRepository<PlanUETrace, Integer>, JpaSpecificationExecutor<PlanUETrace> {
    boolean existsByNameIgnoreCaseAndIdNot(String name, Integer excludeId);
    boolean existsByNameIgnoreCase(String name);
    long countByModeIgnoreCaseAndStatusIgnoreCase(String mode, String status);
}
