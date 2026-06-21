package com.ems.uetrace.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Entity
@Table(name = "plan_ue_trace")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PlanUETrace implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Integer id;

    @Column(name = "name")
    private String name;

    @Column(name = "number_ue")
    private Integer numberUE;

    @Column(name = "ues")
    private String ues;

    @Column(name = "mode")
    private String mode;

    @Column(name = "status")
    private String status;

    @Column(name = "start_time", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date startTime;

    @Column(name = "end_time", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date endTime;

    @Column(name = "create_by")
    private String createBy;

    @Column(name = "mode_run")
    private Integer modeRun;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "result_start")
    private String resultStart;

    @Column(name = "result_stop")
    private String resultStop;

    @Column(name = "is_delete")
    private Integer del;
}