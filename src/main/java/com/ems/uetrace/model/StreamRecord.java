package com.ems.uetrace.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class StreamRecord {
    private long neId;
    private long recordTime;
    private int duration;
    private String location;
    private long cellIndex;

    private Map<String, String> extraFields;
    private Map<String, Long> metrics;
    private Map<String, Double> kpis;

}
