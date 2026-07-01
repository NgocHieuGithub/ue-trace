package com.ems.uetrace.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "stream.kafka")
public class KafkaProducerProperties {
    private String bootstrapServers = "localhost:9092";
    private String topic4ga = "topic_4g";
    private String topic5ga = "topic_5g";
    private String acks = "1";
    private int batchSize = 65536;
    private int lingerMs = 5;
    private long bufferMemory = 67108864L;
    private int maxBlockMs = 0;
    private int deliveryTimeoutMs = 15000;

    private String schemaRegistryUrl = "http://localhost:8081";
    private int inflightLimit = 100;
}
