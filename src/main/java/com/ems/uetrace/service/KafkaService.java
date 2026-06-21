package com.ems.uetrace.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaService {

    @KafkaListener(topics = "test", groupId = "#{@kafkaGroupId}"
    )
    public void listen(String message) {
        System.out.println(message);
    }
}
