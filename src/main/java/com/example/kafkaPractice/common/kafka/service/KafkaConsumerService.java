package com.example.kafkaPractice.common.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final String TOPIC_NAME = "test-topic";
    private static final String GROUP_ID = "my-group";

    @KafkaListener(topics = TOPIC_NAME, groupId = GROUP_ID)
    public void listen(String message) {
        System.out.println("Received message: " + message);
    }
}
