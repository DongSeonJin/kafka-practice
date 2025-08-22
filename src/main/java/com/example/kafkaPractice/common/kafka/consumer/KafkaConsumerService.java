package com.example.kafkaPractice.common.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    private static final String TOPIC_NAME = "test-topic";
    private static final String GROUP_ID = "my-group";

    @KafkaListener(
            topics = TOPIC_NAME,
            groupId = GROUP_ID,
            containerFactory = "rebalanceContainerFactory"
    )
    public void listen(String message) {
        log.info("📨 Received message: {}", message);
    }
}
