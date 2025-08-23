package com.example.kafkaPractice.common.kafka.consumer;

import com.example.kafkaPractice.common.kafka.config.KafkaTopicConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
            topics = KafkaTopicConfig.TEST_TOPIC_NAME,
            groupId = "group-a",
            containerFactory = "rebalanceContainerFactory"
    )
    public void consumeFromGroupA(String message) {
        log.info("[Analytics Team - Group A] Consumed message: %s%n", message);
    }

    @KafkaListener(
            topics =KafkaTopicConfig.TEST_TOPIC_NAME,
            groupId = "group-b",
            containerFactory = "rebalanceContainerFactory"
    )
    public void consumeFromGroupB(String message) {
        log.info("[Analytics Team - Group B] Consumed message: %s%n", message);
    }


}
