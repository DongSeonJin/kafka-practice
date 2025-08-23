package com.example.kafkaPractice.common.kafka.controller;

import com.example.kafkaPractice.common.kafka.config.KafkaTopicConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC_NAME = "my-topic";

    @PostMapping("/send")
    public void sendMessage(@RequestParam("message") String message) {
        System.out.println("[PRODUCER] Sending message: " + message);
        kafkaTemplate.send(KafkaTopicConfig.TEST_TOPIC_NAME, message);
    }
}
