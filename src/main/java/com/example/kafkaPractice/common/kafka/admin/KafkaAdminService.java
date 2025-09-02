package com.example.kafkaPractice.common.kafka.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    boolean createTopic(String topicName, int partitions, int replicationFactor);
    void describeTopic(String topicName);

    @Service
    @Slf4j
    @RequiredArgsConstructor
    class KafkaAdminServiceImpl implements KafkaAdminService {
        private final KafkaAdmin kafkaAdmin;

        @Override
        public boolean createTopic(String topicName, int partitions, int replicationFactor) {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor);

                // 토픽 생성 옵션 (타임아웃 5초)
                CreateTopicsOptions options = new CreateTopicsOptions().timeoutMs(5000);
                // 토픽 생성 요청 및 결과 확인
                adminClient.createTopics(Collections.singleton(newTopic), options).all().get();
                log.info("토픽 '{}'이 성공적으로 생성되었습니다.", topicName);
                return true;
            } catch (ExecutionException | InterruptedException e) {
                log.error("토픽 '{}' 생성 중 오류가 발생했습니다: {}", topicName, e.getMessage());
                return false;
            }
        }

        @Override
        public void describeTopic(String topicName) {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topicName));
                Map<String, TopicDescription> topicInfo = result.allTopicNames().get();

                log.info("토픽 정보 [{}]: {}", topicName, topicInfo);
            } catch (ExecutionException | InterruptedException e) {
                log.error("토픽 '{}' 정보 조회 중 오류가 발생했습니다: {}", topicName, e.getMessage());
            }
        }
    }
}
