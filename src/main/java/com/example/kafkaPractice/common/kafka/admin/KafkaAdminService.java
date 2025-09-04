package com.example.kafkaPractice.common.kafka.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {
    boolean createTopic(String topicName, int partitions, int replicationFactor);

    void describeTopic(String topicName);

    Set<String> getTopicList(String topicName);

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

        @Override
        public Set<String> getTopicList(String topicName) {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                // listTopics()는 ListTopicsResult를 반환한다.
                ListTopicsResult topics = adminClient.listTopics();
                // .names()를 통해 토픽 이름 Set을 담은 KafkaFuture를 얻는다.
                KafkaFuture<Set<String>> names = topics.names();
                // .get()으로 결과를 기다려 실제 Set<String>을 가져온다.
                Set<String> topicNames = names.get();

                log.info("🔍 조회된 토픽 수: {}", topicNames.size());
                return topicNames;
            } catch (ExecutionException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("토픽 목록 조회 중 오류가 발생했습니다: {}", e.getMessage());
                // 실패 시에는 null 대신 비어있는 컬렉션을 반환하는 것이 더 안전하다.
                return Collections.emptySet();
            }
        }
    }
}
