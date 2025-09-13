package com.example.kafkaPractice.common.kafka.admin;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface KafkaAdminService {

    boolean createTopic(String topicName, int partitions, int replicationFactor);

    void describeTopic(String topicName);

    Set<String> getTopicList(String topicName);

    void describeTopicAsync(String topicName);

    void describeTopicConfig(String topicName);

    void deleteRecordsBeforeOffset(String topicName, int partition, long offset);

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

        @Override
        public void describeTopicAsync(String topicName) {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topicName));
                KafkaFuture<Map<String, TopicDescription>> future = result.allTopicNames();

                // whenComplete 콜백 등록
                future.whenComplete((topicInfoMap, throwable) -> {
                    if (throwable == null) {
                        TopicDescription description = topicInfoMap.get(topicName);
                        log.info("(Callback) 토픽 정보 [{}]: {}", topicName, description);
                    } else {
                        log.error("(Callback) 토픽 '{}' 정보 조회 중 오류 발생: {}", topicName, throwable.getMessage());
                    }
                });
            }
        }

        @Override
        public void describeTopicConfig(String topicName) {
            // 1. 설정 조회의 대상을 지정하는 ConfigResource 객체 생성
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            log.info("토픽 '{}'의 설정을 조회합니다...", topicName);
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(topicResource));
                Map<ConfigResource, Config> configs = result.all().get();
                // 4. 설정값 출력
                configs.forEach((resource, config) -> {
                    log.info("'{}'의 설정:", resource.name());
                    Collection<ConfigEntry> entries = config.entries();
                    entries.forEach(entry -> {
                        // 기본값이 아닌 설정만 출력
                        if (!entry.isDefault()) {
                            log.info("  - {} = {}", entry.name(), entry.value());
                        }
                    });
                });
            } catch (Exception e) {
                log.error("토픽 '{}' 설정 조회 중 오류 발생: {}", topicName, e.getMessage());
            }
        }

        @Override
        public void deleteRecordsBeforeOffset(String topicName, int partition, long offset) {
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(offset);
            Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = Map.of(topicPartition, recordsToDelete);

            log.warn("🗑️ [AdminClient] 토픽 '{}'-{} 파티션에서 오프셋 {} 이전의 레코드를 삭제합니다",
                    topicName, partition, offset);

            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                adminClient.deleteRecords(recordsToDeleteMap).all().get();
                log.info("[AdminClient] 레코드 삭제 요청이 성공적으로 완료되었습니다.");
            } catch (Exception e) {
                log.error("[AdminClient] 레코드 삭제 중 오류가 발생했습니다: {}", e.getMessage());
            }
        }
    }
}
