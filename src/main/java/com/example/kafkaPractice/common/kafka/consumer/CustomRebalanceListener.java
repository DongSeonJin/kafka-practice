package com.example.kafkaPractice.common.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class CustomRebalanceListener implements ConsumerAwareRebalanceListener {
    private Consumer<String, String> consumer;
    // 처리 중인 오프셋을 저장하기 위한 맵 (선택적)
    private final Map<TopicPartition, Long> processingOffsets = new HashMap<>();

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 리밸런스 후 새로 할당받은 파티션 정보 로깅
        log.info("✅ Partitions Assigned: {}", partitions);
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // 파티션을 잃기 직전에 호출됨 (스프링이 자동 커밋 하기 전)
        log.warn("🟡 Partitions Revoked (Before Commit): {}", partitions);

        // 중요: 리밸런스가 발생하면, 지금까지 처리한 내용을 안전하게 동기 커밋해야 한다.
        // 스프링 부트의 자동 커밋을 사용하더라도, 안전한 종료를 위해 수동 동기 커밋을 여기서 수행하는 것이 좋다.
        if (consumer != null) {
            log.info("Committing offsets before rebalance...");
            // 현재까지 처리된 오프셋을 동기적으로 커밋한다.
            consumer.commitSync();
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        // 비정상적으로 파티션이 유실되었음을 로깅한다.
        log.error("🚨 Partitions Lost (Abnormally): {}", partitions);

        // 이 상황에서는 커밋이 실패할 가능성이 높으므로, 커밋 시도보다는
        // 정리 작업이나 상태 초기화, 외부 시스템에 경고 알림 전송 등의 로직을 수행하는 것이 좋다.
    }
}
