package com.example.kafkaPractice.common.kafka.config;

import com.example.kafkaPractice.common.kafka.consumer.CustomRebalanceListener;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final CustomRebalanceListener customRebalanceListener;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> rebalanceContainerFactory(ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 컨테이너 속성에 리밸런스 리스너를 설정한다.
        factory.getContainerProperties().setConsumerRebalanceListener(customRebalanceListener);

        return factory;
    }
}
