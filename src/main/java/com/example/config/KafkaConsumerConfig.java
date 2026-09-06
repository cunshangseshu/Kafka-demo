package com.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.common.TopicPartition;

import static com.example.constant.KafkaTopicConstants.ORDER_CREATED_DLT;

/**
 * Kafka Consumer 配置。
 * 主要负责：Consumer 异常处理 Retry BackOff 后面 DLT 也会继续在这里扩展。
 */
@Configuration
public class KafkaConsumerConfig {
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // Match the explicitly declared DLT; the default resolver uses a .DLT suffix.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (record, exception) -> new TopicPartition(ORDER_CREATED_DLT, record.partition()));
        // Do not advance the recovered offset if publishing to the DLT fails.
        recoverer.setFailIfSendResultIsError(true);
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, fixedBackOff);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }
}
