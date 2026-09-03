package com.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
/**
 * Kafka Consumer 配置。
 * 主要负责：Consumer 异常处理 Retry BackOff 后面 DLT 也会继续在这里扩展。
 */
@Configuration
public class KafkaConsumerConfig {
    /**
     * Kafka Consumer 默认异常处理器。
     * FixedBackOff: interval = 1000ms   retries  = 2
     * 所以总执行次数： 第一次正常消费 + 额外重试 2 次 = 最多3次
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2L);
        return new DefaultErrorHandler(fixedBackOff);
    }
}