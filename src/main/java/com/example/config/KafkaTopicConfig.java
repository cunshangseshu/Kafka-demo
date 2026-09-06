package com.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import static com.example.constant.KafkaTopicConstants.*;

/**
 * Kafka Topic 配置类。
 * 当前阶段主要负责：
 * 1. 声明项目需要使用的 Topic；
 * 2. 指定 Topic 的 Partition 数量；
 * 3. 指定副本数量；
 * 4. Spring Boot 启动时，由 KafkaAdmin 自动检查并创建 Topic。
 */
@Configuration
public class KafkaTopicConfig {
    /**
     * 订单创建事件 Topic。
     * Topic 名称：order-created
     * Partition 数量： 3
     * Replica 数量：1
     * 当前只有一个 Kafka Broker，因此副本数暂时只能设置为 1。
     */
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(ORDER_CREATED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCreatedDltTopic() {
        return TopicBuilder.name(ORDER_CREATED_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic producerReliabilityTestTopic() {
        return TopicBuilder.name(PRODUCER_RELIABILITY_TEST).partitions(1).replicas(1).config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2").build();
    }
}
