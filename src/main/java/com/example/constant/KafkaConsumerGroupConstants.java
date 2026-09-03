package com.example.constant;

/**
 * Kafka Consumer Group 常量。
 * 不同业务消费者组分开管理，避免 groupId 到处手写字符串。
 */
public final class KafkaConsumerGroupConstants {
    private KafkaConsumerGroupConstants() {
    }

    //订单服务消费者组。
    public static final String ORDER_SERVICE = "order-service";
}