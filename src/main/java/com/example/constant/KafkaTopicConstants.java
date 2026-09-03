package com.example.constant;

/**
 * Kafka Topic 名称常量。
 * 统一维护当前项目中使用到的 Kafka Topic 名称，避免 Producer、Consumer、Config 等位置出现大量重复的字符串常量。
 */
public final class KafkaTopicConstants {

    private KafkaTopicConstants() {
    }
    // 订单创建事件 Topic。
    public static final String ORDER_CREATED = "order-created";
    // 订单创建事件 Dead Letter Topic。
    public static final String ORDER_CREATED_DLT = "order-created-dlt";
}