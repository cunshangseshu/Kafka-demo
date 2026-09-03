package com.example.event;


import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单创建事件。
 * 这个类表达的是：
 * “订单已经创建”而不是一个普通的 Order 数据对象。
 * Kafka 中推荐培养“事件”的思维：
 * 某件事情发生了
 * ↓
 * 发布 Event
 * ↓
 * 其他系统根据需要消费
 */
public record OrderCreatedEvent(
        // 订单 ID 后面发送 Kafka 时，我们还会把它作为消息 Key。
        Long orderId,
        // 下单用户 ID。
        Long userId,
        // 订单金额。
        BigDecimal amount,
        // 订单创建时间。
        LocalDateTime createdTime
) {

}

