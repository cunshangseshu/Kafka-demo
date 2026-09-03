package com.example.request;

import java.math.BigDecimal;

/**
 * 创建订单测试请求。
 * 这是 HTTP API 的入参和 Kafka Event 的职责不同。
 */
public record CreateOrderRequest(
        Long orderId,
        Long userId,
        BigDecimal amount
) {

}