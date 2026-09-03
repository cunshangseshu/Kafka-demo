package com.example.controller;

import com.example.event.OrderCreatedEvent;
import com.example.producer.OrderEventProducer;
import com.example.request.CreateOrderRequest;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

/**
 * Kafka Demo 测试接口。
 * <p>
 * 当前主要通过 Apifox
 * 触发 Kafka Producer。
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaDemoController {

    private final OrderEventProducer orderEventProducer;

    public KafkaDemoController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    /**
     * 创建并发送订单创建事件。
     */
    @PostMapping("/orders")
    public String sendOrderCreatedEvent(@RequestBody CreateOrderRequest request) {
         // HTTP Request  <-- 转换成 -->  Kafka Event
        OrderCreatedEvent event = new OrderCreatedEvent(request.orderId(), request.userId(), request.amount(), LocalDateTime.now());
        // 发布 Kafka Event。
        orderEventProducer.sendOrderCreatedEvent(event);
        return "订单创建事件已提交给 Kafka Producer";
    }
}