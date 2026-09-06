package com.example.controller;

import com.example.event.OrderCreatedEvent;
import com.example.producer.OrderEventProducer;
import com.example.request.CreateOrderRequest;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Kafka Demo 测试接口。
 * 用 Apifox
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
     * 创建并发送单个订单创建事件。
     */
    @PostMapping("/orders")
    public String sendOrderCreatedEvent(@RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = new OrderCreatedEvent(request.orderId(), request.userId(), request.amount(), LocalDateTime.now());
        orderEventProducer.sendOrderCreatedEvent(event);
        return "订单创建事件已提交给 Kafka Producer";
    }

    /**
     * Producer Reliability 测试接口。
     */
    @PostMapping("/producer/reliability-test")
    public String producerReliabilityTest(@RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = new OrderCreatedEvent(request.orderId(), request.userId(), request.amount(), LocalDateTime.now());
        orderEventProducer.sendProducerReliabilityTest(event);
        return "Producer Reliability 测试消息已提交";
    }

    /**
     * Kafka Consumer Lag 实验。
     * 一次快速生产大量消息，用于制造：Producer生产速度 > Consumer消费速度的场景。
     */
    @PostMapping("/orders/batch")
    public String sendOrderBatch(@RequestParam(defaultValue = "60") int count, @RequestParam(defaultValue = "90000") long startOrderId) {
        // 防止接口误操作一次发送太多消息。
        if (count < 1 || count > 500) {
            throw new IllegalArgumentException("count 必须在 1 ~ 500 之间");
        }
        for (int i = 0; i < count; i++) {
            long orderId = startOrderId + i;
            OrderCreatedEvent event = new OrderCreatedEvent(orderId,orderId, BigDecimal.valueOf(100L + i), LocalDateTime.now());// 参数二还是直接用orderId吧，懒得写UUID了；
            orderEventProducer.sendOrderCreatedEvent(event);// 循环调用 60 次，本类的 sendOrderCreatedEvent方法
        }
        return "\n Kafka 批量消息已提交：[ count=%d ] , [ startOrderId=%d ]".formatted(count, startOrderId);
    }
}