package com.example.consumer;

import com.example.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import static com.example.constant.KafkaConsumerGroupConstants.ORDER_SERVICE;
import static com.example.constant.KafkaTopicConstants.ORDER_CREATED;

/**
 * 订单事件消费者。
 */
@Slf4j
@Component
public class OrderEventConsumer {
    @KafkaListener(topics = ORDER_CREATED, groupId = ORDER_SERVICE, concurrency = "3")
    public void consumeOrderCreatedEvent(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment acknowledgment) {
        // ConsumerRecord 是 Kafka Consumer 实际收到的一条完整记录。
        // 它不只有 Value，还包含：topic、partition、offset、key、value、timestamp、headers
        log.info("\n[ Kafka ] 消息消费成功:\n thread={}\n topic={}\n partition={}\n offset={}\n key={}\n value={}",
                Thread.currentThread().getName(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
        // Kafka Consumer 异常重试实验
        if (record.value().orderId().equals(88888L)) {
            log.warn("\n[ Kafka ] 模拟业务异常:\n thread={}\n partition={}\n offset={}\n key={}",
                    Thread.currentThread().getName(),
                    record.partition(),
                    record.offset(),
                    record.key());
            throw new RuntimeException("模拟订单业务处理失败");
        }
        acknowledgment.acknowledge();
        log.info("\n[ Kafka ] Offset 已请求提交:\n partition={}\n processedOffset={}\n nextOffset={}",
                record.partition(),
                record.offset(),
                record.offset() + 1);
    }
}