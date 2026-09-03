package com.example.consumer;

import com.example.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.example.constant.KafkaConsumerGroupConstants.ORDER_SERVICE;
import static com.example.constant.KafkaTopicConstants.ORDER_CREATED;

/**
 * 订单事件消费者。
 */
@Slf4j
@Component
public class OrderEventConsumer {
    /**
     * 消费订单创建事件。
     * topics: 监听哪个 Topic。
     * groupId: 当前 Consumer 属于哪个 Consumer Group。
     */
    @KafkaListener(topics = ORDER_CREATED,groupId = ORDER_SERVICE)
    public void consumeOrderCreatedEvent(ConsumerRecord<String, OrderCreatedEvent> record) {
        // ConsumerRecord 是 Kafka Consumer 实际收到的一条完整记录。
        // 它不只有 Value，还包含：topi、partition、offset、key、value、timestamp、headers
        log.info("\n[ Kafka ] 消息消费成功:\n topic={}\n partition={}\n offset={}\n key={}\n value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}