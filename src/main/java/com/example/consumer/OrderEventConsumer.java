package com.example.consumer;

import com.example.event.OrderCreatedEvent;
import com.example.service.OrderCreatedEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import static com.example.constant.KafkaConsumerGroupConstants.ORDER_SERVICE;
import static com.example.constant.KafkaTopicConstants.ORDER_CREATED;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {
    private final OrderCreatedEventService orderCreatedEventService;

    @KafkaListener(topics = ORDER_CREATED, groupId = ORDER_SERVICE, concurrency = "3")
    public void consumeOrderCreatedEvent(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment acknowledgment) {
        log.info("\n [ Kafka ] 收到订单创建事件:\n thread={}\n topic={}\n partition={}\n offset={}\n key={}\n value={}",
                Thread.currentThread().getName(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value()
        );
        //如果抓到异常，业务一般会卡在这里不动
        boolean firstProcess = orderCreatedEventService.process(record);
        if (firstProcess) {
            log.info("\n [ Kafka ] 当前事件第一次成功处理: \n orderId={}", record.value().orderId());
        } else {
            log.warn("\n [ Kafka ] 当前事件属于重复事件，已跳过真正业务逻辑: \n orderId={}", record.value().orderId());
        }
        acknowledgment.acknowledge();
        log.info("\n [ Kafka ] Offset 已请求提交: \n partition={} \n processedOffset={} \n nextOffset={}",
                record.partition(),
                record.offset(),
                record.offset() + 1
        );
    }
}