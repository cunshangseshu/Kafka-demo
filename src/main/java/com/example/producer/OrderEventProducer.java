package com.example.producer;

import com.example.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static com.example.constant.KafkaTopicConstants.ORDER_CREATED;

/**
 * 订单 Kafka Event Producer。
 * 负责把订单相关事件发送到 Kafka。
 */
@Slf4j
@Component
public class OrderEventProducer {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送订单创建事件。
     *
     * @param event 订单创建事件
     */
    public void sendOrderCreatedEvent(OrderCreatedEvent event) {
        String key = String.valueOf(event.orderId());
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future = kafkaTemplate.send(ORDER_CREATED, key, event);
        // 注册异步回调。
        future.whenComplete((result, throwable) -> {
            // 发送失败。
            if (throwable != null) {
                log.error("\n [ Kafka ]:消息发送失败\n topic={}\n key={}\n event={}\n",
                        ORDER_CREATED,
                        key,
                        event,
                        throwable
                );
                return;
            }
            /*
             * 发送成功。
             * result.getRecordMetadata()
             * 可以拿到 Kafka 返回的 RecordMetadata。
             */
            var metadata = result.getRecordMetadata();
            log.info("\n[ Kafka ]: 消息发送成功\n topic={}\n partition={}\n offset={}\n key={}\n event={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    key,
                    event
            );
        });
    }
}