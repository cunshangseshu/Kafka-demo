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
    /**
     * KafkaTemplate 是 Spring Kafka
     * 提供的 Kafka Producer 操作模板。
     * <p>
     * 当前：
     * <p>
     * Key   = String
     * Value = OrderCreatedEvent
     */
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    /**
     * 构造器注入。
     */
    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 发送订单创建事件。
     *
     * @param event 订单创建事件
     */
    public void sendOrderCreatedEvent(OrderCreatedEvent event) {
        /*
         * 使用 orderId 作为 Kafka Message Key。
         * 例如：
         * orderId = 10001
         * Kafka Key：
         * "10001"
         * 相同 Key 默认会稳定地选择相同 Partition，从而帮助我们保证： 同一个订单相关事件,在同一个 Partition 内保持顺序。
         */
        String key = String.valueOf(event.orderId());
        /*
         * KafkaTemplate.send(...) 并不是同步等待 Kafka 完成后才返回。
         * 它会返回：
         * CompletableFuture<SendResult<...>>
         * 后面 Kafka 真正返回发送结果以后，Future 才完成。
         */
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future = kafkaTemplate.send(ORDER_CREATED, key, event);
        // 注册异步回调。
        future.whenComplete((result, throwable) -> {
            // 发送失败。
            if (throwable != null) {
                log.error("\n [ Kafka ]:消息发送失败\n topic={}\n key={}\n event={}\n", ORDER_CREATED, key, event, throwable);
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
                    event);
        });
    }
}