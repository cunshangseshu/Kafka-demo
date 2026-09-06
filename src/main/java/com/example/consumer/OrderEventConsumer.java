package com.example.consumer;

import com.example.event.OrderCreatedEvent;
import com.example.service.OrderCreatedEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
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

    /**
     * Kafka Lag 故障实验专用处理延迟。
     * application.yml:demo.kafka.consumer.processing-delay-ms
     * 正常运行时设置为 0；
     * Lag 实验时暂时设置为 1000。
     */
    @Value("${demo.kafka.consumer.processing-delay-ms:0}") // 单位ms，不知道是啥可以去看看application.yml 文件最下面；
    private long processingDelayMs;

    @KafkaListener(topics = ORDER_CREATED, groupId = ORDER_SERVICE, concurrency = "3")
    public void consumeOrderCreatedEvent(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment acknowledgment) {
        log.info("\n [ Kafka ] 收到订单创建事件:\n thread={} \n topic={} \n partition={} \n offset={} \n key={} \n value={}",
                Thread.currentThread().getName(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
        /*
         * ==================================================
         * Kafka Lag 实验
         * ==================================================
         * 故意把 Consumer 处理速度降低。
         * 这不是实际业务代码，只是为了让“生产速度 > 消费速度”，从而制造可观察的 Consumer Lag。
         */
        slowDownForLagExperiment();
        boolean firstProcess = orderCreatedEventService.process(record);
        if (firstProcess) {
            log.info(" [ Kafka ] 当前事件第一次成功处理: \n orderId={}", record.value().orderId());
        } else {
            log.warn("[ Kafka ] 当前事件属于重复事件， 已跳过真正业务逻辑: \n orderId={}", record.value().orderId());
        }
        acknowledgment.acknowledge();
        log.info(" [ Kafka ] Offset 已请求提交: \n partition={} \n processedOffset={} \n nextOffset={}",
                record.partition(),
                record.offset(),
                record.offset() + 1);
    }

    /**
     * Lag 实验专用延迟。
     */
    private void slowDownForLagExperiment() {
        if (processingDelayMs <= 0) return;
        try {
            Thread.sleep(processingDelayMs);
        } catch (InterruptedException exception) {
            // 恢复线程中断标记。
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka Consumer Lag 实验线程被中断", exception);
        }
    }
}