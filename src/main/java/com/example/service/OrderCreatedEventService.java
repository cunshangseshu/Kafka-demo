package com.example.service;

import com.example.event.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

import static com.example.constant.KafkaConsumerGroupConstants.ORDER_SERVICE;

@Slf4j
@Service
public class OrderCreatedEventService {
    private static final String EVENT_TYPE = "ORDER_CREATED";
    private final JdbcTemplate jdbcTemplate;

    public OrderCreatedEventService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public boolean process(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent event = record.value();
        try {
            jdbcTemplate.update(
                    """
                            INSERT INTO kafka_consume_record
                            (
                                consumer_group,
                                event_type,
                                business_key,
                                topic,
                                partition_no,
                                offset_value
                            )
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                    ORDER_SERVICE,
                    EVENT_TYPE,
                    String.valueOf(event.orderId()),
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
            /*
             * 故障实验：
             * 幂等记录已经 INSERT，
             * 但真正业务还没执行时模拟程序异常。
             */
            if (event.orderId().equals(30003L)) {
                log.error("\n[ Kafka ] 故意制造事务异常:\n orderId={}\n 当前阶段=幂等记录已写入，业务数据尚未写入", event.orderId());
                throw new RuntimeException("模拟：幂等记录写入成功后，订单业务处理失败");
            }
        } catch (DuplicateKeyException exception) {
            log.warn("\n[ Kafka ] MySQL 幂等校验命中，跳过重复事件:\n orderId={}\n topic={}\n partition={}\n offset={}",
                    event.orderId(),
                    record.topic(),
                    record.partition(),
                    record.offset()
            );
            return false;
        }
        jdbcTemplate.update(
                """
                        INSERT INTO order_projection
                        (
                            order_id,
                            user_id,
                            amount,
                            created_time
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                event.orderId(),
                event.userId(),
                event.amount(),
                Timestamp.valueOf(event.createdTime())
        );
        log.info("\n[ Kafka ] 订单业务处理成功:\n orderId={}\n userId={}\n amount={}",
                event.orderId(),
                event.userId(),
                event.amount()
        );
        return true;
    }
}