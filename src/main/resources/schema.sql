-- ==========================================================
-- Kafka Consumer 幂等记录表
-- ==========================================================
CREATE TABLE IF NOT EXISTS kafka_consume_record
(
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- 哪个 Consumer Group 处理的
    consumer_group VARCHAR(100) NOT NULL,
    -- 事件类型
    event_type VARCHAR(100) NOT NULL,
    -- 业务唯一 Key
    business_key VARCHAR(128) NOT NULL,
    -- Kafka 原始位置信息
    topic VARCHAR(200) NOT NULL,
    partition_no INT NOT NULL,
    offset_value BIGINT NOT NULL,
    processed_at DATETIME(6)
        NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    -- ======================================================
    -- 业务幂等约束
    -- ======================================================
    -- 同一个 Consumer Group
    -- + 同一种 Event
    -- + 同一个业务 Key
    -- 只能真正处理一次。
    UNIQUE KEY uk_business_event
        (consumer_group, event_type, business_key),
    -- ======================================================
    -- Kafka Record 唯一约束
    -- ======================================================
    -- 同一个 Consumer Group 下，
    -- 同一条 Topic + Partition + Offset
    -- 也只能登记一次。
    UNIQUE KEY uk_kafka_record
        (consumer_group, topic, partition_no, offset_value)
);
-- ==========================================================
-- 模拟真正的订单业务结果表
-- ==========================================================
CREATE TABLE IF NOT EXISTS order_projection
(
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    created_time DATETIME(6) NOT NULL,
    consumed_at DATETIME(6)
        NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);