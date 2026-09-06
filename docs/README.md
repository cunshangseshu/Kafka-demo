# Kafka 实验与源码手册

本手册对应仓库现有代码，首页概览见 [根 README](../README.md)。以下命令使用 PowerShell；先按首页启动依赖和应用，每轮实验使用新的订单 ID，避免此前幂等记录影响观察。

## 1. 正常消费与持久化幂等

```powershell
$order = '{"orderId":10001,"userId":20001,"amount":99.90}'
Invoke-RestMethod -Method Post -Uri http://localhost:8090/api/kafka/orders -ContentType application/json -Body $order
Invoke-RestMethod -Method Post -Uri http://localhost:8090/api/kafka/orders -ContentType application/json -Body $order
```

使用数据库客户端连接 localhost:3308，账号取自 `.env`，执行：

```sql
SELECT * FROM kafka_consume_record WHERE business_key = '10001';
SELECT * FROM order_projection WHERE order_id = 10001;
```

预期各一条。业务唯一约束是 `(consumer_group, event_type, business_key)`，消息位置约束是 `(consumer_group, topic, partition_no, offset_value)`。`OrderCreatedEventService.process` 在事务内先登记再写订单，重复键跳过业务，Consumer 成功返回后调用 `acknowledgment.acknowledge()`。

Producer 使用 `orderId` 字符串作为 Key。相同 Key 在分区数不变时落到同一分区；只能讨论分区内顺序，不能声称整个 Topic 全局有序。

## 2. 事务回滚与业务异常 Retry / DLT

请求 DTO 没有非空校验，可通过 `userId=null` 触发 MySQL `order_projection.user_id NOT NULL` 约束失败：

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8090/api/kafka/orders -ContentType application/json -Body '{"orderId":10002,"userId":null,"amount":99.90}'
```

预期业务异常首次失败后重试 2 次、间隔 1 秒，最终投递到 `order-created-dlt`。两张表均不保留 `10002`，说明失败时幂等登记也已回滚。`setCommitRecovered(true)` 配合 `MANUAL_IMMEDIATE` 用于恢复成功后提交 Offset。

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-created-dlt --from-beginning --timeout-ms 10000 --formatter-property print.partition=true --formatter-property print.offset=true
```

结束时若 console consumer 因 10 秒无新消息报超时，应结合此前输出判断；这不等于 DLT 投递失败。持续查看可去掉 `--timeout-ms`，结束按 Ctrl+C。

## 3. ErrorHandlingDeserializer 与原始字节 DLT

```powershell
'not-json' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic order-created
```

JSON 反序列化失败发生在业务监听器之前，`ErrorHandlingDeserializer` 将异常交给容器。该异常默认不可重试，直接进入恢复处理。自定义 `DelegatingByTypeSerializer` 分别处理 `OrderCreatedEvent` 和 `byte[]`，避免原始坏消息进入 DLT 时再次序列化失败；Key 同样支持 String 与 byte[]。

沿用上一节 DLT 消费命令查看 `not-json`，再发送合法订单确认业务分区没有被坏消息阻塞。DLT 没有自动业务重放，后续人工检查/修复重放属于扩展方向。

## 4. Producer Idempotence、acks 与 ISR

现有配置：

| 参数 | 当前值 | 解释 |
| --- | --- | --- |
| `enable.idempotence` | true | 客户端重试的幂等发送，不能代替业务唯一键 |
| `acks` | all | 等待当前 ISR 的确认，实际容灾能力仍取决于副本与 ISR |
| `max.in.flight.requests.per.connection` | 5 | 配合幂等发送使用 |
| `request.timeout.ms` | 30000 | 单次请求等待超时 |
| `delivery.timeout.ms` | 120000 | 一条消息投递的总期限，包含重试等待等 |
| `retries` | 未显式设置 | 沿用当前 Kafka 客户端默认值；以启动日志中的有效配置为准 |

`producer_reliability_test` 配置单副本、`min.insync.replicas=2`，用于观察配置与实际发送行为的关系：

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8090/api/kafka/producer/reliability-test -ContentType application/json -Body '{"orderId":10003,"userId":20001,"amount":99.90}'
```

**实测差异：** 本次 Kafka 4.2.1 单节点 KRaft 环境中，Topic 查询确认 `ReplicationFactor=1`、`min.insync.replicas=2`、ISR 只有一个节点，Producer 日志确认 `acks=-1`（all）且幂等开启，但该消息仍收到发送成功回调。不能把此配置当作稳定的 `NotEnoughReplicas` 或超时触发器。

Apache 的 [KAFKA-18762](https://issues.apache.org/jira/browse/KAFKA-18762) 记录了 Kafka 3.9.0 KRaft 中类似现象；这是相关背景，不能据此断言本次 4.2.1 的内部根因完全相同。需要演示确定的超时路径时使用下一节 Broker Pause；验证真实副本容错需要搭建多 Broker 环境并控制 ISR，本仓库尚未实现。

HTTP 返回“已提交”不代表发送成功。该 Topic 仅用于配置观察，不应作为正常业务 Topic。

## 5. Broker Pause 与恢复

先正常发送一次，确保应用已获得 Topic 元数据，再进行短暂停顿：

```powershell
docker compose pause kafka
try {
    Invoke-RestMethod -Method Post -Uri http://localhost:8090/api/kafka/orders -ContentType application/json -Body '{"orderId":10004,"userId":20001,"amount":99.90}'
    Start-Sleep -Seconds 10
} finally {
    docker compose unpause kafka
}
```

预期投递期限内恢复后可能重试成功，以回调为准；做最终超时实验时使用新的订单 ID，并将等待改为 135 秒，再恢复 Broker。不要把 HTTP 耗时当作投递耗时：首次无元数据或缓冲区受限时，`send()` 本身也可能等待或抛出异常。最终发送失败目前只记日志，不自动落库补偿。

**超时不代表消息一定未到达。** 本次实验在约 120006 ms 后收到 `TimeoutException`，但 Broker 恢复后该订单仍被消费，MySQL 中有一条业务记录。这说明客户端等待结果超时与服务端最终是否写入是两件事；超时后的业务重发必须沿用业务唯一键并校验最终结果，不能仅凭失败回调断言“消息丢失”或“数据库一定为空”。

## 6. Consumer Lag 与 3 Partition × 3 Consumer

停掉当前 Java 应用，带实验延迟重新启动：

```powershell
java -jar target/kafka-demo-0.0.1-SNAPSHOT.jar --demo.kafka.consumer.processing-delay-ms=1000
```

在另一个终端批量发送并查看消费组：

```powershell
Invoke-RestMethod -Method Post -Uri 'http://localhost:8090/api/kafka/orders/batch?count=60&startOrderId=90000'
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service --members --verbose
```

观察 `CURRENT-OFFSET`、`LOG-END-OFFSET`、`LAG` 与分区分配；在当前普通 Topic 场景，Lag 可通过日志末端与已提交位置的差来理解。当前提交 Offset 表示下一条待消费位置。反复查询直到 Lag 归零，并在日志中检查 3 个线程和各自 Partition。

同组 3 个并发 Consumer 对应 3 个分区；增加超过 3 个 Consumer 并不会增加这个 Topic 的分区并行度。不要从配置直接推导“性能提升三倍”，实际表现还受 Key 分布、数据库与延迟影响。实验结束后恢复延迟为 0。

## 7. 工程边界与启动注意事项

- Kafka 为单 Broker KRaft Combined Mode、PLAINTEXT、单副本；不包含高可用部署和安全认证。
- 数据库本地事务与 Offset 不原子提交。重复投递仍可能发生，持久化幂等用于保护当前订单业务。
- 没有 Kafka 事务、Outbox、Producer 失败落库补偿、DLT 管理后台或生产监控告警。
- Spring Boot 的 Compose 自动生命周期已关闭，由 `docker compose up -d` / `docker compose stop` 显式管理依赖。
- MySQL 数据保留在 `kafka_mysql_data/`，该目录与 `.env` 被 Git 忽略。不要为切换密码直接删除已有数据。
- 仓库默认没有自动化测试；`mvn clean package` 的成功表示编译和打包成功，故障实验需运行依赖另行验证。
