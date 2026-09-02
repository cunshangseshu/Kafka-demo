package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Kafka 学习 Demo 启动类。
 *
 * 当前第一阶段只负责：
 *
 * 1. 启动 Spring Boot；
 * 2. 通过 Spring Boot Docker Compose 支持启动 Kafka；
 * 3. 建立 Kafka 基础运行环境。
 *
 * 后续会逐步加入：
 *
 * Topic
 * Producer
 * Consumer
 * Consumer Group
 * Offset
 * ACK
 * Retry
 * DLQ
 * 幂等
 * 等内容。
 */
@SpringBootApplication
public class KafkaDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}