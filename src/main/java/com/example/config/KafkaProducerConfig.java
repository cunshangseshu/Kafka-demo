package com.example.config;

import com.example.event.OrderCreatedEvent;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka Producer 配置。
 * <p>
 * 主要负责：
 * 1. 正常业务事件的 JSON 序列化；
 * 2. 反序列化失败消息进入 DLT 时，原始 byte[] 的序列化；
 * 3. 根据消息运行时类型自动选择对应 Serializer。
 */
@Configuration
public class KafkaProducerConfig {
    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory(KafkaProperties kafkaProperties, ObjectProvider<SslBundles> sslBundles) {
        /*
         * 继续复用 application.yml 中的：
         * bootstrap-servers、acks、retries 等 Producer 配置。
         * 我们这里只接管 Serializer。
         */
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable());
        /*
         * ==================================================
         * Key Serializer
         * ==================================================
         * 正常业务：
         * String key
         *      ↓
         * StringSerializer
         *
         * 如果未来 Key 本身反序列化失败进入 DLT：
         * byte[]
         *      ↓
         * ByteArraySerializer
         */
        Map<Class<?>, Serializer<?>> keySerializers = new LinkedHashMap<>();
        keySerializers.put(String.class, new StringSerializer());
        keySerializers.put(byte[].class, new ByteArraySerializer());
        /*
         * ==================================================
         * Value Serializer
         * ==================================================
         * 正常业务事件：
         * OrderCreatedEvent
         *      ↓
         * JsonSerializer
         *
         * 反序列化失败消息：
         * 原始 byte[]
         *      ↓
         * ByteArraySerializer
         */
        Map<Class<?>, Serializer<?>> valueSerializers = new LinkedHashMap<>();
        valueSerializers.put(OrderCreatedEvent.class, new JsonSerializer<>());
        valueSerializers.put(byte[].class, new ByteArraySerializer());
        /*
         * DelegatingByTypeSerializer：根据实际 Java 类型，自动找到对应 Serializer。
         */
        DelegatingByTypeSerializer keySerializer = new DelegatingByTypeSerializer(keySerializers);
        DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(valueSerializers);
        return new DefaultKafkaProducerFactory<>(producerProperties, keySerializer, valueSerializer);
    }
}