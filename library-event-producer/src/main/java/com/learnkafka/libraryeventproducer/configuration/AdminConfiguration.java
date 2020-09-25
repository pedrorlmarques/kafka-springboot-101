package com.learnkafka.libraryeventproducer.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;


/**
 * Kafka Admin Example Usage
 * Not recommended in Production Env
 */
@Configuration
@Profile("local || default")
public class AdminConfiguration {


    @Bean
    NewTopic libraryEvents(@Value("${spring.kafka.template.default-topic}") String defaultTopic) {
        return TopicBuilder.name(defaultTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
