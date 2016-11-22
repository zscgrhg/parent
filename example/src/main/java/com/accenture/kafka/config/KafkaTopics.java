package com.accenture.kafka.config;


import com.accenture.kafka.service.autoconfig.TopicDefine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by THINK on 2016/11/17.
 */
@Configuration
public class KafkaTopics {
    @Bean
    TopicDefine topicDefine1() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("prod1")
                        .partitions(3)
                        .replicationFactor(1)
                        .rackAwareMode(null)
                        .build();
        return topicDefine;
    }

    @Bean
    TopicDefine topicDefine2() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("prod2")
                        .partitions(30)
                        .replicationFactor(1)
                        .build();
        return topicDefine;
    }
}
