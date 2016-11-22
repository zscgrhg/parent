package com.accenture.kafka.config;


import com.accenture.kafka.service.autoconfig.TopicDefine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static com.accenture.kafka.shared.Profiles.DEV;
import static com.accenture.kafka.shared.Profiles.TEST;


/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
@Profile({DEV, TEST})
public class KafkaTopics4Dev {
    @Bean
    TopicDefine topicDefine1() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("local1")
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
                        .topic("local2")
                        .partitions(30)
                        .replicationFactor(1)
                        .build();
        return topicDefine;
    }

    @Bean
    TopicDefine topicDefine3() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("local3")
                        .partitions(30)
                        .replicationFactor(1)
                        .build();
        return topicDefine;
    }

    @Bean
    TopicDefine topicDefine4() {
        TopicDefine topicDefine =
                TopicDefine
                        .builder()
                        .topic("local4")
                        .partitions(30)
                        .replicationFactor(1)
                        .build();
        return topicDefine;
    }
}
