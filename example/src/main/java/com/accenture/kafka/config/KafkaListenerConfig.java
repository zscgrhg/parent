package com.accenture.kafka.config;


import com.accenture.kafka.client.domain.Metrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

import static com.accenture.kafka.service.autoconfig.KafkaAutoConfiguration.KafkaListenerContainerFactoryAutoConfiguration.BATCH_CONTAINER_FACTORY;


/**
 * Created by THINK on 2016/11/16.
 */
@Configuration
@EnableKafka
public class KafkaListenerConfig {


    @Bean
    public Listenner listenner() {
        return new Listenner();
    }

    public static class Listenner {
        @KafkaListener(topics = "local1", containerFactory = BATCH_CONTAINER_FACTORY)
        public void listen(List<ConsumerRecord<String, Metrics>> list) throws InterruptedException {
            list.parallelStream().forEach(it->{
                System.out.println("received : content="+it.value());
            });
        }
    }
}
