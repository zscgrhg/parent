package com.accenture.kafka.service.autoconfig;

import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import java.util.Map;

/**
 * Created by THINK on 2016/11/15.
 */
public interface ListenerContainerFactoryBuilder<K, V> {
    Map<String, Object> consumerConfigs();

    ConsumerFactory<K, V> consumerFactory();

    KafkaListenerContainerFactory containerFactory();
}
