package com.accenture.kafka.service.autoconfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */
public class ListenerCfbAdapater<K, V> implements ListenerContainerFactoryBuilder<K, V> {
    protected final KafkaInspection kafkaInspection;
    protected final Map<String, Object> configs;

    public ListenerCfbAdapater(
            final KafkaInspection kafkaInspection,
            final Map<String, Object> configs) {
        this.kafkaInspection = kafkaInspection;
        this.configs = configs;
    }


    @Override
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(configs);
        adjustConsumerConfigs(props);
        return props;
    }

    @Override
    public ConsumerFactory<K, V> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Override
    public KafkaListenerContainerFactory containerFactory() {
        ConcurrentKafkaListenerContainerFactory<K, V> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        adjustConcurrentKafkaListenerContainerFactory(factory);
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(Math.max(1, kafkaInspection.maxPartitions));
        ContainerProperties containerProperties = factory.getContainerProperties();
        adjustContainerProperties(containerProperties);
        return factory;
    }


    protected void adjustConsumerConfigs(final Map<String, Object> consumerConfigs) {
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaInspection.kafkaConnection.brokersAddress);
    }


    protected void adjustContainerProperties(final ContainerProperties containerProperties) {

    }


    protected void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {

    }
}
