package com.accenture.kafka.service.autoconfig;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */
public class BatchListenerCfbAdapater<K, V> extends ListenerCfbAdapater<K, V> {


    public BatchListenerCfbAdapater(final KafkaInspection kafkaInspection, final Map<String, Object> configs) {
        super(kafkaInspection, configs);
    }

    @Override
    protected void adjustConcurrentKafkaListenerContainerFactory(final ConcurrentKafkaListenerContainerFactory<K, V> factory) {
        factory.setBatchListener(true);
    }
}
