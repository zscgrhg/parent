package com.accenture.kafka.client.marsh;


import com.accenture.kafka.client.domain.Metrics;

/**
 * Created by THINK on 2016/11/20.
 */
public class MetricsDeserilizer extends KafkaMessageDeserializer<Metrics> {
    @Override
    public Metrics newInstance() {
        return new Metrics();
    }
}
