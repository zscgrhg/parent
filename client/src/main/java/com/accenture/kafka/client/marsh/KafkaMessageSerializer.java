package com.accenture.kafka.client.marsh;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Created by THINK on 2016/11/20.
 */
public abstract class KafkaMessageSerializer<T extends KafkaMessage> implements Serializer<T> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        } else {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                data.writeOut(objectOutputStream);
            } catch (IOException e) {
                throw new RuntimeException("Serialize Exception:" + e.getMessage());
            }
            byte[] bytes = byteArrayOutputStream.toByteArray();
            return bytes;
        }

    }

    @Override
    public void close() {

    }
}
