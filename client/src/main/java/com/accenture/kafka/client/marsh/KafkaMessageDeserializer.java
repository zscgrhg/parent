package com.accenture.kafka.client.marsh;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Created by THINK on 2016/11/20.
 */
public abstract class KafkaMessageDeserializer<T extends KafkaMessage<T>> implements Deserializer<T> {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    public abstract T newInstance();

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        } else {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            T t = newInstance();
            try (ObjectInputStream objIn = new ObjectInputStream(in)) {
                t = t.readIn(objIn);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Deserialize Exception:" + e.getMessage());
            }
            return t;
        }
    }

    @Override
    public void close() {

    }
}
