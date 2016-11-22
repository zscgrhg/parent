package com.accenture.kafka.client.marsh;

import java.io.*;

/**
 * Created by THINK on 2016/11/21.
 */
public interface KafkaMessage<T extends KafkaMessage> extends Serializable {
    void writeOut(ObjectOutput out) throws IOException;

    T readInByVersion(ObjectInput in,int version) throws IOException, ClassNotFoundException;

    int getVersion();

    String getTopic();
}
