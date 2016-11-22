package com.accenture.kafka.client.domain;


import com.accenture.kafka.client.marsh.DeserializeException;
import com.accenture.kafka.client.marsh.KafkaMessage;
import lombok.*;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

/**
 * Created by THINK on 2016/11/20.
 */
@Data
@ToString
@EqualsAndHashCode
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Metrics implements KafkaMessage<Metrics> {

    private static final long serialVersionUID = 6842977566063744564L;

    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    private final int version = VERSION_2;

    private int cpUtilization;
    private int mem;
    private int network;
    private Date created = new Date();
    private String talk = "";


    public void writeOut(final ObjectOutput out) throws IOException {
        out.writeInt(cpUtilization);
        out.writeLong(created.getTime());
        out.writeInt(mem);
        out.writeObject(talk);
        out.writeInt(network);
    }


    public Metrics readInByVersion(final ObjectInput in, int version) throws IOException, ClassNotFoundException {
        Metrics cpu;
        switch (version) {

            case VERSION_2:
                cpu = Metrics.builder().cpUtilization(in.readInt())
                        .created(new Date(in.readLong()))
                        .mem(in.readInt())
                        .talk((String) in.readObject())
                        .network(in.readInt()).build();
                break;
            case VERSION_1:
                cpu = Metrics.builder().cpUtilization(in.readInt())
                        .created(new Date(in.readLong()))
                        .mem(in.readInt())
                        .talk((String) in.readObject())
                        .network(in.readInt()).build();
                break;
            default:
                throw new DeserializeException(version);
        }
        return cpu;
    }

    @Override
    public String getTopic() {
        return "local1";
    }
}
