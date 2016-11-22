package com.accenture.kafka.client.producter;

import com.accenture.kafka.client.domain.Metrics;
import com.accenture.kafka.client.marsh.KafkaMessage;
import com.accenture.kafka.client.marsh.MetricsSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by THINK on 2016/11/21.
 */
public class Client<T extends KafkaMessage> {
    public final String brokers;

    public static String getBrokersFromProperties() {
        InputStream resourceAsStream = Client.class.getResourceAsStream("/server.properties");
        Properties p = new Properties();
        try {
            p.load(resourceAsStream);
            return p.getProperty("brokers");
        } catch (IOException e) {
            return null;
        }
    }


    public final String clientId;
    private final Producer<Integer, T> producer;
    private final Integer clientHash;


    public Client(final String clientId, final ProductorConfig productorConfig, final String brokers) throws IOException {
        this.clientId = clientId;
        this.clientHash = clientId.hashCode();
        this.brokers = brokers;
        Properties props = new Properties();
        Map<String, Object> producerConfig = productorConfig.getProducerConfig();
        props.putAll(producerConfig);
        props.put("bootstrap.servers", this.brokers);
        producer = new KafkaProducer<>(props);
    }


    public Future<RecordMetadata> send(T t) {
        return producer.send(new ProducerRecord<Integer, T>(t.getTopic(), clientHash, t));
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        ProductorConfig build = ProductorConfig.builder()
                .keySerializer(IntegerSerializer.class)
                .valueSerializer(MetricsSerializer.class)
                .batchSize(1024 * 256).build();
        String brokers;
        if (args != null && args.length > 0) {
            brokers = args[0];
        } else {
            brokers = getBrokersFromProperties();
        }
        Client<Metrics> client = new Client<>("aaa", build, brokers);
        final Random random = new Random();
        while (true) {
            Metrics m = Metrics.builder()
                    .cpUtilization(random.nextInt(100))
                    .created(new Date())
                    .sth("oops!")
                    .mem(random.nextInt(100))
                    .network(random.nextInt(100))
                    .build();
            Future<RecordMetadata> send = client.send(m);
            RecordMetadata recordMetadata = send.get();
            System.out.println("keySize=" + recordMetadata.serializedKeySize()
                    + " valueSize= " + recordMetadata.serializedValueSize()
                    + " offset=" + recordMetadata.offset()
                    + " timestamp=" + recordMetadata.timestamp()
                    + " topic=" + recordMetadata.topic()
                    + " " + recordMetadata.toString());
            Thread.sleep(500L);
        }
    }
}
