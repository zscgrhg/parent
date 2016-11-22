package com.accenture.kafka.thread;



import com.accenture.kafka.client.domain.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Date;
import java.util.Random;

;

/**
 * Created by THINK on 2016/11/20.
 */
@Slf4j
public class MetricsProductor extends Thread {
    public static final Random random = new Random();
    final String key;
    final String topic;
    final KafkaTemplate kafkaTemplate;

    public MetricsProductor(final String key, final String topic, final KafkaTemplate kafkaTemplate) {
        super();
        this.key = key;
        setName(String.format("CpuProductor-%s-%s", getId(), key));
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run() {
        log.info(getName() + " start work!");
        while (!isInterrupted()) {
            Metrics cpu = Metrics.builder()
                    .cpUtilization(random.nextInt(100))
                    .created(new Date())
                    .sth("oops!")
                    .mem(random.nextInt(100))
                    .network(random.nextInt(100))
                    .build();
            kafkaTemplate.send(topic, (Integer) key.hashCode(), cpu);
            try {
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                interrupt();
            }
        }
        log.info(getName() + " exit！");
    }
}
