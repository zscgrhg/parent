package com.accenture.kafka.controller;




import com.accenture.kafka.thread.MetricsProductor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by THINK on 2016/11/18.
 */
@RestController
public class Hello {
    final List<Thread> productors = new ArrayList<>();
    final Random random = new Random();
    @Autowired
    KafkaTemplate kafkaTemplate;

    synchronized private void addProductor(MetricsProductor metricsProductor) {
        productors.add(metricsProductor);
        metricsProductor.start();
    }

    synchronized private void stopAndClear() {
        for (Thread productor : productors) {
            try {
                productor.interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        productors.clear();
    }

    @RequestMapping("start")
    public String sayHelloToKafka(String clientId, String topic) {
        MetricsProductor metricsProductor = new MetricsProductor(clientId, topic, kafkaTemplate);
        addProductor(metricsProductor);
        return "1";
    }

    @RequestMapping("stop")
    public String stop() {
        stopAndClear();
        return "1";
    }
}
