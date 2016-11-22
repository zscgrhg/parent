package com.accenture.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class ExampleApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ExampleApplication.class);
        Map<String, Object> defaultMap = new HashMap<String, Object>();
        defaultMap.put("kafka.local.broker-id-and-port.1", "8086");
        defaultMap.put("kafka.local.broker-id-and-port.2", "8087");
        defaultMap.put("kafka.local.broker-id-and-port.3", "8088");
        application.setDefaultProperties(defaultMap);
        application.run(args);
    }
}
