package com.accenture.kafka.service.autoconfig;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by THINK on 2016/11/21.
 */
@Configuration
@PropertySource("classpath:/kafka/application.properties")
public class PropertiesLoader {
}
