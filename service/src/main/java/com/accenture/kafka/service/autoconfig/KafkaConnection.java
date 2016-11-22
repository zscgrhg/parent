package com.accenture.kafka.service.autoconfig;

import lombok.Builder;

/**
 * Created by THINK on 2016/11/16.
 */
@Builder
public class KafkaConnection {
    public final String zookeeperConnectionString;
    public final String brokersAddress;
    public final boolean isEmbedded;
}
