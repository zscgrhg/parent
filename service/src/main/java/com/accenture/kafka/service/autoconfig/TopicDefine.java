package com.accenture.kafka.service.autoconfig;

import kafka.admin.RackAwareMode;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;

import java.util.Map;

/**
 * Created by THINK on 2016/11/16.
 */

@Builder
@EqualsAndHashCode(of = {"topic"})
public class TopicDefine {
    public final String topic;
    public final int partitions;
    public final int replicationFactor;

    @Singular
    public final Map topicConfigs;
    public final RackAwareMode rackAwareMode;
}
