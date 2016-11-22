package com.accenture.kafka.service.autoconfig;

import lombok.Builder;
import lombok.Singular;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.Map;
import java.util.Set;

@Builder
public class KafkaInspection {
    public final KafkaConnection kafkaConnection;
    public final int maxPartitions;
    @Singular
    public final Map<String, Integer> topicPartitions;
    @Singular
    public final Set<MetadataResponse.TopicMetadata> metadatas;
}
