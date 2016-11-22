package com.accenture.kafka.service.utils;


import com.accenture.kafka.service.autoconfig.KafkaConnection;
import com.accenture.kafka.service.autoconfig.KafkaInspection;
import com.accenture.kafka.service.autoconfig.TopicDefine;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.requests.MetadataResponse;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by THINK on 2016/11/16.
 */
@Slf4j
public class KafkaUtil {


    private Set<String> fetchExistTopics(ZkUtils zkUtils) {
        Seq<String> allTopics = zkUtils.getAllTopics();
        List<String> t = JavaConversions.seqAsJavaList(allTopics);
        Set<String> topics = new HashSet<>();
        topics.addAll(t);
        return topics;
    }

    public void createTopics(Set<TopicDefine> topicDefines, KafkaConnection kafkaConnection) {
        ZkUtils zkUtils = createZkUtils(kafkaConnection);
        for (TopicDefine topicDefine : topicDefines) {
            if (AdminUtils.topicExists(zkUtils, topicDefine.topic)) {
                continue;
            }
            Properties p = new Properties();
            p.putAll(topicDefine.topicConfigs);
            try {
                AdminUtils.createTopic(zkUtils,
                        topicDefine.topic,
                        Math.max(topicDefine.partitions, 1),
                        Math.max(topicDefine.replicationFactor, 1),
                        p,
                        topicDefine.rackAwareMode);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public KafkaInspection getKafkaInfo(KafkaConnection kafkaConnection) {


        ZkUtils zkUtils = createZkUtils(kafkaConnection);
        Set<String> topics = fetchExistTopics(zkUtils);
        scala.collection.Set<MetadataResponse.TopicMetadata> topicMetadataSet
                = AdminUtils.fetchTopicMetadataFromZk(JavaConversions.asScalaSet(topics), zkUtils);
        Set<MetadataResponse.TopicMetadata> topicMetadatas = JavaConversions.setAsJavaSet(topicMetadataSet);
        int max = 1;
        Map<String, Integer> topicPartitions = new HashMap<>();
        for (MetadataResponse.TopicMetadata topicMetadata : topicMetadatas) {
            String topic = topicMetadata.topic();
            List<MetadataResponse.PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
            int size = partitionMetadatas.size();
            max = Math.max(max, size);
            topicPartitions.put(topic, size);
        }
        KafkaInspection kafkaInspection = KafkaInspection.builder().kafkaConnection(kafkaConnection)
                .topicPartitions(topicPartitions)
                .metadatas(topicMetadatas)
                .maxPartitions(max).build();
        return kafkaInspection;
    }

    private ZkUtils createZkUtils(KafkaConnection kafkaConnection) {
        ZkUtils zkUtils = new ZkUtils(new ZkClient(kafkaConnection.zookeeperConnectionString, 60 * 1000, 60 * 1000,
                ZKStringSerializer$.MODULE$), null, false);
        return zkUtils;
    }
}
