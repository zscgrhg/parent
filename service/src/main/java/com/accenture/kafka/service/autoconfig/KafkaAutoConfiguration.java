package com.accenture.kafka.service.autoconfig;


import com.accenture.kafka.service.autoconfig.embedded.KafkaEmbedded;
import com.accenture.kafka.service.autoconfig.local.ExecUtil;
import com.accenture.kafka.service.utils.KafkaUtil;
import com.accenture.kafka.shared.Profiles;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by THINK on 2016/11/17.
 */

@AutoConfigureAfter(PropertiesLoader.class)
@ConditionalOnClass({ZkClient.class, KafkaServer.class})
@EnableConfigurationProperties(
        {KafkaProperties.External.class,
                KafkaProperties.Broker.class,
                KafkaProperties.Local.class,
                KafkaProperties.Embedded.class,
                KafkaProperties.Consumer.class,
                KafkaProperties.Producer.class})
@Slf4j
@Data
public class KafkaAutoConfiguration {


    @Autowired(required = false)
    Set<TopicDefine> topicDefines;


    @Bean
    @ConditionalOnMissingBean
    KafkaUtil kafkaUtil() {
        return new KafkaUtil();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaInspection kafkaUpdater(
            KafkaConnection kafkaConnection, KafkaUtil kafkaUtil) {

        if (topicDefines != null
                && !topicDefines.isEmpty()) {
            kafkaUtil.createTopics(topicDefines, kafkaConnection);
        }
        return kafkaUtil.getKafkaInfo(kafkaConnection);
    }

    @Configuration
    @ConditionalOnClass(KafkaTemplate.class)
    public static class KafkaTemplateAutoConfiguration {
        @Autowired
        KafkaProperties.Producer producer;
        @Autowired
        KafkaConnection kafkaConnection;

        @Bean
        @ConditionalOnMissingBean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> producerConfig = producer.getProducerConfig();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnection.brokersAddress);
            return new DefaultKafkaProducerFactory<>(producerConfig);
        }


        @Bean
        @ConditionalOnMissingBean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(producerFactory());
        }

    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.external.enabled", havingValue = "true")
    @Profile(Profiles.PRODUCTION)
    public static class ExternalAutoConfiguration {


        @Bean
        @ConditionalOnProperty(name = {"kafka.zookeeperConnectionString", "kafka.brokersAddress"})
        @ConditionalOnMissingBean
        public KafkaConnection kafkaExternal(@Autowired
                                                     KafkaProperties.External external) throws Exception {

            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(external.getBrokersAddress())
                    .isEmbedded(false)
                    .zookeeperConnectionString(external.getZookeeperConnectionString()).build();
            return kafkaConnection;
        }

    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.local.enabled", havingValue = "true")
    @Profile(Profiles.NON_PRODUCTION)
    public static class LocalAutoConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public KafkaConnection kafkaLocal(@Autowired
                                                  KafkaProperties.Local local,
                                          KafkaProperties.Broker brokerProperties) throws Exception {
            int zookeeperPort = local.getZookeeperPort();
            Map<Integer, Integer> brokerIdAndPort = local.getBrokerIdAndPort();
            File kafkaHome = new File(local.getKafkaHome());
            File dataDir = new File(local.getDataDir());
            KafkaConnection kafkaConnection =
                    ExecUtil.startServer(kafkaHome,
                            dataDir,
                            Collections.<String,Object>singletonMap("clientPort", zookeeperPort),
                            brokerProperties.getBrokerConfig(), brokerIdAndPort);


            return kafkaConnection;
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "kafka.embedded.enabled", havingValue = "true")
    @ConditionalOnClass({TestUtils.class})
    @Profile(Profiles.NON_PRODUCTION)
    public static class EmbeddedAutoConfiguration {
        @Bean
        @ConditionalOnMissingBean
        public KafkaConnection kafkaEmbedded(@Autowired
                                                     KafkaProperties.Embedded embedded,
                                             KafkaProperties.Broker brokerProperties) throws Exception {
            KafkaEmbedded kafka = new KafkaEmbedded(
                    embedded.getPort(),
                    embedded.getBrokerCount(),
                    true,
                    embedded.getPartitions(),
                    embedded.getLogFilenamePattern(),
                    embedded.getZookeeperDataDir(),
                    brokerProperties.getBrokerConfig());
            kafka.start();
            String brokersAsString = kafka.getBrokersAsString();
            String zookeeperConnectionString = kafka.getZookeeperConnectionString();
            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(brokersAsString)
                    .isEmbedded(true)
                    .zookeeperConnectionString(zookeeperConnectionString).build();
            return kafkaConnection;
        }
    }

    @Configuration
    @ConditionalOnProperty(prefix = "kafka.consumer", value = "group-id")
    @ConditionalOnClass(KafkaListenerContainerFactory.class)
    public static class KafkaListenerContainerFactoryAutoConfiguration {

        public static final String CONTAINER_FACTORY = "kafkaListenerContainerFactory";
        public static final String BATCH_CONTAINER_FACTORY = "kafkaBatchListenerContainerFactory";
        @Autowired
        KafkaProperties.Consumer consumer;

        @Bean
        KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
        kafkaListenerContainerFactory(KafkaInspection kafkaInspection) throws Exception {
            ListenerCfbAdapater<String, String> adapater =
                    new ListenerCfbAdapater(kafkaInspection,
                            consumer.getConsumerConfig());
            return adapater.containerFactory();
        }


        @Bean
        @ConditionalOnProperty(prefix = "kafka.consumer", value = "fetch-min-bytes")
        public KafkaListenerContainerFactory<?> kafkaBatchListenerContainerFactory(KafkaInspection kafkaInspection) throws Exception {
            BatchListenerCfbAdapater<String, String> adapater =
                    new BatchListenerCfbAdapater(kafkaInspection,
                            consumer.getConsumerConfig());
            return adapater.containerFactory();
        }
    }
}
