package com.accenture.kafka.service.autoconfig;


import com.accenture.kafka.service.server.KafkaStarter;
import com.accenture.kafka.service.server.ZkStarter;
import com.accenture.kafka.service.utils.KafkaUtil;
import com.accenture.kafka.shared.Profiles;
import kafka.server.KafkaServer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
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

import java.util.*;

/**
 * Created by THINK on 2016/11/17.
 */


@ConditionalOnClass({ZkClient.class, KafkaServer.class})
@EnableConfigurationProperties(
        {KafkaProperties.Production.class,
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
    @ConditionalOnProperty(name = "kafka.production.enabled", havingValue = "true")
    @Profile(Profiles.PRODUCTION)
    public static class ProductionAutoConfiguration {


        @Bean
        @ConditionalOnProperty(name = {"kafka.zookeeperConnectionString", "kafka.brokersAddress"})
        @ConditionalOnMissingBean
        public KafkaConnection kafkaProduction(@Autowired
                                                       KafkaProperties.Production production) throws Exception {

            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(production.getBrokersAddress())
                    .isEmbedded(false)
                    .zookeeperConnectionString(production.getZookeeperConnectionString()).build();
            return kafkaConnection;
        }

    }


    @Configuration
    @ConditionalOnProperty(name = "kafka.embedded.enabled", havingValue = "true")
    @ConditionalOnClass({KafkaStarter.class, ZkStarter.class})
    @Profile(Profiles.NON_PRODUCTION)
    public static class EmbeddedAutoConfiguration {
        @Bean
        @ConditionalOnMissingBean
        public KafkaConnection kafkaEmbedded(@Autowired
                                                     KafkaProperties.Embedded embedded) throws Exception {
            Collection<String> kafkaConfigPath = embedded.getKafkaConfigPath().values();
            Set<String> set=new HashSet<>();
            set.addAll(kafkaConfigPath);
            String zkConfigPath = embedded.getZkConfigPath();
            KafkaStarter kafkaStarter = new KafkaStarter(zkConfigPath, set);
            kafkaStarter.start();
            KafkaConnection kafkaConnection = KafkaConnection.builder().brokersAddress(kafkaStarter.getBrokersAddress())
                    .isEmbedded(true)
                    .zookeeperConnectionString(kafkaStarter.getZkAddress()).build();
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
