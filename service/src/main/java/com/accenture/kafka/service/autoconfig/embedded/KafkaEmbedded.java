package com.accenture.kafka.service.autoconfig.embedded;

import kafka.admin.AdminUtils;
import kafka.admin.AdminUtils$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.MetadataResponse;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Set;
import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by THINK on 2016/11/14.
 */
public class KafkaEmbedded {
    public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

    public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;

    public final int KAFKA_SERVER_PORT;

    private final int count;

    private final boolean controlledShutdown;

    private final String[] topics;

    private final int partitionsPerTopic;

    private final String logDirBase;
    private final String zookeeperDataDirBase;

    private List<KafkaServer> kafkaServers;

    private ZookeeperEmbedded zookeeper;

    private ZkClient zookeeperClient;

    private String zkConnect;

    public KafkaEmbedded(int port, int count, final String logDirBase, final String zookeeperDataDirBase) {
        this(port, count, false, logDirBase, zookeeperDataDirBase);
    }

    /**
     * Create embedded Kafka brokers.
     *  @param count              the number of brokers.
     * @param controlledShutdown passed into TestUtils.createBrokerConfig.
     * @param logDirBase
     * @param zookeeperDataDirBase
     * @param topics             the topics to create (2 partitions per).
     */
    public KafkaEmbedded(int port, int count, boolean controlledShutdown, final String logDirBase, final String zookeeperDataDirBase, String... topics) {
        this(port, count, controlledShutdown, 2, logDirBase, zookeeperDataDirBase, topics);
    }

    /**
     * Create embedded Kafka brokers.
     *  @param count              the number of brokers.
     * @param controlledShutdown passed into TestUtils.createBrokerConfig.
     * @param partitions         partitions per topic.
     * @param logDirBase
     * @param zookeeperDataDirBase
     * @param topics             the topics to create.
     */
    public KafkaEmbedded(int port, int count, boolean controlledShutdown, int partitions, final String logDirBase, final String zookeeperDataDirBase, String... topics) {
        this.KAFKA_SERVER_PORT = port;
        this.count = count;
        this.controlledShutdown = controlledShutdown;
        this.logDirBase = logDirBase;
        this.zookeeperDataDirBase = zookeeperDataDirBase;
        if (topics != null) {
            this.topics = topics;
        } else {
            this.topics = new String[0];
        }
        this.partitionsPerTopic = partitions;
    }


    public void start() throws Exception { //NOSONAR
        startZookeeper();
        int zkConnectionTimeout = 6000;
        int zkSessionTimeout = 6000;

        this.zkConnect = "127.0.0.1:" + this.zookeeper.getClientPort();
        this.zookeeperClient = new ZkClient(this.zkConnect, zkSessionTimeout, zkConnectionTimeout,
                ZKStringSerializer$.MODULE$);
        this.kafkaServers = new ArrayList<>();
        for (int i = 0; i < this.count; i++) {
            int kafkaServerPort = this.KAFKA_SERVER_PORT + i;
            Properties brokerConfigProperties = TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
                    true, kafkaServerPort,
                    scala.Option.<SecurityProtocol>apply(null),
                    scala.Option.<File>apply(null),
                    scala.Option.<Properties>apply(null),
                    true, false, 0, false, 0, false, 0, scala.Option.<String>apply(null));
            brokerConfigProperties.setProperty("replica.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("controller.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("offsets.topic.replication.factor", "1");
            brokerConfigProperties.setProperty("log.dirs", logDirBase + i);

            KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), SystemTime$.MODULE$);
            this.kafkaServers.add(server);
        }
        ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
        Properties props = new Properties();
        for (String topic : this.topics) {
            AdminUtils.createTopic(zkUtils, topic, this.partitionsPerTopic, this.count, props, null);
        }
        System.setProperty(SPRING_EMBEDDED_KAFKA_BROKERS, getBrokersAsString());
    }


    public void stop() {
        System.getProperties().remove(SPRING_EMBEDDED_KAFKA_BROKERS);
        for (KafkaServer kafkaServer : this.kafkaServers) {
            try {
                if (kafkaServer.brokerState().currentState() != (NotRunning.state())) {
                    kafkaServer.shutdown();
                    kafkaServer.awaitShutdown();
                }
            } catch (Exception e) {
                // do nothing
            }
            try {
                CoreUtils.delete(kafkaServer.config().logDirs());
            } catch (Exception e) {
                // do nothing
            }
        }
        try {
            this.zookeeperClient.close();
        } catch (ZkInterruptedException e) {
            // do nothing
        }
        try {
            this.zookeeper.stop();
        } catch (Exception e) {
            // do nothing
        }
    }


    public List<KafkaServer> getKafkaServers() {
        return this.kafkaServers;
    }

    public KafkaServer getKafkaServer(int id) {
        return this.kafkaServers.get(id);
    }

    public ZookeeperEmbedded getZookeeper() {
        return this.zookeeper;
    }


    public ZkClient getZkClient() {
        return this.zookeeperClient;
    }


    public String getZookeeperConnectionString() {
        return this.zkConnect;
    }


    public void startZookeeper() {
        this.zookeeper = new ZookeeperEmbedded(2181, zookeeperDataDirBase);
        this.zookeeper.start();
    }

    public void bounce(int index, boolean waitForPropagation) {
        this.kafkaServers.get(index).shutdown();
        if (waitForPropagation) {
            long initialTime = System.currentTimeMillis();
            boolean canExit = false;
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
                canExit = true;
                ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
                Map<String, Properties> topicProperties = AdminUtils$.MODULE$.fetchAllTopicConfigs(zkUtils);
                Set<MetadataResponse.TopicMetadata> topicMetadatas =
                        AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topicProperties.keySet(), zkUtils);
                for (MetadataResponse.TopicMetadata topicMetadata : JavaConversions.asJavaCollection(topicMetadatas)) {
                    if (Errors.forCode(topicMetadata.error().code()).exception() == null) {
                        for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
                            Collection<Node> inSyncReplicas = partitionMetadata.isr();
                            for (Node node : inSyncReplicas) {
                                if (node.id() == index) {
                                    canExit = false;
                                }
                            }
                        }
                    }
                }
            }
            while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
        }

    }

    public void bounce(int index) {
        bounce(index, true);
    }

    public void restart(final int index) throws Exception { //NOSONAR

        // retry restarting repeatedly, first attempts may fail

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10,
                Collections.<Class<? extends Throwable>, Boolean>singletonMap(Exception.class, true));

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(100);
        backOffPolicy.setMaxInterval(1000);
        backOffPolicy.setMultiplier(2);

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);


        retryTemplate.execute(new RetryCallback<Void, Exception>() {

            @Override
            public Void doWithRetry(RetryContext context) throws Exception { //NOSONAR
                KafkaEmbedded.this.kafkaServers.get(index).startup();
                return null;
            }
        });
    }

    public void waitUntilSynced(String topic, int brokerId) {
        long initialTime = System.currentTimeMillis();
        boolean canExit = false;
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
            canExit = true;
            ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
            MetadataResponse.TopicMetadata topicMetadata = AdminUtils$.MODULE$.fetchTopicMetadataFromZk(topic, zkUtils);
            if (Errors.forCode(topicMetadata.error().code()).exception() == null) {
                for (MetadataResponse.PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata()) {
                    Collection<Node> isr = partitionMetadata.isr();
                    boolean containsIndex = false;
                    for (Node node : isr) {
                        if (node.id() == brokerId) {
                            containsIndex = true;
                        }
                    }
                    if (!containsIndex) {
                        canExit = false;
                    }

                }
            }
        }
        while (!canExit && (System.currentTimeMillis() - initialTime < METADATA_PROPAGATION_TIMEOUT));
    }


    public String getBrokersAsString() {
        StringBuilder builder = new StringBuilder();
        for (KafkaServer kafkaServer : this.kafkaServers) {
            builder.append("127.0.0.1:").append(kafkaServer.config().port()).append(',');
        }
        return builder.substring(0, builder.length() - 1);
    }


    public boolean isEmbedded() {
        return true;
    }

    /**
     * Subscribe a consumer to all the embedded topics.
     *
     * @param consumer the consumer.
     * @throws Exception an exception.
     */
    public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) throws Exception {
        final CountDownLatch consumerLatch = new CountDownLatch(1);
        consumer.subscribe(Arrays.asList(this.topics), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumerLatch.countDown();
            }

        });
        consumer.poll(0); // force assignment
        assertThat(consumerLatch.await(30, TimeUnit.SECONDS))
                .as("Failed to be assigned partitions from the embedded topics")
                .isTrue();
    }

    /**
     * Subscribe a consumer to one of the embedded topics.
     *
     * @param consumer the consumer.
     * @param topic    the topic.
     * @throws Exception an exception.
     */
    public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic) throws Exception {
        assertThat(this.topics).as("topic is not in embedded topic list").contains(topic);
        final CountDownLatch consumerLatch = new CountDownLatch(1);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                consumerLatch.countDown();
            }

        });
        consumer.poll(0); // force assignment
        assertThat(consumerLatch.await(30, TimeUnit.SECONDS))
                .as("Failed to be assigned partitions from the embedded topics")
                .isTrue();
    }

}
