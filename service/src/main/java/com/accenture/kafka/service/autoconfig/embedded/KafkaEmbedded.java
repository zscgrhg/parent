package com.accenture.kafka.service.autoconfig.embedded;

import kafka.admin.AdminUtils$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.NotRunning;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.common.Node;
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
import java.io.FileOutputStream;
import java.util.*;

import static com.accenture.kafka.service.utils.PropertiesUtil.store;

/**
 * Created by THINK on 2016/11/14.
 */
public class KafkaEmbedded {
    public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

    public static final long METADATA_PROPAGATION_TIMEOUT = 10000L;

    public final int KAFKA_SERVER_PORT;

    private final int count;

    private final boolean controlledShutdown;


    private final String logDirBase;
    private final String zookeeperDataDirBase;
    private final java.util.Map<String, Object> brokerConfigs;
    private List<KafkaServer> kafkaServers;

    private ZookeeperEmbedded zookeeper;

    private ZkClient zookeeperClient;

    private String zkConnect;
    final static String TIME = "T"+new Date().getTime();


    public KafkaEmbedded(int port, int count, final String logDirBase, final String zookeeperDataDirBase, final java.util.Map<String, Object> brokerConfigs) {
        this(port, count, false, logDirBase, zookeeperDataDirBase, brokerConfigs);
    }

    /**
     * Create embedded Kafka brokers.
     *
     * @param count                the number of brokers.
     * @param controlledShutdown   passed into TestUtils.createBrokerConfig.
     * @param logDirBase
     * @param zookeeperDataDirBase
     * @param brokerConfigs
     */
    public KafkaEmbedded(int port, int count, boolean controlledShutdown, final String logDirBase, final String zookeeperDataDirBase, final java.util.Map<String, Object> brokerConfigs) {
        this(port, count, controlledShutdown, 2, logDirBase, zookeeperDataDirBase, brokerConfigs);
    }

    /**
     * Create embedded Kafka brokers.
     *
     * @param count                the number of brokers.
     * @param controlledShutdown   passed into TestUtils.createBrokerConfig.
     * @param partitions           partitions per topic.
     * @param logDirBase
     * @param zookeeperDataDirBase
     * @param brokerConfigs
     */
    public KafkaEmbedded(int port, int count, boolean controlledShutdown, int partitions, final String logDirBase, final String zookeeperDataDirBase, final java.util.Map<String, Object> brokerConfigs) {
        this.KAFKA_SERVER_PORT = port;
        this.count = count;
        this.controlledShutdown = controlledShutdown;
        this.logDirBase = logDirBase;
        this.zookeeperDataDirBase = zookeeperDataDirBase;
        this.brokerConfigs = brokerConfigs;

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
            Properties p = new Properties();
            File tmp = new File(String.format(logDirBase,TIME,"broker_"+i+".properties"));
            File parentFile = tmp.getParentFile();
            if(!parentFile.exists()){
                parentFile.mkdirs();
            }
            p.putAll(brokerConfigs);
            p.put("broker.id", i);
            store(p, new FileOutputStream(tmp), "oops");
            Properties brokerConfigProperties = TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
                    true, kafkaServerPort,
                    scala.Option.<SecurityProtocol>apply(null),
                    scala.Option.<File>apply(tmp),
                    scala.Option.<Properties>apply(null),
                    true, false, 0, false, 0, false, 0, scala.Option.<String>apply(null));
            brokerConfigProperties.setProperty("replica.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("controller.socket.timeout.ms", "1000");
            brokerConfigProperties.setProperty("offsets.topic.replication.factor", "1");



            brokerConfigProperties.setProperty("log.dirs", String.format(logDirBase,TIME,i));

            KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), SystemTime$.MODULE$);
            this.kafkaServers.add(server);
        }
        ZkUtils zkUtils = new ZkUtils(getZkClient(), null, false);
        Properties props = new Properties();

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
        this.zookeeper = new ZookeeperEmbedded(2181, String.format(zookeeperDataDirBase,TIME));
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


}
