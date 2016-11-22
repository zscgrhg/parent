package com.accenture.kafka.service.autoconfig;

import lombok.Data;
import org.apache.kafka.common.serialization.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by THINK on 2016/11/17.
 */
@ConfigurationProperties("kafka")
@Data
public class KafkaProperties {
    @Data
    @ConfigurationProperties("kafka.external")
    public static class External {
        private boolean enabled = false;
        private String zookeeperConnectionString;
        private String brokersAddress;
    }


    @Data
    @ConfigurationProperties("kafka.local")
    public static class Local {
        private boolean enabled = false;
        private int zookeeperPort = 2181;
        private Map<Integer, Integer> brokerIdAndPort;
        private String kafkaHome;
        private String dataDir;
    }

    @Data
    @ConfigurationProperties("kafka.embedded")
    public static class Embedded {
        private boolean enabled = false;
        private String logFilenamePattern = "/tmp/kafka-logs";
        private String zookeeperDataDir = "/tmp/zk-logs";
        private int port;
        private int brokerCount = 1;
        private int partitions = 1;
    }

    /**
     * <a href="http://kafka.apache.org/documentation#newconsumerconfigs">Consumer configs</a>
     */
    @Data
    @ConfigurationProperties("kafka.consumer")
    public static class Consumer {
        StringDeserializer stringDeserializer;
        private String bootstrapServers;
        private Class<? extends Deserializer> keyDeserializer = IntegerDeserializer.class;
        private Class<? extends Deserializer> valueDeserializer = StringDeserializer.class;
        private Integer fetchMinBytes;
        private String groupId;
        private Integer heartbeatIntervalMs;
        private Integer maxPartitionFetchBytes;
        private Integer sessionTimeoutMs;
        private String sslKeyPassword;
        private String sslKeystoreLocation;
        private String sslKeystorePassword;
        private String sslTruststoreLocation;
        private String sslTruststorePassword;
        private String autoOffsetReset;
        private Long connectionsMaxIdleMs;
        private Boolean enableAutoCommit;
        private Boolean excludeInternalTopics;
        private Integer fetchMaxBytes;
        private Integer maxPollIntervalMs;
        private Integer maxPollRecords;
        private String partitionAssignmentStrategy;
        private Integer receiveBufferBytes;
        private Integer requestTimeoutMs;
        private String saslKerberosServiceName;
        private String saslMechanism;
        private String securityProtocol;
        private Integer sendBufferBytes;
        private String sslEnabledProtocols;
        private String sslKeystoreType;
        private String sslProtocol;
        private String sslProvider;
        private String sslTruststoreType;
        private Integer autoCommitIntervalMs;
        private Boolean checkCrcs;
        private String clientId;
        private Integer fetchMaxWaitMs;
        private String interceptorClasses;
        private Long metadataMaxAgeMs;
        private String metricReporters;
        private Integer metricsNumSamples;
        private Long metricsSampleWindowMs;
        private Long reconnectBackoffMs;
        private Long retryBackoffMs;
        private String saslKerberosKinitCmd;
        private Long saslKerberosMinTimeBeforeRelogin;
        private Double saslKerberosTicketRenewJitter;
        private Double saslKerberosTicketRenewWindowFactor;
        private String sslCipherSuites;
        private String sslEndpointIdentificationAlgorithm;
        private String sslKeymanagerAlgorithm;
        private String sslSecureRandomImplementation;
        private String sslTrustmanagerAlgorithm;

        public Map<String, Object> getConsumerConfig() {
            Map<String, Object> map = new HashMap<>();
            putIfNotNull(map, "bootstrap.servers", bootstrapServers);
            putIfNotNull(map, "key.deserializer", keyDeserializer);
            putIfNotNull(map, "value.deserializer", valueDeserializer);
            putIfNotNull(map, "fetch.min.bytes", fetchMinBytes);
            putIfNotNull(map, "group.id", groupId);
            putIfNotNull(map, "heartbeat.interval.ms", heartbeatIntervalMs);
            putIfNotNull(map, "max.partition.fetch.bytes", maxPartitionFetchBytes);
            putIfNotNull(map, "session.timeout.ms", sessionTimeoutMs);
            putIfNotNull(map, "ssl.key.password", sslKeyPassword);
            putIfNotNull(map, "ssl.keystore.location", sslKeystoreLocation);
            putIfNotNull(map, "ssl.keystore.password", sslKeystorePassword);
            putIfNotNull(map, "ssl.truststore.location", sslTruststoreLocation);
            putIfNotNull(map, "ssl.truststore.password", sslTruststorePassword);
            putIfNotNull(map, "auto.offset.reset", autoOffsetReset);
            putIfNotNull(map, "connections.max.idle.ms", connectionsMaxIdleMs);
            putIfNotNull(map, "enable.auto.commit", enableAutoCommit);
            putIfNotNull(map, "exclude.internal.topics", excludeInternalTopics);
            putIfNotNull(map, "fetch.max.bytes", fetchMaxBytes);
            putIfNotNull(map, "max.poll.interval.ms", maxPollIntervalMs);
            putIfNotNull(map, "max.poll.records", maxPollRecords);
            putIfNotNull(map, "partition.assignment.strategy", partitionAssignmentStrategy);
            putIfNotNull(map, "receive.buffer.bytes", receiveBufferBytes);
            putIfNotNull(map, "request.timeout.ms", requestTimeoutMs);
            putIfNotNull(map, "sasl.kerberos.service.name", saslKerberosServiceName);
            putIfNotNull(map, "sasl.mechanism", saslMechanism);
            putIfNotNull(map, "security.protocol", securityProtocol);
            putIfNotNull(map, "send.buffer.bytes", sendBufferBytes);
            putIfNotNull(map, "ssl.enabled.protocols", sslEnabledProtocols);
            putIfNotNull(map, "ssl.keystore.type", sslKeystoreType);
            putIfNotNull(map, "ssl.protocol", sslProtocol);
            putIfNotNull(map, "ssl.provider", sslProvider);
            putIfNotNull(map, "ssl.truststore.type", sslTruststoreType);
            putIfNotNull(map, "auto.commit.interval.ms", autoCommitIntervalMs);
            putIfNotNull(map, "check.crcs", checkCrcs);
            putIfNotNull(map, "client.id", clientId);
            putIfNotNull(map, "fetch.max.wait.ms", fetchMaxWaitMs);
            putIfNotNull(map, "interceptor.classes", interceptorClasses);
            putIfNotNull(map, "metadata.max.age.ms", metadataMaxAgeMs);
            putIfNotNull(map, "metric.reporters", metricReporters);
            putIfNotNull(map, "metrics.num.samples", metricsNumSamples);
            putIfNotNull(map, "metrics.sample.window.ms", metricsSampleWindowMs);
            putIfNotNull(map, "reconnect.backoff.ms", reconnectBackoffMs);
            putIfNotNull(map, "retry.backoff.ms", retryBackoffMs);
            putIfNotNull(map, "sasl.kerberos.kinit.cmd", saslKerberosKinitCmd);
            putIfNotNull(map, "sasl.kerberos.min.time.before.relogin", saslKerberosMinTimeBeforeRelogin);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.jitter", saslKerberosTicketRenewJitter);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.window.factor", saslKerberosTicketRenewWindowFactor);
            putIfNotNull(map, "ssl.cipher.suites", sslCipherSuites);
            putIfNotNull(map, "ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
            putIfNotNull(map, "ssl.keymanager.algorithm", sslKeymanagerAlgorithm);
            putIfNotNull(map, "ssl.secure.random.implementation", sslSecureRandomImplementation);
            putIfNotNull(map, "ssl.trustmanager.algorithm", sslTrustmanagerAlgorithm);
            return map;
        }
    }

    /**
     * <a href="http://kafka.apache.org/documentation#producerconfigs">Producer configs</a>
     */
    @Data
    @ConfigurationProperties("kafka.producer")
    public static class Producer {
        private String bootstrapServers;
        private Class<? extends Serializer> keySerializer = IntegerSerializer.class;
        private Class<? extends Serializer> valueSerializer = StringSerializer.class;
        private String acks;
        private Long bufferMemory;
        private String compressionType;
        private Integer retries;
        private String sslKeyPassword;
        private String sslKeystoreLocation;
        private String sslKeystorePassword;
        private String sslTruststoreLocation;
        private String sslTruststorePassword;
        private Integer batchSize;
        private String clientId;
        private Long connectionsMaxIdleMs;
        private Long lingerMs;
        private Long maxBlockMs;
        private Integer maxRequestSize;
        private String partitionerClass;
        private Integer receiveBufferBytes;
        private Integer requestTimeoutMs;
        private String saslKerberosServiceName;
        private String saslMechanism;
        private String securityProtocol;
        private Integer sendBufferBytes;
        private String sslEnabledProtocols;
        private String sslKeystoreType;
        private String sslProtocol;
        private String sslProvider;
        private String sslTruststoreType;
        private Integer timeoutMs;
        private Boolean blockOnBufferFull;
        private String interceptorClasses;
        private Integer maxInFlightRequestsPerConnection;
        private Long metadataFetchTimeoutMs;
        private Long metadataMaxAgeMs;
        private String metricReporters;
        private Integer metricsNumSamples;
        private Long metricsSampleWindowMs;
        private Long reconnectBackoffMs;
        private Long retryBackoffMs;
        private String saslKerberosKinitCmd;
        private Long saslKerberosMinTimeBeforeRelogin;
        private Double saslKerberosTicketRenewJitter;
        private Double saslKerberosTicketRenewWindowFactor;
        private String sslCipherSuites;
        private String sslEndpointIdentificationAlgorithm;
        private String sslKeymanagerAlgorithm;
        private String sslSecureRandomImplementation;
        private String sslTrustmanagerAlgorithm;

        public Map<String, Object> getProducerConfig() {
            Map<String, Object> map = new HashMap<>();
            putIfNotNull(map, "bootstrap.servers", bootstrapServers);
            putIfNotNull(map, "key.serializer", keySerializer);
            putIfNotNull(map, "value.serializer", valueSerializer);
            putIfNotNull(map, "acks", acks);
            putIfNotNull(map, "buffer.memory", bufferMemory);
            putIfNotNull(map, "compression.type", compressionType);
            putIfNotNull(map, "retries", retries);
            putIfNotNull(map, "ssl.key.password", sslKeyPassword);
            putIfNotNull(map, "ssl.keystore.location", sslKeystoreLocation);
            putIfNotNull(map, "ssl.keystore.password", sslKeystorePassword);
            putIfNotNull(map, "ssl.truststore.location", sslTruststoreLocation);
            putIfNotNull(map, "ssl.truststore.password", sslTruststorePassword);
            putIfNotNull(map, "batch.size", batchSize);
            putIfNotNull(map, "client.id", clientId);
            putIfNotNull(map, "connections.max.idle.ms", connectionsMaxIdleMs);
            putIfNotNull(map, "linger.ms", lingerMs);
            putIfNotNull(map, "max.block.ms", maxBlockMs);
            putIfNotNull(map, "max.request.size", maxRequestSize);
            putIfNotNull(map, "partitioner.class", partitionerClass);
            putIfNotNull(map, "receive.buffer.bytes", receiveBufferBytes);
            putIfNotNull(map, "request.timeout.ms", requestTimeoutMs);
            putIfNotNull(map, "sasl.kerberos.service.name", saslKerberosServiceName);
            putIfNotNull(map, "sasl.mechanism", saslMechanism);
            putIfNotNull(map, "security.protocol", securityProtocol);
            putIfNotNull(map, "send.buffer.bytes", sendBufferBytes);
            putIfNotNull(map, "ssl.enabled.protocols", sslEnabledProtocols);
            putIfNotNull(map, "ssl.keystore.type", sslKeystoreType);
            putIfNotNull(map, "ssl.protocol", sslProtocol);
            putIfNotNull(map, "ssl.provider", sslProvider);
            putIfNotNull(map, "ssl.truststore.type", sslTruststoreType);
            putIfNotNull(map, "timeout.ms", timeoutMs);
            putIfNotNull(map, "block.on.buffer.full", blockOnBufferFull);
            putIfNotNull(map, "interceptor.classes", interceptorClasses);
            putIfNotNull(map, "max.in.flight.requests.per.connection", maxInFlightRequestsPerConnection);
            putIfNotNull(map, "metadata.fetch.timeout.ms", metadataFetchTimeoutMs);
            putIfNotNull(map, "metadata.max.age.ms", metadataMaxAgeMs);
            putIfNotNull(map, "metric.reporters", metricReporters);
            putIfNotNull(map, "metrics.num.samples", metricsNumSamples);
            putIfNotNull(map, "metrics.sample.window.ms", metricsSampleWindowMs);
            putIfNotNull(map, "reconnect.backoff.ms", reconnectBackoffMs);
            putIfNotNull(map, "retry.backoff.ms", retryBackoffMs);
            putIfNotNull(map, "sasl.kerberos.kinit.cmd", saslKerberosKinitCmd);
            putIfNotNull(map, "sasl.kerberos.min.time.before.relogin", saslKerberosMinTimeBeforeRelogin);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.jitter", saslKerberosTicketRenewJitter);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.window.factor", saslKerberosTicketRenewWindowFactor);
            putIfNotNull(map, "ssl.cipher.suites", sslCipherSuites);
            putIfNotNull(map, "ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
            putIfNotNull(map, "ssl.keymanager.algorithm", sslKeymanagerAlgorithm);
            putIfNotNull(map, "ssl.secure.random.implementation", sslSecureRandomImplementation);
            putIfNotNull(map, "ssl.trustmanager.algorithm", sslTrustmanagerAlgorithm);
            return map;
        }
    }

    private static <K, V> void putIfNotNull(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}