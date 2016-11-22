package com.accenture.kafka.service.autoconfig;

import lombok.Data;
import org.apache.kafka.common.serialization.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
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
    @ConfigurationProperties("kafka.broker")
    public static class Broker {
        private String zookeeperConnect;
        private String advertisedHostName;
        private String advertisedListeners;
        private Integer advertisedPort;
        private Boolean autoCreateTopicsEnable;
        private Boolean autoLeaderRebalanceEnable;
        private Integer backgroundThreads;
        private Integer brokerId;
        private String compressionType;
        private Boolean deleteTopicEnable;
        private String hostName;
        private Long leaderImbalanceCheckIntervalSeconds;
        private Integer leaderImbalancePerBrokerPercentage;
        private String listeners;
        private String logDir;
        private String logDirs;
        private Long logFlushIntervalMessages;
        private Long logFlushIntervalMs;
        private Integer logFlushOffsetCheckpointIntervalMs;
        private Long logFlushSchedulerIntervalMs;
        private Long logRetentionBytes;
        private Integer logRetentionHours;
        private Integer logRetentionMinutes;
        private Long logRetentionMs;
        private Integer logRollHours;
        private Integer logRollJitterHours;
        private Long logRollJitterMs;
        private Long logRollMs;
        private Integer logSegmentBytes;
        private Long logSegmentDeleteDelayMs;
        private Integer messageMaxBytes;
        private Integer minInsyncReplicas;
        private Integer numIoThreads;
        private Integer numNetworkThreads;
        private Integer numRecoveryThreadsPerDataDir;
        private Integer numReplicaFetchers;
        private Integer offsetMetadataMaxBytes;
        private Short offsetsCommitRequiredAcks;
        private Integer offsetsCommitTimeoutMs;
        private Integer offsetsLoadBufferSize;
        private Long offsetsRetentionCheckIntervalMs;
        private Integer offsetsRetentionMinutes;
        private Integer offsetsTopicCompressionCodec;
        private Integer offsetsTopicNumPartitions;
        private Short offsetsTopicReplicationFactor;
        private Integer offsetsTopicSegmentBytes;
        private Integer port;
        private Integer queuedMaxRequests;
        private Long quotaConsumerDefault;
        private Long quotaProducerDefault;
        private Integer replicaFetchMinBytes;
        private Integer replicaFetchWaitMaxMs;
        private Long replicaHighWatermarkCheckpointIntervalMs;
        private Long replicaLagTimeMaxMs;
        private Integer replicaSocketReceiveBufferBytes;
        private Integer replicaSocketTimeoutMs;
        private Integer requestTimeoutMs;
        private Integer socketReceiveBufferBytes;
        private Integer socketRequestMaxBytes;
        private Integer socketSendBufferBytes;
        private Boolean uncleanLeaderElectionEnable;
        private Integer zookeeperConnectionTimeoutMs;
        private Integer zookeeperSessionTimeoutMs;
        private Boolean zookeeperSetAcl;
        private Boolean brokerIdGenerationEnable;
        private String brokerRack;
        private Long connectionsMaxIdleMs;
        private Boolean controlledShutdownEnable;
        private Integer controlledShutdownMaxRetries;
        private Long controlledShutdownRetryBackoffMs;
        private Integer controllerSocketTimeoutMs;
        private Integer defaultReplicationFactor;
        private Integer fetchPurgatoryPurgeIntervalRequests;
        private Integer groupMaxSessionTimeoutMs;
        private Integer groupMinSessionTimeoutMs;
        private String interBrokerProtocolVersion;
        private Long logCleanerBackoffMs;
        private Long logCleanerDedupeBufferSize;
        private Long logCleanerDeleteRetentionMs;
        private Boolean logCleanerEnable;
        private Double logCleanerIoBufferLoadFactor;
        private Integer logCleanerIoBufferSize;
        private Double logCleanerIoMaxBytesPerSecond;
        private Double logCleanerMinCleanableRatio;
        private Long logCleanerMinCompactionLagMs;
        private Integer logCleanerThreads;
        private String logCleanupPolicy;
        private Integer logIndexIntervalBytes;
        private Integer logIndexSizeMaxBytes;
        private String logMessageFormatVersion;
        private Long logMessageTimestampDifferenceMaxMs;
        private String logMessageTimestampType;
        private Boolean logPreallocate;
        private Long logRetentionCheckIntervalMs;
        private Integer maxConnectionsPerIp;
        private String maxConnectionsPerIpOverrides;
        private Integer numPartitions;
        private Class principalBuilderClass;
        private Integer producerPurgatoryPurgeIntervalRequests;
        private Integer replicaFetchBackoffMs;
        private Integer replicaFetchMaxBytes;
        private Integer replicaFetchResponseMaxBytes;
        private Integer reservedBrokerMaxId;
        private String saslEnabledMechanisms;
        private String saslKerberosKinitCmd;
        private Long saslKerberosMinTimeBeforeRelogin;
        private String saslKerberosPrincipalToLocalRules;
        private String saslKerberosServiceName;
        private Double saslKerberosTicketRenewJitter;
        private Double saslKerberosTicketRenewWindowFactor;
        private String saslMechanismInterBrokerProtocol;
        private String securityInterBrokerProtocol;
        private String sslCipherSuites;
        private String sslClientAuth;
        private String sslEnabledProtocols;
        private String sslKeyPassword;
        private String sslKeymanagerAlgorithm;
        private String sslKeystoreLocation;
        private String sslKeystorePassword;
        private String sslKeystoreType;
        private String sslProtocol;
        private String sslProvider;
        private String sslTrustmanagerAlgorithm;
        private String sslTruststoreLocation;
        private String sslTruststorePassword;
        private String sslTruststoreType;
        private String authorizerClassName;
        private String metricReporters;
        private Integer metricsNumSamples;
        private Long metricsSampleWindowMs;
        private Integer quotaWindowNum;
        private Integer quotaWindowSizeSeconds;
        private Integer replicationQuotaWindowNum;
        private Integer replicationQuotaWindowSizeSeconds;
        private String sslEndpointIdentificationAlgorithm;
        private String sslSecureRandomImplementation;
        private Integer zookeeperSyncTimeMs;

        public Map<String, Object> getBrokerConfig() {
            Map<String, Object> map = new HashMap<>();
            putIfNotNull(map, "zookeeper.connect", zookeeperConnect);
            putIfNotNull(map, "advertised.host.name", advertisedHostName);
            putIfNotNull(map, "advertised.listeners", advertisedListeners);
            putIfNotNull(map, "advertised.port", advertisedPort);
            putIfNotNull(map, "auto.create.topics.enable", autoCreateTopicsEnable);
            putIfNotNull(map, "auto.leader.rebalance.enable", autoLeaderRebalanceEnable);
            putIfNotNull(map, "background.threads", backgroundThreads);
            putIfNotNull(map, "broker.id", brokerId);
            putIfNotNull(map, "compression.type", compressionType);
            putIfNotNull(map, "delete.topic.enable", deleteTopicEnable);
            putIfNotNull(map, "host.name", hostName);
            putIfNotNull(map, "leader.imbalance.check.interval.seconds", leaderImbalanceCheckIntervalSeconds);
            putIfNotNull(map, "leader.imbalance.per.broker.percentage", leaderImbalancePerBrokerPercentage);
            putIfNotNull(map, "listeners", listeners);
            putIfNotNull(map, "log.dir", logDir);
            putIfNotNull(map, "log.dirs", logDirs);
            putIfNotNull(map, "log.flush.interval.messages", logFlushIntervalMessages);
            putIfNotNull(map, "log.flush.interval.ms", logFlushIntervalMs);
            putIfNotNull(map, "log.flush.offset.checkpoint.interval.ms", logFlushOffsetCheckpointIntervalMs);
            putIfNotNull(map, "log.flush.scheduler.interval.ms", logFlushSchedulerIntervalMs);
            putIfNotNull(map, "log.retention.bytes", logRetentionBytes);
            putIfNotNull(map, "log.retention.hours", logRetentionHours);
            putIfNotNull(map, "log.retention.minutes", logRetentionMinutes);
            putIfNotNull(map, "log.retention.ms", logRetentionMs);
            putIfNotNull(map, "log.roll.hours", logRollHours);
            putIfNotNull(map, "log.roll.jitter.hours", logRollJitterHours);
            putIfNotNull(map, "log.roll.jitter.ms", logRollJitterMs);
            putIfNotNull(map, "log.roll.ms", logRollMs);
            putIfNotNull(map, "log.segment.bytes", logSegmentBytes);
            putIfNotNull(map, "log.segment.delete.delay.ms", logSegmentDeleteDelayMs);
            putIfNotNull(map, "message.max.bytes", messageMaxBytes);
            putIfNotNull(map, "min.insync.replicas", minInsyncReplicas);
            putIfNotNull(map, "num.io.threads", numIoThreads);
            putIfNotNull(map, "num.network.threads", numNetworkThreads);
            putIfNotNull(map, "num.recovery.threads.per.data.dir", numRecoveryThreadsPerDataDir);
            putIfNotNull(map, "num.replica.fetchers", numReplicaFetchers);
            putIfNotNull(map, "offset.metadata.max.bytes", offsetMetadataMaxBytes);
            putIfNotNull(map, "offsets.commit.required.acks", offsetsCommitRequiredAcks);
            putIfNotNull(map, "offsets.commit.timeout.ms", offsetsCommitTimeoutMs);
            putIfNotNull(map, "offsets.load.buffer.size", offsetsLoadBufferSize);
            putIfNotNull(map, "offsets.retention.check.interval.ms", offsetsRetentionCheckIntervalMs);
            putIfNotNull(map, "offsets.retention.minutes", offsetsRetentionMinutes);
            putIfNotNull(map, "offsets.topic.compression.codec", offsetsTopicCompressionCodec);
            putIfNotNull(map, "offsets.topic.num.partitions", offsetsTopicNumPartitions);
            putIfNotNull(map, "offsets.topic.replication.factor", offsetsTopicReplicationFactor);
            putIfNotNull(map, "offsets.topic.segment.bytes", offsetsTopicSegmentBytes);
            putIfNotNull(map, "port", port);
            putIfNotNull(map, "queued.max.requests", queuedMaxRequests);
            putIfNotNull(map, "quota.consumer.default", quotaConsumerDefault);
            putIfNotNull(map, "quota.producer.default", quotaProducerDefault);
            putIfNotNull(map, "replica.fetch.min.bytes", replicaFetchMinBytes);
            putIfNotNull(map, "replica.fetch.wait.max.ms", replicaFetchWaitMaxMs);
            putIfNotNull(map, "replica.high.watermark.checkpoint.interval.ms", replicaHighWatermarkCheckpointIntervalMs);
            putIfNotNull(map, "replica.lag.time.max.ms", replicaLagTimeMaxMs);
            putIfNotNull(map, "replica.socket.receive.buffer.bytes", replicaSocketReceiveBufferBytes);
            putIfNotNull(map, "replica.socket.timeout.ms", replicaSocketTimeoutMs);
            putIfNotNull(map, "request.timeout.ms", requestTimeoutMs);
            putIfNotNull(map, "socket.receive.buffer.bytes", socketReceiveBufferBytes);
            putIfNotNull(map, "socket.request.max.bytes", socketRequestMaxBytes);
            putIfNotNull(map, "socket.send.buffer.bytes", socketSendBufferBytes);
            putIfNotNull(map, "unclean.leader.election.enable", uncleanLeaderElectionEnable);
            putIfNotNull(map, "zookeeper.connection.timeout.ms", zookeeperConnectionTimeoutMs);
            putIfNotNull(map, "zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);
            putIfNotNull(map, "zookeeper.set.acl", zookeeperSetAcl);
            putIfNotNull(map, "broker.id.generation.enable", brokerIdGenerationEnable);
            putIfNotNull(map, "broker.rack", brokerRack);
            putIfNotNull(map, "connections.max.idle.ms", connectionsMaxIdleMs);
            putIfNotNull(map, "controlled.shutdown.enable", controlledShutdownEnable);
            putIfNotNull(map, "controlled.shutdown.max.retries", controlledShutdownMaxRetries);
            putIfNotNull(map, "controlled.shutdown.retry.backoff.ms", controlledShutdownRetryBackoffMs);
            putIfNotNull(map, "controller.socket.timeout.ms", controllerSocketTimeoutMs);
            putIfNotNull(map, "default.replication.factor", defaultReplicationFactor);
            putIfNotNull(map, "fetch.purgatory.purge.interval.requests", fetchPurgatoryPurgeIntervalRequests);
            putIfNotNull(map, "group.max.session.timeout.ms", groupMaxSessionTimeoutMs);
            putIfNotNull(map, "group.min.session.timeout.ms", groupMinSessionTimeoutMs);
            putIfNotNull(map, "inter.broker.protocol.version", interBrokerProtocolVersion);
            putIfNotNull(map, "log.cleaner.backoff.ms", logCleanerBackoffMs);
            putIfNotNull(map, "log.cleaner.dedupe.buffer.size", logCleanerDedupeBufferSize);
            putIfNotNull(map, "log.cleaner.delete.retention.ms", logCleanerDeleteRetentionMs);
            putIfNotNull(map, "log.cleaner.enable", logCleanerEnable);
            putIfNotNull(map, "log.cleaner.io.buffer.load.factor", logCleanerIoBufferLoadFactor);
            putIfNotNull(map, "log.cleaner.io.buffer.size", logCleanerIoBufferSize);
            putIfNotNull(map, "log.cleaner.io.max.bytes.per.second", logCleanerIoMaxBytesPerSecond);
            putIfNotNull(map, "log.cleaner.min.cleanable.ratio", logCleanerMinCleanableRatio);
            putIfNotNull(map, "log.cleaner.min.compaction.lag.ms", logCleanerMinCompactionLagMs);
            putIfNotNull(map, "log.cleaner.threads", logCleanerThreads);
            putIfNotNull(map, "log.cleanup.policy", logCleanupPolicy);
            putIfNotNull(map, "log.index.interval.bytes", logIndexIntervalBytes);
            putIfNotNull(map, "log.index.size.max.bytes", logIndexSizeMaxBytes);
            putIfNotNull(map, "log.message.format.version", logMessageFormatVersion);
            putIfNotNull(map, "log.message.timestamp.difference.max.ms", logMessageTimestampDifferenceMaxMs);
            putIfNotNull(map, "log.message.timestamp.type", logMessageTimestampType);
            putIfNotNull(map, "log.preallocate", logPreallocate);
            putIfNotNull(map, "log.retention.check.interval.ms", logRetentionCheckIntervalMs);
            putIfNotNull(map, "max.connections.per.ip", maxConnectionsPerIp);
            putIfNotNull(map, "max.connections.per.ip.overrides", maxConnectionsPerIpOverrides);
            putIfNotNull(map, "num.partitions", numPartitions);
            putIfNotNull(map, "principal.builder.class", principalBuilderClass);
            putIfNotNull(map, "producer.purgatory.purge.interval.requests", producerPurgatoryPurgeIntervalRequests);
            putIfNotNull(map, "replica.fetch.backoff.ms", replicaFetchBackoffMs);
            putIfNotNull(map, "replica.fetch.max.bytes", replicaFetchMaxBytes);
            putIfNotNull(map, "replica.fetch.response.max.bytes", replicaFetchResponseMaxBytes);
            putIfNotNull(map, "reserved.broker.max.id", reservedBrokerMaxId);
            putIfNotNull(map, "sasl.enabled.mechanisms", saslEnabledMechanisms);
            putIfNotNull(map, "sasl.kerberos.kinit.cmd", saslKerberosKinitCmd);
            putIfNotNull(map, "sasl.kerberos.min.time.before.relogin", saslKerberosMinTimeBeforeRelogin);
            putIfNotNull(map, "sasl.kerberos.principal.to.local.rules", saslKerberosPrincipalToLocalRules);
            putIfNotNull(map, "sasl.kerberos.service.name", saslKerberosServiceName);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.jitter", saslKerberosTicketRenewJitter);
            putIfNotNull(map, "sasl.kerberos.ticket.renew.window.factor", saslKerberosTicketRenewWindowFactor);
            putIfNotNull(map, "sasl.mechanism.inter.broker.protocol", saslMechanismInterBrokerProtocol);
            putIfNotNull(map, "security.inter.broker.protocol", securityInterBrokerProtocol);
            putIfNotNull(map, "ssl.cipher.suites", sslCipherSuites);
            putIfNotNull(map, "ssl.client.auth", sslClientAuth);
            putIfNotNull(map, "ssl.enabled.protocols", sslEnabledProtocols);
            putIfNotNull(map, "ssl.key.password", sslKeyPassword);
            putIfNotNull(map, "ssl.keymanager.algorithm", sslKeymanagerAlgorithm);
            putIfNotNull(map, "ssl.keystore.location", sslKeystoreLocation);
            putIfNotNull(map, "ssl.keystore.password", sslKeystorePassword);
            putIfNotNull(map, "ssl.keystore.type", sslKeystoreType);
            putIfNotNull(map, "ssl.protocol", sslProtocol);
            putIfNotNull(map, "ssl.provider", sslProvider);
            putIfNotNull(map, "ssl.trustmanager.algorithm", sslTrustmanagerAlgorithm);
            putIfNotNull(map, "ssl.truststore.location", sslTruststoreLocation);
            putIfNotNull(map, "ssl.truststore.password", sslTruststorePassword);
            putIfNotNull(map, "ssl.truststore.type", sslTruststoreType);
            putIfNotNull(map, "authorizer.class.name", authorizerClassName);
            putIfNotNull(map, "metric.reporters", metricReporters);
            putIfNotNull(map, "metrics.num.samples", metricsNumSamples);
            putIfNotNull(map, "metrics.sample.window.ms", metricsSampleWindowMs);
            putIfNotNull(map, "quota.window.num", quotaWindowNum);
            putIfNotNull(map, "quota.window.size.seconds", quotaWindowSizeSeconds);
            putIfNotNull(map, "replication.quota.window.num", replicationQuotaWindowNum);
            putIfNotNull(map, "replication.quota.window.size.seconds", replicationQuotaWindowSizeSeconds);
            putIfNotNull(map, "ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
            putIfNotNull(map, "ssl.secure.random.implementation", sslSecureRandomImplementation);
            putIfNotNull(map, "zookeeper.sync.time.ms", zookeeperSyncTimeMs);
            return map;
        }
    }

    @Data
    @ConfigurationProperties("kafka.local")
    public static class Local {
        private boolean enabled = false;
        private Integer zookeeperPort = 2181;
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
        private Integer port;
        private Integer brokerCount = 3;
        private Integer partitions = 1;
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