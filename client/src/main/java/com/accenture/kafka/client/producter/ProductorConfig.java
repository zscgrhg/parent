package com.accenture.kafka.client.producter;

import lombok.Builder;
import lombok.Data;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by THINK on 2016/11/22.
 */
@Data
@Builder
public class ProductorConfig {
    private final String bootstrapServers;
    private final Class<? extends Serializer> keySerializer;
    private final Class<? extends Serializer> valueSerializer;
    private final String acks;
    private final Long bufferMemory;
    private final String compressionType;
    private final Integer retries;
    private final String sslKeyPassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final Integer batchSize;
    private final String clientId;
    private final Long connectionsMaxIdleMs;
    private final Long lingerMs;
    private final Long maxBlockMs;
    private final Integer maxRequestSize;
    private final String partitionerClass;
    private final Integer receiveBufferBytes;
    private final Integer requestTimeoutMs;
    private final String saslKerberosServiceName;
    private final String saslMechanism;
    private final String securityProtocol;
    private final Integer sendBufferBytes;
    private final String sslEnabledProtocols;
    private final String sslKeystoreType;
    private final String sslProtocol;
    private final String sslProvider;
    private final String sslTruststoreType;
    private final Integer timeoutMs;
    private final Boolean blockOnBufferFull;
    private final String interceptorClasses;
    private final Integer maxInFlightRequestsPerConnection;
    private final Long metadataFetchTimeoutMs;
    private final Long metadataMaxAgeMs;
    private final String metricReporters;
    private final Integer metricsNumSamples;
    private final Long metricsSampleWindowMs;
    private final Long reconnectBackoffMs;
    private final Long retryBackoffMs;
    private final String saslKerberosKinitCmd;
    private final Long saslKerberosMinTimeBeforeRelogin;
    private final Double saslKerberosTicketRenewJitter;
    private final Double saslKerberosTicketRenewWindowFactor;
    private final String sslCipherSuites;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslKeymanagerAlgorithm;
    private final String sslSecureRandomImplementation;
    private final String sslTrustmanagerAlgorithm;

    public Map<String, Object> getProducerConfig() {
        Map<String, Object> map = new HashMap<>();
        putNonNull(map, "bootstrap.servers", bootstrapServers);
        putNonNull(map, "key.serializer", keySerializer);
        putNonNull(map, "value.serializer", valueSerializer);
        putNonNull(map, "acks", acks);
        putNonNull(map, "buffer.memory", bufferMemory);
        putNonNull(map, "compression.type", compressionType);
        putNonNull(map, "retries", retries);
        putNonNull(map, "ssl.key.password", sslKeyPassword);
        putNonNull(map, "ssl.keystore.location", sslKeystoreLocation);
        putNonNull(map, "ssl.keystore.password", sslKeystorePassword);
        putNonNull(map, "ssl.truststore.location", sslTruststoreLocation);
        putNonNull(map, "ssl.truststore.password", sslTruststorePassword);
        putNonNull(map, "batch.size", batchSize);
        putNonNull(map, "client.id", clientId);
        putNonNull(map, "connections.max.idle.ms", connectionsMaxIdleMs);
        putNonNull(map, "linger.ms", lingerMs);
        putNonNull(map, "max.block.ms", maxBlockMs);
        putNonNull(map, "max.request.size", maxRequestSize);
        putNonNull(map, "partitioner.class", partitionerClass);
        putNonNull(map, "receive.buffer.bytes", receiveBufferBytes);
        putNonNull(map, "request.timeout.ms", requestTimeoutMs);
        putNonNull(map, "sasl.kerberos.service.name", saslKerberosServiceName);
        putNonNull(map, "sasl.mechanism", saslMechanism);
        putNonNull(map, "security.protocol", securityProtocol);
        putNonNull(map, "send.buffer.bytes", sendBufferBytes);
        putNonNull(map, "ssl.enabled.protocols", sslEnabledProtocols);
        putNonNull(map, "ssl.keystore.type", sslKeystoreType);
        putNonNull(map, "ssl.protocol", sslProtocol);
        putNonNull(map, "ssl.provider", sslProvider);
        putNonNull(map, "ssl.truststore.type", sslTruststoreType);
        putNonNull(map, "timeout.ms", timeoutMs);
        putNonNull(map, "block.on.buffer.full", blockOnBufferFull);
        putNonNull(map, "interceptor.classes", interceptorClasses);
        putNonNull(map, "max.in.flight.requests.per.connection", maxInFlightRequestsPerConnection);
        putNonNull(map, "metadata.fetch.timeout.ms", metadataFetchTimeoutMs);
        putNonNull(map, "metadata.max.age.ms", metadataMaxAgeMs);
        putNonNull(map, "metric.reporters", metricReporters);
        putNonNull(map, "metrics.num.samples", metricsNumSamples);
        putNonNull(map, "metrics.sample.window.ms", metricsSampleWindowMs);
        putNonNull(map, "reconnect.backoff.ms", reconnectBackoffMs);
        putNonNull(map, "retry.backoff.ms", retryBackoffMs);
        putNonNull(map, "sasl.kerberos.kinit.cmd", saslKerberosKinitCmd);
        putNonNull(map, "sasl.kerberos.min.time.before.relogin", saslKerberosMinTimeBeforeRelogin);
        putNonNull(map, "sasl.kerberos.ticket.renew.jitter", saslKerberosTicketRenewJitter);
        putNonNull(map, "sasl.kerberos.ticket.renew.window.factor", saslKerberosTicketRenewWindowFactor);
        putNonNull(map, "ssl.cipher.suites", sslCipherSuites);
        putNonNull(map, "ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
        putNonNull(map, "ssl.keymanager.algorithm", sslKeymanagerAlgorithm);
        putNonNull(map, "ssl.secure.random.implementation", sslSecureRandomImplementation);
        putNonNull(map, "ssl.trustmanager.algorithm", sslTrustmanagerAlgorithm);
        return Collections.unmodifiableMap(map);
    }

    private final static <K, V> void putNonNull(Map<K, V> map, K key, V value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
