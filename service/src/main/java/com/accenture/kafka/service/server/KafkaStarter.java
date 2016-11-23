package com.accenture.kafka.service.server;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.SecurityProtocol;
import scala.collection.JavaConversions;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by THINK on 2016/11/23.
 */
@Slf4j
public class KafkaStarter {


    public static final String HOST_PORT_FORMAT = "%s:%d";

    private final ZkStarter zkStarter;
    private List<KafkaServer> kafkaServers;
    private final List<Properties> brokerProperties;

    public KafkaStarter(final String zkProperties, Set<String> brokersConfig) {
        this.zkStarter = new ZkStarter(zkProperties);
        if (brokersConfig != null) {
            List<Properties> list = new ArrayList<>();
            for (String s : brokersConfig) {
                try (InputStream in = getClass().getResourceAsStream(s)) {
                    Properties p = new Properties();
                    p.load(in);
                    list.add(p);
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
            this.brokerProperties = Collections.unmodifiableList(list);
        } else {
            this.brokerProperties = Collections.emptyList();
        }
    }

    private void sleepSafely(long t) {
        try {
            Thread.sleep(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized void start() {
        zkStarter.start();
        sleepSafely(1000);
        this.kafkaServers = new ArrayList<>();
        for (Properties brokerProperty : brokerProperties) {
            KafkaConfig config = new KafkaConfig(brokerProperty);
            KafkaServer server = new KafkaServer(config, SystemTime$.MODULE$, scala.Option.<String>apply("KafkaServer"));
            server.startup();
            this.kafkaServers.add(server);
        }
        boolean started = false;
        while (!started) {
            started = true;
            for (KafkaServer kafkaServer : this.kafkaServers) {
                byte b = kafkaServer.brokerState().currentState();
                if (b < 3) {
                    started = false;
                    sleepSafely(500);
                    break;
                }
            }
        }
    }

    public synchronized void stop() {
        for (KafkaServer kafkaServer : this.kafkaServers) {
            try {
                kafkaServer.shutdown();
                kafkaServer.awaitShutdown();
            } catch (Exception e) {
                // do nothing
            }
        }
        zkStarter.stop();
    }

    private static String getConnectionString(String host, int port) {
        return String.format(HOST_PORT_FORMAT, host, port);
    }

    public String getBrokersAddress() {
        StringBuilder sb = new StringBuilder();
        for (KafkaServer kafkaServer : kafkaServers) {
            KafkaConfig config = kafkaServer.config();
            Map<SecurityProtocol, EndPoint> listeners = config.listeners();
            java.util.Map<SecurityProtocol, EndPoint> map = JavaConversions.mapAsJavaMap(listeners);
            for (java.util.Map.Entry<SecurityProtocol, EndPoint> entry : map.entrySet()) {
                SecurityProtocol key = entry.getKey();
                if (SecurityProtocol.PLAINTEXT.equals(key)) {
                    EndPoint endPoint = entry.getValue();
                    sb.append(",").append(getConnectionString(endPoint.host(), endPoint.port()));
                }
            }
        }
        return sb.substring(1);
    }

    public String getZkAddress() {
        int zkClientPort = zkStarter.getZkClientPort();
        return "localhost:" + zkClientPort;
    }
}
