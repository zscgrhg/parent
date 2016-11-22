package com.accenture.kafka.service.autoconfig.local;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by THINK on 2016/11/19.
 */
public class ServerProperties {
    public static final AtomicInteger ATOMIC_ID = new AtomicInteger(1);

    public static Properties getKafkaProperties(String logDiRoot, String zookeeperConnect, Map<String, Object> p) {
        Properties properties = new Properties();
        properties.put("broker.id", Integer.toString(ATOMIC_ID.getAndIncrement()));
        properties.put("log.dirs", logDiRoot + File.separator + "kafka" + File.separator + (new Date().getTime() + "_" + ATOMIC_ID.getAndIncrement()) + File.separator + "log");
        properties.put("zookeeper.connect", zookeeperConnect);
        properties.put("num.network.threads", "3");
        properties.put("listeners", "PLAINTEXT://0.0.0.0:9092");
        properties.put("num.io.threads", "8");
        properties.put("socket.send.buffer.bytes", "102400");
        properties.put("socket.receive.buffer.bytes", "102400");
        properties.put("socket.request.max.bytes", "104857600");
        properties.put("num.partitions", "1");
        properties.put("num.recovery.threads.per.data.dir", "1");
        properties.put("log.retention.hours", "168");
        properties.put("log.segment.bytes", "1073741824");
        properties.put("log.retention.check.interval.ms", "300000");
        properties.put("zookeeper.connection.timeout.ms", "6000");
        properties.putAll(p);
        return properties;
    }

    public static Properties getZookeeperProperties(String dataDirRoot, String clientPort, Map<String, Object> p) {
        Properties properties = new Properties();

        properties.put("maxClientCnxns", "0");
        properties.put("dataDir", dataDirRoot + File.separator + "zookeeper" + File.separator + "data");
        properties.put("clientPort", clientPort);
        properties.putAll(p);
        return properties;
    }
}
