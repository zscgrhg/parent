package com.accenture.kafka.service.autoconfig.embedded;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Created by THINK on 2016/11/18.
 */
@Slf4j
@Data
public class ZookeeperEmbedded {


    private final int clientPort;
    private final String dataDir;
    private volatile Thread zkServerThread;
    private volatile ZooKeeperServerMain zkServer;

    public ZookeeperEmbedded(final int clientPort, final String dataDir) {
        this.clientPort = clientPort;
        this.dataDir = dataDir;
    }

    public synchronized void start() {
        if (zkServerThread == null) {
            zkServerThread = new Thread(new ServerRunnable(), "ZooKeeper Server Starter");
            zkServerThread.setDaemon(true);
            zkServerThread.start();
        }
    }


    public synchronized void stop() {
        if (zkServerThread != null) {
            // The shutdown method is protected...thus this hack to invoke it.
            // This will log an exception on shutdown; see
            // https://issues.apache.org/jira/browse/ZOOKEEPER-1873 for details.
            try {
                Method shutdown = ZooKeeperServerMain.class.getDeclaredMethod("shutdown");
                shutdown.setAccessible(true);
                shutdown.invoke(zkServer);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }

            // It is expected that the thread will exit after
            // the server is shutdown; this will block until
            // the shutdown is complete.
            try {
                zkServerThread.join(5000);
                zkServerThread = null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for embedded ZooKeeper to exit");
                // abandoning zk thread
                zkServerThread = null;
            }
        }
    }


    private class ServerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Properties properties = new Properties();
                File file = new File(dataDir);
                file.deleteOnExit();
                properties.setProperty("dataDir", file.getAbsolutePath());
                properties.setProperty("clientPort", String.valueOf(clientPort));

                QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
                quorumPeerConfig.parseProperties(properties);

                zkServer = new ZooKeeperServerMain();
                ServerConfig configuration = new ServerConfig();
                configuration.readFrom(quorumPeerConfig);

                zkServer.runFromConfig(configuration);
            } catch (Exception e) {
                log.error("Exception running embedded ZooKeeper", e);
            }
        }
    }
}
