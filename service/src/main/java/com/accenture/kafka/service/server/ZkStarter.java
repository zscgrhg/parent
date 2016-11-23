package com.accenture.kafka.service.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by THINK on 2016/11/23.
 */
@Slf4j
public class ZkStarter {
    private final Properties zkProperties;
    private ZkThread zkThread;


    public int getZkClientPort() {
        return Integer.parseInt(zkProperties.getProperty("clientPort"));
    }

    public ZkStarter(final String zkPropertiesPath) {
        InputStream resourceAsStream = getClass().getResourceAsStream(zkPropertiesPath);
        Properties p = new Properties();
        try {
            p.load(resourceAsStream);
            this.zkProperties = p;
        } catch (IOException e) {
            throw new RuntimeException("Start Zookeeper Failed", e);
        }
    }

    public synchronized void start() {
        if (zkThread == null) {
            zkThread = new ZkThread();
            zkThread.start();
        }
    }

    public synchronized void stop() {
        if (zkThread != null) {
            zkThread.interrupt();
            zkThread = null;
        }
    }


    private class ZkThread extends Thread {
        final ZkServerEmbedded zkServer;

        private ZkThread() {
            super();
            this.zkServer = new ZkServerEmbedded();
        }

        @Override
        public void run() {
            try {
                QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
                quorumPeerConfig.parseProperties(zkProperties);
                ServerConfig configuration = new ServerConfig();
                configuration.readFrom(quorumPeerConfig);
                zkServer.runFromConfig(configuration);
            } catch (Exception e) {
                log.error("Exception running embedded ZooKeeper", e);
            }
        }

        @Override
        public void interrupt() {
            zkServer.shutdown();
            super.interrupt();
        }
    }

    private class ZkServerEmbedded extends ZooKeeperServerMain {
        @Override
        protected void shutdown() {
            super.shutdown();
        }
    }
}
