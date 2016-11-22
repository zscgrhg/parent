package com.accenture.kafka.service.autoconfig.local;


import com.accenture.kafka.service.autoconfig.KafkaConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by THINK on 2016/11/19.
 */
@Slf4j
public class ExecUtil {

    public static final String OS_NAME = System.getProperty("os.name");
    public static final boolean IS_WINDOWS_OS = OS_NAME.toLowerCase().contains("windows");

    public static String selectKafkaCmdOfOs(String cmd) {
        if (IS_WINDOWS_OS) {
            return "bin\\windows\\" + cmd + ".bat";
        } else {
            return "bin/" + cmd + ".sh";
        }

    }

    public static void rmFileOrDir(File file) {
        if(file.exists()){
            Thread t;
            if (IS_WINDOWS_OS) {
                if (file.isDirectory()) {
                    t = exec(null, null, "rd/s/q", file.getAbsolutePath());
                } else {
                    t = exec(null, null, "del/f/s/q", file.getAbsolutePath());
                }
            } else {
                t = exec(null, null, "rm -rf", file.getAbsolutePath());
            }
            try {
                t.join(1500L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static Thread startZookeeper(File kafkaHome, File tempRoot, Map<String, String> zookeeperProperties, final String zkPort) throws InterruptedException, IOException {

        Properties zkProp = ServerProperties.getZookeeperProperties(tempRoot.getAbsolutePath(), zkPort, zookeeperProperties);
        final CountDownLatch zookeeperLatch = new CountDownLatch(1);
        File configDir = new File(tempRoot.getAbsolutePath() + File.separator + "config");
        configDir.mkdirs();
        File zookeeperCfgFile = createPropertiesFile(configDir.getAbsolutePath(), zkProp, "zookeeper.properties");
        zookeeperCfgFile.deleteOnExit();
        Thread startZookeeper = execNonBlock(kafkaHome,
                new OutputHandler() {
                    @Override
                    public void handleLine(final String line) {
                        log.info(line);
                        if (line.toLowerCase().contains("binding to port 0.0.0.0/0.0.0.0:" + zkPort)) {
                            zookeeperLatch.countDown();
                        } else if (line.contains("ERROR Unexpected exception, exiting abnormally")) {
                            throw new RuntimeException(line);
                        }
                    }
                }, selectKafkaCmdOfOs("zookeeper-server-start"),
                zookeeperCfgFile.getAbsolutePath());
        zookeeperLatch.await(5, TimeUnit.SECONDS);
        return startZookeeper;
    }

    public static Thread stopZookeeperIfExist(File workDir) throws InterruptedException {
        Thread exec = exec(workDir, null, selectKafkaCmdOfOs("zookeeper-server-stop"));
        return exec;
    }


    public static List<Thread> startKafka(File kafkaHome, File tempRoot, Map<String, String> serverConfig, String zkPort, Map<Integer, Integer> brokerIdAndPorts) throws InterruptedException, IOException {

        File configDir = new File(tempRoot.getAbsolutePath() + File.separator + "config");
        configDir.mkdirs();
        List<Thread> threads = new ArrayList<>();
        for (Map.Entry<Integer, Integer> brokerIdAndPort : brokerIdAndPorts.entrySet()) {
            final CountDownLatch kafkaLatch = new CountDownLatch(1);
            Properties serverProperties = ServerProperties.getKafkaProperties(
                    tempRoot.getAbsolutePath(),
                    "localhost:" + zkPort, serverConfig);
            serverProperties.put("broker.id", Integer.toString(brokerIdAndPort.getKey()));
            serverProperties.put("listeners", "PLAINTEXT://:" + brokerIdAndPort.getValue());
            File serverCfgFile = createPropertiesFile(configDir.getAbsolutePath(), serverProperties, "server" + brokerIdAndPort.getKey() + ".properties");
            serverCfgFile.deleteOnExit();
            Thread startKafka = execNonBlock(kafkaHome,
                    new OutputHandler() {
                        @Override
                        public void handleLine(final String line) {
                            log.info(line);
                            if (line.contains("started (kafka.server.KafkaServer)")) {
                                kafkaLatch.countDown();
                            }
                        }
                    }, selectKafkaCmdOfOs("kafka-server-start"),
                    serverCfgFile.getAbsolutePath());
            threads.add(startKafka);
            kafkaLatch.await(5, TimeUnit.SECONDS);
        }

        return threads;
    }

    public static Thread stopKafkaIfExist(File workDir) {
        return exec(workDir, null, selectKafkaCmdOfOs("kafka-server-stop"));
    }

    public static Thread exec(File workDir, OutputHandler handler, String... cmds) {
        final ProcessBuilder process = createProcess(workDir, cmds);
        Thread thread = new ProcessExcuter(process, handler);
        thread.start();
        return thread;
    }

    public static Thread execNonBlock(File workDir, OutputHandler handler, String... cmds) {
        final ProcessBuilder process = createProcess(workDir, cmds);
        Thread thread = new ProcessExcuter(process, handler);
        thread.start();
        return thread;
    }

    private static ProcessBuilder createProcess(File workDir, String... cmds) {
        log.info(cmds[0]);
        List<String> cmdLine = new ArrayList<String>();
        if (IS_WINDOWS_OS) {
            cmdLine.add("cmd");
            cmdLine.add("/c");
        }
        cmdLine.addAll(Arrays.asList(cmds));
        ProcessBuilder pb = new ProcessBuilder(cmdLine);
        pb.directory(workDir);
        return pb;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        File tmpRoot = new File("D:/library/kafkatest/localtmp");

        File kafkaHome = new File("E:/kafka_2.11-0.10.1.0/");

        if (!IS_WINDOWS_OS) {
            tmpRoot = new File("/root/kafkalog/tmp/");
            kafkaHome = new File("/root/kafka/kafka_2.11-0.10.1.0/");
        }
        Map<Integer, Integer> brokerIdAndPort = new HashMap<>();
        for (int i = 1; i < 3; i++) {
            brokerIdAndPort.put(i, 9202 + i);
        }
        startServer(kafkaHome, tmpRoot, Collections.EMPTY_MAP, Collections.EMPTY_MAP, brokerIdAndPort);
    }

    public static KafkaConnection startServer(File kafkaHome,
                                              File tmpRoot,
                                              Map<String, String> zookeeperProperties,
                                              Map<String, String> kafkaProperties,
                                              Map<Integer, Integer> brokerIdAndPort) throws IOException, InterruptedException {
        stopKafkaIfExist(kafkaHome);
        stopZookeeperIfExist(kafkaHome);
        if (!tmpRoot.exists()) {
            tmpRoot.mkdirs();
            tmpRoot.deleteOnExit();
        }

        boolean hasZkPort = zookeeperProperties.containsKey("clientPort");
        final String zkPort = hasZkPort ? zookeeperProperties.get("clientPort") : "2181";
        startZookeeper(kafkaHome, tmpRoot, zookeeperProperties, zkPort);
        startKafka(kafkaHome, tmpRoot, kafkaProperties, zkPort, brokerIdAndPort);
        log.info("start kafka complete");

        String zkAddress = "localhost:" + zkPort;
        StringBuilder sb = new StringBuilder();
        for (Integer port : brokerIdAndPort.values()) {
            sb.append(",").append("localhost:").append(port);
        }
        KafkaConnection kafkaConnection = KafkaConnection.builder().isEmbedded(false)
                .zookeeperConnectionString(zkAddress)
                .brokersAddress(sb.substring(1))
                .build();
        return kafkaConnection;
    }

    public static File createPropertiesFile(String dir, Properties properties, String filename) throws IOException {
        File serverProperties = new File(dir + File.separator + filename);
        if (serverProperties.exists()) {
            serverProperties.delete();
        }
        serverProperties.createNewFile();
        properties.store(new FileOutputStream(serverProperties), null);
        return serverProperties;
    }
}
