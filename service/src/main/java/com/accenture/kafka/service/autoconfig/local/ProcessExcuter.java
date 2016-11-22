package com.accenture.kafka.service.autoconfig.local;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by THINK on 2016/11/19.
 */
@Slf4j
public class ProcessExcuter extends Thread {
    public static final AtomicInteger ATOMIC_ID = new AtomicInteger(1);
    private final ProcessBuilder pb;
    private final OutputHandler outputHandler;
    private final long eid;
    public static final Random RANDOM = new Random();
    public volatile Integer exitValue;

    public ProcessExcuter(final ProcessBuilder pb, final OutputHandler outputHandler) {
        super();
        eid = ATOMIC_ID.getAndIncrement();
        StringBuilder s = new StringBuilder();
        s.append("ProcessExcuter-").append(eid).append("[");
        for (String s1 : pb.command()) {
            s.append(s1).append(" ");
        }
        s.append("]");
        setName(s.toString());
        this.pb = pb;
        this.outputHandler = outputHandler == null ? new OutputHandler.StdOutputHandler() : outputHandler;
    }


    @Override
    public void run() {
        super.run();
        try {
            startProcess();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void startProcess() throws IOException {
        Process p = null;
        pb.redirectErrorStream(true);
        try {
            p = pb.start();
            BufferedReader bf = new BufferedReader(new InputStreamReader(p.getInputStream(), "GBK"));
            while ((!isInterrupted())) {
                while (bf.ready()) {
                    log.info(getName());
                    outputHandler.handleLine(bf.readLine());
                }
                try {
                    Thread.sleep(100 + RANDOM.nextInt(100));
                } catch (InterruptedException e) {
                    interrupt();
                }
            }

            bf.close();
        } finally {

            if (p != null) {
                p.destroy();
                exitValue = p.exitValue();
            }

            log.info("Thread exit  " + getId());
        }
    }
}
