package com.accenture.kafka.service.autoconfig.local;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by THINK on 2016/11/19.
 */
public interface OutputHandler {
    public void handleLine(String line);
    @Slf4j
    static class StdOutputHandler implements OutputHandler{
        @Override
        public void handleLine(final String line) {
            log.info(line);
        }
    }
}
