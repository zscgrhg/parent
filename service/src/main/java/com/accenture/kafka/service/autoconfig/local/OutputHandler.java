package com.accenture.kafka.service.autoconfig.local;

import lombok.extern.java.Log;

/**
 * Created by THINK on 2016/11/19.
 */
public interface OutputHandler {
    public void handleLine(String line);
    @Log
    static class StdOutputHandler implements OutputHandler{
        @Override
        public void handleLine(final String line) {
            log.info(line);
        }
    }
}
