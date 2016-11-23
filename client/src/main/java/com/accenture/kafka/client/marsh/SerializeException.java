package com.accenture.kafka.client.marsh;

/**
 * Created by THINK on 2016/11/22.
 */
public class SerializeException extends RuntimeException {
    public static final String MESSAGE = "Serialize Exception Caused By:%s";

    public SerializeException(final Throwable cause) {
        super(String.format(MESSAGE, cause.getMessage()), cause);
    }
}
