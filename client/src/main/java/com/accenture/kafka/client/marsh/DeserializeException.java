package com.accenture.kafka.client.marsh;

/**
 * Created by THINK on 2016/11/22.
 */
public class DeserializeException extends RuntimeException {
    public static final String MESSAGE = "Deserialize Exception";
    public static final String MESSAGE_FORMAT = "Deserialize Exception, Unsupported Version: %d";

    public DeserializeException() {
        super(MESSAGE);
    }

    public DeserializeException(int unsupportedVersion) {
        super(String.format(MESSAGE_FORMAT, unsupportedVersion));
    }

    public DeserializeException(final Throwable cause) {
        super(MESSAGE, cause);
    }
}
