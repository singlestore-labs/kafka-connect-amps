package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Payload format used by the AMPS topic URI.
 */
public enum MessageType {
    JSON("json"),
    NVFIX("nvfix");

    private final String configValue;

    MessageType(String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    public static MessageType fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return JSON;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (MessageType type : values()) {
            if (type.configValue.equals(normalized)) {
                return type;
            }
        }
        throw new ConfigException(
                "Invalid message type '" + value + "'. Valid values: " + validValuesString());
    }

    public static String validValuesString() {
        return Arrays.stream(values()).map(MessageType::configValue).collect(Collectors.joining(", "));
    }

    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }
            fromString(value.toString());
        }

        @Override
        public String toString() {
            return "[" + validValuesString() + "]";
        }
    };
}
