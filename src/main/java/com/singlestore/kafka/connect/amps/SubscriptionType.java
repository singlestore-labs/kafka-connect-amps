package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * How the task reads from an AMPS topic on each {@code poll()} cycle.
 */
public enum SubscriptionType {
    /** Bookmark subscription (transaction log replay + live). */
    SUBSCRIBE("subscribe"),
    /** SOW snapshot then bookmark subscription (live updates after snapshot). */
    SOW_AND_SUBSCRIBE("sow_and_subscribe"),
    /** SOW query only (no standing subscription); each poll issues a new SOW command. */
    SOW("sow");

    private final String configValue;

    SubscriptionType(String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    public static SubscriptionType fromString(String value) {
        if (value == null || value.isEmpty()) {
            return SUBSCRIBE;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (SubscriptionType t : values()) {
            if (t.configValue.equals(normalized)) {
                return t;
            }
        }
        throw new ConfigException(
                "Invalid subscription type '" + value + "'. Valid values: " + validValuesString());
    }

    private static String validValuesString() {
        return Arrays.stream(values()).map(SubscriptionType::configValue).collect(Collectors.joining(", "));
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
