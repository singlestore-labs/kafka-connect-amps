package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Starting position for subscribe when no stored bookmark exists.
 * The first sow_and_subscribe reads the current snapshot and ignores this value; it always sends NOW.
 * sow ignores it.
 *
 * <ul>
 *   <li>{@link #LATEST} - new messages only, uses AMPS NOW bookmark.</li>
 *   <li>{@link #EPOCH} - replay from beginning of transaction log.</li>
 * </ul>
 */
public enum Position {
    LATEST,
    EPOCH;

    public static Position fromString(String value) {
        if (value == null) {
            return EPOCH;
        }
        try {
            return Position.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                    "Invalid position '" + value + "'. Valid values: " + validValuesString());
        }
    }

    private static String validValuesString() {
        return Arrays.stream(values()).map(Enum::name).collect(Collectors.joining(", "));
    }

    /** Validator for {@link org.apache.kafka.common.config.ConfigDef}. */
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
