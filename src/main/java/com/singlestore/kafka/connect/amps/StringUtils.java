package com.singlestore.kafka.connect.amps;

/**
 * String checks shared by the connector.
 */
final class StringUtils {

    private StringUtils() {
    }

    static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
