package com.singlestore.kafka.connect.amps;

/**
 * String checks shared by the connector. Whitespace is not empty here; callers that trim
 * use their own {@code isBlank}.
 */
final class StringUtils {

    private StringUtils() {
    }

    static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
