package com.singlestore.kafka.connect.amps;

/**
 * Utility class for retrieving connector version information.
 */
public class VersionUtil {

    private static final String VERSION = "0.1.0";

    private VersionUtil() {
    }

    public static String getVersion() {
        return VERSION;
    }

    public static String version(Class<?> cls) {
        return VERSION;
    }
}
