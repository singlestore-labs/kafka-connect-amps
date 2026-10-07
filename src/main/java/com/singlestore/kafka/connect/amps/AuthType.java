package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Authentication mode used by the connector when connecting to AMPS.
 *
 * <ul>
 *   <li>{@link #NONE} - no authentication; connect using the URI as-is.</li>
 *   <li>{@link #PASSWORD} - username/password forwarded to AMPS via the default authenticator.
 *       This covers any AMPS server-side password module (LDAP, file/htpasswd, custom).</li>
 *   <li>{@link #KERBEROS_GSSAPI} - Kerberos via the vendored 60East
 *       {@code AMPSKerberosGSSAPIAuthenticator}. Requires a JAAS login configuration.</li>
 * </ul>
 */
public enum AuthType {
    NONE,
    PASSWORD,
    KERBEROS_GSSAPI;

    public static AuthType fromString(String value) {
        if (value == null) {
            return NONE;
        }
        try {
            return AuthType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                    "Invalid auth type '" + value + "'. Valid values: " + validValuesString());
        }
    }

    public static String validValuesString() {
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
