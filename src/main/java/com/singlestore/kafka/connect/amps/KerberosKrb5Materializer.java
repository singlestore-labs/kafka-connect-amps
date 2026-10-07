package com.singlestore.kafka.connect.amps;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Copies a host {@code krb5.conf} and appends Java GSS overrides so the extractor can
 * prefer TCP to the KDC without editing {@code /etc/krb5.conf} on every leaf.
 *
 * <p>Java GSS only falls back from UDP to TCP after {@code KRB_ERR_RESPONSE_TOO_BIG}.
 * A silently dropped UDP/88 packet times out instead. MIT {@code kinit} retries TCP
 * on its own; setting {@code udp_preference_limit = 1} makes the JDK do the same.</p>
 */
public final class KerberosKrb5Materializer {

    static final String DEFAULT_UNIX_KRB5_CONF = "/etc/krb5.conf";

    private KerberosKrb5Materializer() {
    }

    /**
     * Writes a temp krb5.conf that is the source file plus a trailing {@code [libdefaults]}
     * block. Java's parser merges later keys over earlier ones, so these win over the host file.
     */
    public static String wrapForTcp(String sourcePath) {
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new ConnectException("Kerberos krb5.conf path must be non-empty");
        }
        Path src = Path.of(sourcePath);
        if (!Files.isRegularFile(src) || !Files.isReadable(src)) {
            throw new ConnectException("Cannot read Kerberos config '" + sourcePath
                    + "'. Install /etc/krb5.conf on the leaf or set amps.kerberos.krb5.conf.");
        }
        try {
            String original = Files.readString(src, StandardCharsets.UTF_8);
            StringBuilder body = new StringBuilder(original);
            if (!original.isEmpty() && !original.endsWith("\n")) {
                body.append('\n');
            }
            body.append('\n')
                    .append("# Appended by kafka-connect-amps (amps.kerberos.force.tcp=true).\n")
                    .append("# Java GSS stays on UDP unless this limit is below the AS-REQ size.\n")
                    .append("[libdefaults]\n")
                    .append("udp_preference_limit = 1\n")
                    .append("kdc_timeout = 3000\n");
            Path out = Files.createTempFile("amps-connect-krb5-", ".conf");
            Files.writeString(out, body.toString(), StandardCharsets.UTF_8);
            out.toFile().deleteOnExit();
            return out.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new ConnectException("Failed to write temporary krb5.conf: " + e.getMessage(), e);
        }
    }
}
