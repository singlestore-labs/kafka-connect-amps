package com.singlestore.kafka.connect.amps;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Builds a minimal JAAS configuration file for {@code Krb5LoginModule} keytab login
 * from a classpath template, so operators can omit {@code amps.kerberos.jaas.config}
 * when {@code amps.kerberos.keytab} and {@code amps.kerberos.client.principal} are set.
 */
public final class KerberosJaasMaterializer {

    static final String TEMPLATE_RESOURCE = "/jaas/amps-keytab-jaas.template";
    private static final String PLACEHOLDER_ENTRY = "@@JAAS_ENTRY@@";
    private static final String PLACEHOLDER_KEYTAB = "@@KEYTAB@@";
    private static final String PLACEHOLDER_PRINCIPAL = "@@PRINCIPAL@@";

    private KerberosJaasMaterializer() {
    }

    /**
     * Reads the bundled template, substitutes keytab path and principal, writes a temp file,
     * and returns its absolute path string suitable for {@code java.security.auth.login.config}.
     */
    public static String materializeToTempFile(String jaasEntryName, String keytabPath, String clientPrincipal) {
        Objects.requireNonNull(jaasEntryName, "jaasEntryName");
        Objects.requireNonNull(keytabPath, "keytabPath");
        Objects.requireNonNull(clientPrincipal, "clientPrincipal");
        if (jaasEntryName.isEmpty() || keytabPath.isEmpty() || clientPrincipal.isEmpty()) {
            throw new ConnectException("jaas entry, keytab path, and client principal must be non-empty");
        }
        if (jaasEntryName.contains("\"") || jaasEntryName.contains("{") || jaasEntryName.contains("}")) {
            throw new ConnectException("Invalid amps.kerberos.jaas.entry name: " + jaasEntryName);
        }

        String template = readTemplate();
        String body = template
                .replace(PLACEHOLDER_ENTRY, jaasEntryName)
                .replace(PLACEHOLDER_KEYTAB, escapeForJaasQuotedString(keytabPath))
                .replace(PLACEHOLDER_PRINCIPAL, escapeForJaasQuotedString(clientPrincipal));

        try {
            Path out = Files.createTempFile("amps-connect-jaas-", ".conf");
            Files.write(out, body.getBytes(StandardCharsets.UTF_8));
            out.toFile().deleteOnExit();
            return out.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new ConnectException("Failed to write temporary JAAS file: " + e.getMessage(), e);
        }
    }

    static String readTemplate() {
        try (InputStream in = KerberosJaasMaterializer.class.getResourceAsStream(TEMPLATE_RESOURCE)) {
            if (in == null) {
                throw new ConnectException("Missing bundled JAAS template resource: " + TEMPLATE_RESOURCE);
            }
            byte[] bytes = in.readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConnectException("Failed to read JAAS template: " + e.getMessage(), e);
        }
    }

    /**
     * JAAS {@code keyTab="..."} strings need backslashes doubled on Windows.
     * Double-quotes in paths are escaped as {@code \"}.
     */
    static String escapeForJaasQuotedString(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
