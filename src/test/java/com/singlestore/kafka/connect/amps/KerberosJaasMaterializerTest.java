package com.singlestore.kafka.connect.amps;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class KerberosJaasMaterializerTest {

    @Test
    void templateIsBundled() {
        String t = KerberosJaasMaterializer.readTemplate();
        assertTrue(t.contains("@@JAAS_ENTRY@@"));
        assertTrue(t.contains("@@KEYTAB@@"));
        assertTrue(t.contains("@@PRINCIPAL@@"));
    }

    @Test
    void escapeDoublesBackslashesForJaas() {
        assertEquals("C:\\\\Users\\\\svc\\\\k.keytab",
                KerberosJaasMaterializer.escapeForJaasQuotedString("C:\\Users\\svc\\k.keytab"));
    }

    @Test
    void escapeQuotesInPath() {
        assertEquals("\\\\\\\"weird\\\\\\\"",
                KerberosJaasMaterializer.escapeForJaasQuotedString("\\\"weird\\\""));
    }

    @Test
    void materializeSubstitutesEntryKeytabAndPrincipal() throws Exception {
        String path = KerberosJaasMaterializer.materializeToTempFile(
                "AMPSClient",
                "/var/lib/kafka/connect.keytab",
                "connect@EXAMPLE.COM");
        Path p = Path.of(path);
        assertTrue(Files.isRegularFile(p));
        String body = Files.readString(p);
        assertTrue(body.contains("AMPSClient {"));
        assertTrue(body.contains("keyTab=\"/var/lib/kafka/connect.keytab\""));
        assertTrue(body.contains("principal=\"connect@EXAMPLE.COM\""));
        assertFalse(body.contains("@@"));
    }
}
