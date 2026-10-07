package com.singlestore.kafka.connect.amps;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class KerberosKrb5MaterializerTest {

    @Test
    void wrapForTcpPreservesSourceAndAppendsUdpLimit() throws Exception {
        Path src = Files.createTempFile("krb5-src-", ".conf");
        Files.writeString(src, "[realms]\nEXAMPLE.COM = {\n  kdc = 10.0.0.1\n}\n");

        String wrapped = KerberosKrb5Materializer.wrapForTcp(src.toString());
        String body = Files.readString(Path.of(wrapped));

        assertTrue(body.contains("kdc = 10.0.0.1"), body);
        assertTrue(body.contains("[libdefaults]"), body);
        assertTrue(body.contains("udp_preference_limit = 1"), body);
        assertTrue(body.indexOf("kdc = 10.0.0.1") < body.indexOf("udp_preference_limit = 1"), body);
    }

    @Test
    void wrapForTcpRejectsMissingFile() {
        ConnectException ex = assertThrows(ConnectException.class,
                () -> KerberosKrb5Materializer.wrapForTcp("/no/such/krb5.conf"));
        assertTrue(ex.getMessage().contains("/no/such/krb5.conf"), ex.getMessage());
    }
}
