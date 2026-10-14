package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.TCPSTransport;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AMPSSslConfiguratorTest {

    private SSLContext previous;

    @BeforeEach
    void saveSslContext() {
        try {
            previous = TCPSTransport.getDefaultSSLContext();
        } catch (Exception e) {
            previous = null;
        }
    }

    @AfterEach
    void restoreSslContext() {
        TCPSTransport.setDefaultSSLContext(previous);
    }

    @Test
    void pemTrustCertificateBuildsContext() throws Exception {
        Map<String, String> props = baseTcps();
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUST_CERTIFICATE_CONF, resource("ssl/test-ca.pem"));
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        KeyStore trust = AMPSSslConfigurator.loadTrustStore(config);
        assertNotNull(trust);
        assertTrue(trust.containsAlias("amps-trust-0"));

        SSLContext ctx = AMPSSslConfigurator.buildContext(config);
        assertNotNull(ctx);
        AMPSSslConfigurator.apply(config);
        assertNotNull(TCPSTransport.getDefaultSSLContext());
    }

    @Test
    void pkcs12TruststoreBuildsContext() throws Exception {
        Map<String, String> props = baseTcps();
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUSTSTORE_CONF, resource("ssl/test-trust.p12"));
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUSTSTORE_PASSWORD_CONF, "changeit");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        KeyStore trust = AMPSSslConfigurator.loadTrustStore(config);
        assertNotNull(trust);
        assertTrue(trust.aliases().hasMoreElements());
    }

    @Test
    void missingPemFails() {
        Map<String, String> props = baseTcps();
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUST_CERTIFICATE_CONF, "/no/such/ca.pem");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);
        org.apache.kafka.connect.errors.ConnectException ex = assertThrows(
                org.apache.kafka.connect.errors.ConnectException.class,
                () -> AMPSSslConfigurator.loadTrustStore(config));
        assertTrue(ex.getMessage().contains("amps.ssl.trust.certificate"), ex.getMessage());
    }

    @Test
    void inferStoreTypeFromExtension() {
        assertEquals("PKCS12", AMPSSslConfigurator.inferStoreType("/a/trust.p12", ""));
        assertEquals("JKS", AMPSSslConfigurator.inferStoreType("/a/trust.jks", ""));
        assertEquals("PKCS12", AMPSSslConfigurator.inferStoreType("/a/x.jks", "PKCS12"));
    }

    @Test
    void sslConfigOnTcpUriIsRejected() {
        Map<String, String> props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "orders");
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUST_CERTIFICATE_CONF, "/etc/memsql/amps-ca.pem");

        ConfigException ex = assertThrows(ConfigException.class, () -> new AMPSSourceConnectorConfig(props));
        assertTrue(ex.getMessage().contains("tcps://"), ex.getMessage());
    }

    @Test
    void sslConfigOnTcpsUriIsAccepted() {
        Map<String, String> props = baseTcps();
        props.put(AMPSSourceConnectorConfig.AMPS_SSL_TRUST_CERTIFICATE_CONF, "/etc/memsql/amps-ca.pem");
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);
        assertEquals("/etc/memsql/amps-ca.pem", cfg.ampsSslTrustCertificate);
    }

    @Test
    void notConfiguredWhenSslKeysEmpty() {
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(baseTcps());
        assertFalse(AMPSSslConfigurator.isConfigured(config));
    }

    private static Map<String, String> baseTcps() {
        Map<String, String> props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcps://ampssvc.example.com:9008/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "orders");
        return props;
    }

    private static String resource(String name) throws Exception {
        URI uri = AMPSSslConfiguratorTest.class.getResource("/" + name).toURI();
        return Path.of(uri).toString();
    }
}
