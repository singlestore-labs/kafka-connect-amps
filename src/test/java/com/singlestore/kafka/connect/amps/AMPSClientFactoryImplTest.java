package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.ServerChooser;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the static helpers in {@link AMPSClientFactoryImpl}: URI credential
 * injection, JAAS system property handling, and the {@link ServerChooser}
 * dispatch by {@code amps.auth.type}.
 */
class AMPSClientFactoryImplTest {

    private String savedJaasProp;
    private String savedKrb5Debug;
    private String savedJgssDebug;
    private String savedKrb5Conf;

    @BeforeEach
    void saveSystemProperty() {
        savedJaasProp = System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY);
        savedKrb5Debug = System.getProperty(AMPSClientFactoryImpl.KRB5_DEBUG_PROPERTY);
        savedJgssDebug = System.getProperty(AMPSClientFactoryImpl.JGSS_DEBUG_PROPERTY);
        savedKrb5Conf = System.getProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY);
        System.clearProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY);
        System.clearProperty(AMPSClientFactoryImpl.KRB5_DEBUG_PROPERTY);
        System.clearProperty(AMPSClientFactoryImpl.JGSS_DEBUG_PROPERTY);
        System.clearProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY);
    }

    @AfterEach
    void restoreSystemProperty() {
        restore(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY, savedJaasProp);
        restore(AMPSClientFactoryImpl.KRB5_DEBUG_PROPERTY, savedKrb5Debug);
        restore(AMPSClientFactoryImpl.JGSS_DEBUG_PROPERTY, savedJgssDebug);
        restore(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY, savedKrb5Conf);
    }

    private static void restore(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "orders");
        return props;
    }

    @Test
    void injectCredentialsAddsUserInfo() {
        String uri = AMPSClientFactoryImpl.injectCredentials(
                "tcp://amps-host.example.com:9007/amps/json", "alice", "s3cret");
        assertEquals("tcp://alice:s3cret@amps-host.example.com:9007/amps/json", uri);
    }

    @Test
    void injectCredentialsUrlEncodesSpecialCharacters() {
        // username with @, password with : and / and @
        String uri = AMPSClientFactoryImpl.injectCredentials(
                "tcp://h:9007/amps/json", "alice@CORP", "p:a/s@s");
        // URLEncoder uses %40 for @, %3A for :, %2F for /
        assertTrue(uri.contains("alice%40CORP:p%3Aa%2Fs%40s@h:9007"),
                "Expected URL-encoded credentials in: " + uri);
    }

    @Test
    void injectCredentialsRejectsBlanks() {
        assertThrows(ConnectException.class,
                () -> AMPSClientFactoryImpl.injectCredentials("tcp://h:9007/amps/json", "", "p"));
        assertThrows(ConnectException.class,
                () -> AMPSClientFactoryImpl.injectCredentials("tcp://h:9007/amps/json", "u", ""));
        assertThrows(ConnectException.class,
                () -> AMPSClientFactoryImpl.injectCredentials("", "u", "p"));
    }

    @Test
    void injectCredentialsRejectsMalformedUri() {
        assertThrows(ConnectException.class,
                () -> AMPSClientFactoryImpl.injectCredentials("not a uri", "u", "p"));
    }

    @Test
    void buildServerChooserNoneReturnsDefaultChooserWithUriAsIs() {
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(baseProps());
        ServerChooser chooser = AMPSClientFactoryImpl.buildServerChooser(cfg, cfg.ampsUri);

        assertTrue(chooser instanceof DefaultServerChooser);
        assertFalse(chooser instanceof KerberosAuthServerChooser);
        assertEquals("tcp://localhost:9007/amps/json", chooser.getCurrentURI());
    }

    @Test
    void buildServerChooserPasswordRewritesUri() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "PASSWORD");
        props.put(AMPSSourceConnectorConfig.AMPS_USERNAME_CONF, "alice");
        props.put(AMPSSourceConnectorConfig.AMPS_PASSWORD_CONF, "secret");
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        ServerChooser chooser = AMPSClientFactoryImpl.buildServerChooser(cfg, cfg.ampsUri);

        assertTrue(chooser instanceof DefaultServerChooser);
        assertEquals("tcp://alice:secret@localhost:9007/amps/json", chooser.getCurrentURI());
    }

    @Test
    void buildServerChooserKerberosReturnsKerberosChooserAndSetsJaasProperty() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/test-jaas.conf");
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        ServerChooser chooser = AMPSClientFactoryImpl.buildServerChooser(cfg, cfg.ampsUri);

        assertTrue(chooser instanceof KerberosAuthServerChooser);
        assertNotNull(((KerberosAuthServerChooser) chooser).getCurrentAuthenticator());
        assertEquals("/tmp/test-jaas.conf",
                System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY));
    }

    @Test
    void buildServerChooserKerberosWithKeytabPairMaterializesJaasFile() throws Exception {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/secure/client.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "svc@REALM");
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        AMPSClientFactoryImpl.buildServerChooser(cfg, cfg.ampsUri);

        String jaasPath = System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY);
        assertNotNull(jaasPath);
        assertTrue(jaasPath.contains("amps-connect-jaas-"), jaasPath);
        String body = java.nio.file.Files.readString(java.nio.file.Path.of(jaasPath));
        assertTrue(body.contains("keyTab=\"/secure/client.keytab\""));
        assertTrue(body.contains("principal=\"svc@REALM\""));
    }

    @Test
    void buildServerChooserKerberosExplicitJaasOverridesKeytabFields() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/etc/explicit.jaas");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/ignored/keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ignored@REALM");
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        AMPSClientFactoryImpl.buildServerChooser(cfg, cfg.ampsUri);

        assertEquals("/etc/explicit.jaas",
                System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY));
    }

    @Test
    void maybeSetJaasSystemPropertyDoesNotOverrideExisting() {
        System.setProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY, "/etc/preset-jaas.conf");
        AMPSClientFactoryImpl.maybeSetJaasSystemProperty("/tmp/connector-jaas.conf");
        assertEquals("/etc/preset-jaas.conf",
                System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY));
    }

    @Test
    void maybeSetJaasSystemPropertyIgnoresEmptyInput() {
        AMPSClientFactoryImpl.maybeSetJaasSystemProperty("");
        AMPSClientFactoryImpl.maybeSetJaasSystemProperty(null);
        assertNull(System.getProperty(AMPSClientFactoryImpl.JAAS_SYSTEM_PROPERTY));
    }

    /**
     * The AMPS client skips Authenticator.authenticate() when the URI has no user component,
     * producing a logon with no user_id and no GSS token.
     */
    @Test
    void kerberosChooserUriCarriesUserFromClientPrincipal() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://100.31.208.228:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/ampssvc.corp.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");

        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);
        assertEquals("ampsusr", AMPSClientFactoryImpl.resolveKerberosUser(config));
        assertEquals("tcp://ampsusr@100.31.208.228:9007/amps/json",
                AMPSClientFactoryImpl.injectUser(config.ampsUri, "ampsusr"));
    }

    @Test
    void resolveKerberosUserPrefersExplicitUsername() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/ampssvc.corp.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");
        props.put(AMPSSourceConnectorConfig.AMPS_USERNAME_CONF, "pipeline");

        assertEquals("pipeline",
                AMPSClientFactoryImpl.resolveKerberosUser(new AMPSSourceConnectorConfig(props)));
    }

    @Test
    void injectUserKeepsExistingUserInfoAndAddsNoPassword() {
        assertEquals("tcp://someone@h:9007/amps/json",
                AMPSClientFactoryImpl.injectUser("tcp://someone@h:9007/amps/json", "ignored"));
        String added = AMPSClientFactoryImpl.injectUser("tcp://h:9007/amps/json", "ampsusr");
        assertEquals("tcp://ampsusr@h:9007/amps/json", added);
    }

    @Test
    void configureConnectRetryCapsHaClientReconnects() {
        com.crankuptheamps.client.HAClient client = new com.crankuptheamps.client.HAClient("timeout-test");
        AMPSClientFactoryImpl.configureConnectRetry(client, 15000);
        assertEquals(15000, client.getTimeout());
        assertTrue(client.getReconnectDelayStrategy() instanceof com.crankuptheamps.client.ExponentialDelayStrategy);
    }

    @Test
    void defaultConnectTimeoutIsFifteenSeconds() {
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(baseProps());
        assertEquals(15000, config.ampsConnectTimeoutMs);
    }

    @Test
    void failedToConnectMentionsAuthAndTimeout() {
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(baseProps());
        ConnectException ex = AMPSClientFactoryImpl.failedToConnect("kafka-connect-amps-market-ticker",
                config.ampsUri, config, null, 90207L, new RuntimeException("auth failure"));
        assertTrue(ex.getMessage().contains("15000"));
        assertTrue(ex.getMessage().contains("90207"), ex.getMessage());
        assertTrue(ex.getMessage().contains("kafka-connect-amps-market-ticker"));
        assertTrue(ex.getMessage().contains("auth failure"));
    }

    /**
     * DefaultServerChooser.getError() is hardcoded to "" and its reportFailure discards the
     * exception, so the real cause has to come from the chooser we install.
     */
    @Test
    void failedToConnectSurfacesChooserRecordedError() throws Exception {
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(baseProps());
        LastErrorServerChooser chooser = new LastErrorServerChooser();
        chooser.add(config.ampsUri);
        assertEquals("", chooser.getError());

        chooser.reportFailure(new java.net.ConnectException("Connection refused"), null);
        assertTrue(chooser.getError().contains("Connection refused"), chooser.getError());

        ConnectException ex = AMPSClientFactoryImpl.failedToConnect("c", config.ampsUri, config,
                chooser, 100L, new RuntimeException("The last connection error was: "));
        assertTrue(ex.getMessage().contains("Connection refused"), ex.getMessage());
    }

    @Test
    void describeRendersWholeCauseChain() {
        Throwable root = new IllegalStateException("no ticket for service");
        Throwable mid = new RuntimeException("GSS failure", root);
        Throwable top = new RuntimeException("The last connection error was: ", mid);

        String described = LastErrorServerChooser.describe(top);

        assertTrue(described.contains("GSS failure"), described);
        assertTrue(described.contains("no ticket for service"), described);
    }

    @Test
    void kerberosFailureHintPointsAtKdcAndKeytab() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/ampssvc.corp.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        ConnectException ex = AMPSClientFactoryImpl.failedToConnect("c", config.ampsUri, config, null,
                90000L, new RuntimeException("boom"));

        assertTrue(ex.getMessage().contains("port 88"), ex.getMessage());
        assertTrue(ex.getMessage().contains("/etc/memsql/amps_user.keytab"), ex.getMessage());
        assertTrue(ex.getMessage().contains("AMPS/ampssvc.corp.example.com"), ex.getMessage());
    }

    @Test
    void applyKerberosDebugSetsJdkFlags() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_DEBUG_CONF, "true");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        assertTrue(config.ampsKerberosDebug);
        AMPSClientFactoryImpl.applyKerberosDebug(config);
        assertEquals("true", System.getProperty(AMPSClientFactoryImpl.KRB5_DEBUG_PROPERTY));
        assertEquals("true", System.getProperty(AMPSClientFactoryImpl.JGSS_DEBUG_PROPERTY));
    }

    @Test
    void applyKerberosDebugDoesNothingWhenDisabled() {
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(baseProps());
        assertFalse(config.ampsKerberosDebug);
        AMPSClientFactoryImpl.applyKerberosDebug(config);
        assertNull(System.getProperty(AMPSClientFactoryImpl.KRB5_DEBUG_PROPERTY));
        assertNull(System.getProperty(AMPSClientFactoryImpl.JGSS_DEBUG_PROPERTY));
    }

    @Test
    void applyKerberosForceTcpWrapsHostKrb5AndSetsSystemProperty() throws Exception {
        Path host = java.nio.file.Files.createTempFile("host-krb5-", ".conf");
        java.nio.file.Files.writeString(host, "[libdefaults]\ndefault_realm = CORP.EXAMPLE.COM\n");

        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KRB5_CONF_CONF, host.toString());
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        assertTrue(config.ampsKerberosForceTcp);
        AMPSClientFactoryImpl.applyKerberosForceTcp(config);

        String wrapped = System.getProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY);
        assertNotNull(wrapped);
        String body = java.nio.file.Files.readString(java.nio.file.Path.of(wrapped));
        assertTrue(body.contains("default_realm = CORP.EXAMPLE.COM"), body);
        assertTrue(body.contains("udp_preference_limit = 1"), body);
        assertTrue(body.contains("kdc_timeout = 3000"), body);
    }

    @Test
    void applyKerberosForceTcpDoesNotOverrideExistingKrb5Property() {
        System.setProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY, "/etc/preset-krb5.conf");
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/j.conf");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        AMPSClientFactoryImpl.applyKerberosForceTcp(config);
        assertEquals("/etc/preset-krb5.conf", System.getProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY));
    }

    @Test
    void applyKerberosForceTcpSkippedWhenDisabled() {
        Map<String, String> props = baseProps();
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_FORCE_TCP_CONF, "false");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/j.conf");
        AMPSSourceConnectorConfig config = new AMPSSourceConnectorConfig(props);

        AMPSClientFactoryImpl.applyKerberosForceTcp(config);
        assertNull(System.getProperty(AMPSClientFactoryImpl.KRB5_CONF_PROPERTY));
    }
}
