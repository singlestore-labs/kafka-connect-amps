package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validation tests for the new {@code amps.auth.type} family of config keys.
 */
class AMPSSourceConnectorConfigAuthTest {

    private Map<String, String> props;

    @BeforeEach
    void setUp() {
        props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "orders");
    }

    @Test
    void defaultsToNoneAuth() {
        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);
        assertEquals(AuthType.NONE, cfg.ampsAuthType);
        assertEquals(MessageType.JSON, cfg.ampsMessageType);
        assertEquals("", cfg.ampsUsername);
        assertNull(cfg.ampsPassword);
        assertEquals("", cfg.ampsKerberosSpn);
        assertEquals("AMPSClient", cfg.ampsKerberosJaasEntry);
        assertEquals("", cfg.ampsKerberosKeytab);
        assertEquals("", cfg.ampsKerberosClientPrincipal);
        assertEquals(8 * 3600, cfg.ampsKerberosLifetimeSeconds);
        assertFalse(cfg.ampsKerberosDebug);
        assertTrue(cfg.ampsKerberosForceTcp);
        assertEquals("", cfg.ampsKerberosKrb5Conf);
    }

    @Test
    void passwordModeWithCredentialsBuildsSuccessfully() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "PASSWORD");
        props.put(AMPSSourceConnectorConfig.AMPS_USERNAME_CONF, "alice");
        props.put(AMPSSourceConnectorConfig.AMPS_PASSWORD_CONF, "secret");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(AuthType.PASSWORD, cfg.ampsAuthType);
        assertEquals("alice", cfg.ampsUsername);
        assertNotNull(cfg.ampsPassword);
        assertEquals("secret", cfg.ampsPassword.value());
    }

    @Test
    void passwordModeRejectsBlankUsername() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "PASSWORD");
        props.put(AMPSSourceConnectorConfig.AMPS_PASSWORD_CONF, "secret");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> new AMPSSourceConnectorConfig(props));
        assertTrue(ex.getMessage().contains("amps.username"),
                "Expected username error, got: " + ex.getMessage());
    }

    @Test
    void passwordModeRejectsMissingPassword() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "PASSWORD");
        props.put(AMPSSourceConnectorConfig.AMPS_USERNAME_CONF, "alice");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> new AMPSSourceConnectorConfig(props));
        assertTrue(ex.getMessage().contains("amps.password"),
                "Expected password error, got: " + ex.getMessage());
    }

    @Test
    void kerberosModeWithSpnBuildsSuccessfully() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/amps-host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/etc/kafka-connect/amps-jaas.conf");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(AuthType.KERBEROS_GSSAPI, cfg.ampsAuthType);
        assertEquals("AMPS/amps-host.example.com", cfg.ampsKerberosSpn);
        assertEquals("AMPSClient", cfg.ampsKerberosJaasEntry);
        assertEquals("/etc/kafka-connect/amps-jaas.conf", cfg.ampsKerberosJaasConfig);
    }

    @Test
    void kerberosModeWithKeytabPairBuildsSuccessfully() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/amps-host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/secure/kafka-connect.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "kafka-connect@CORP.EXAMPLE.COM");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(AuthType.KERBEROS_GSSAPI, cfg.ampsAuthType);
        assertEquals("/secure/kafka-connect.keytab", cfg.ampsKerberosKeytab);
        assertEquals("kafka-connect@CORP.EXAMPLE.COM", cfg.ampsKerberosClientPrincipal);
        assertEquals("", cfg.ampsKerberosJaasConfig);
    }

    @Test
    void kerberosModeRejectsBlankSpn() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/jaas.conf");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> new AMPSSourceConnectorConfig(props));
        assertTrue(ex.getMessage().contains("amps.kerberos.spn"),
                "Expected SPN error, got: " + ex.getMessage());
    }

    @Test
    void kerberosModeRejectsNeitherJaasNorKeytabPair() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");

        ConfigException ex = assertThrows(ConfigException.class,
                () -> new AMPSSourceConnectorConfig(props));
        assertTrue(ex.getMessage().contains("amps.kerberos.jaas.config")
                        || ex.getMessage().contains("amps.kerberos.keytab"),
                "Expected Kerberos credential error, got: " + ex.getMessage());
    }

    @Test
    void invalidAuthTypeIsRejected() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "OAUTH2");
        assertThrows(ConfigException.class, () -> new AMPSSourceConnectorConfig(props));
    }

    @Test
    void authTypeIsCaseInsensitive() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "kerberos_gssapi");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/j.conf");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);
        assertEquals(AuthType.KERBEROS_GSSAPI, cfg.ampsAuthType);
    }

    @Test
    void kerberosLifetimeRangeIsEnforced() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_JAAS_CONFIG_CONF, "/tmp/j.conf");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_LIFETIME_CONF, "10");

        // 10 seconds is below the documented minimum (60s)
        assertThrows(ConfigException.class, () -> new AMPSSourceConnectorConfig(props));
    }

    @Test
    void nvfixMessageTypeBuildsSuccessfully() {
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/nvfix");
        props.put(AMPSSourceConnectorConfig.AMPS_MESSAGE_TYPE_CONF, "nvfix");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(MessageType.NVFIX, cfg.ampsMessageType);
    }

    @Test
    void invalidMessageTypeIsRejected() {
        props.put(AMPSSourceConnectorConfig.AMPS_MESSAGE_TYPE_CONF, "csv");
        assertThrows(ConfigException.class, () -> new AMPSSourceConnectorConfig(props));
    }

    @Test
    void sowQueryBatchSizeDefaultsToBatchSizeWhenUnset() {
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "321");
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(0, cfg.ampsSowQueryBatchSize);
        assertEquals(321, cfg.getEffectiveSowQueryBatchSize());
    }

    @Test
    void sowQueryBatchSizeOverrideIsApplied() {
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "321");
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.AMPS_SOW_QUERY_BATCH_SIZE_CONF, "777");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertEquals(777, cfg.ampsSowQueryBatchSize);
        assertEquals(777, cfg.getEffectiveSowQueryBatchSize());
    }

    @Test
    void kerberosDebugCanBeEnabledFromConfig() {
        props.put(AMPSSourceConnectorConfig.AMPS_AUTH_TYPE_CONF, "KERBEROS_GSSAPI");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_SPN_CONF, "AMPS/host.example.com");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_KEYTAB_CONF, "/etc/memsql/amps_user.keytab");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, "ampsusr@CORP.EXAMPLE.COM");
        props.put(AMPSSourceConnectorConfig.AMPS_KERBEROS_DEBUG_CONF, "true");

        AMPSSourceConnectorConfig cfg = new AMPSSourceConnectorConfig(props);

        assertTrue(cfg.ampsKerberosDebug);
    }

    @Test
    void sowQueryBatchSizeRejectsValuesAboveAmpsLimit() {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.AMPS_SOW_QUERY_BATCH_SIZE_CONF, "10001");

        assertThrows(ConfigException.class, () -> new AMPSSourceConnectorConfig(props));
    }
}
