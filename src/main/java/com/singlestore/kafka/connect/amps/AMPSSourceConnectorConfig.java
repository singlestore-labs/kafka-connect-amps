package com.singlestore.kafka.connect.amps;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class AMPSSourceConnectorConfig extends AbstractConfig {

    // AMPS Connection Configuration
    public static final String AMPS_URI_CONF = "amps.uri";
    private static final String AMPS_URI_DOC = "AMPS server URI (e.g., tcp://localhost:9007/amps/json)";

    public static final String AMPS_CLIENT_NAME_CONF = "amps.client.name";
    private static final String AMPS_CLIENT_NAME_DOC = "Client name prefix for AMPS connections. Final name = prefix + AMPS topic name.";
    private static final String AMPS_CLIENT_NAME_DEFAULT = "kafka-connect-amps-";

    // Topic Configuration
    public static final String AMPS_TOPIC_CONF = "amps.topic";
    private static final String AMPS_TOPIC_DOC = "The AMPS topic to subscribe to.";

    // Kafka Configuration
    public static final String KAFKA_TOPIC_PREFIX_CONF = "kafka.topic.prefix";
    private static final String KAFKA_TOPIC_PREFIX_DOC = "Prefix for Kafka topic names. Final topic = prefix + AMPS topic name.";
    private static final String KAFKA_TOPIC_PREFIX_DEFAULT = "amps-";

    // Position/Offset Configuration
    public static final String AMPS_POSITION_CONF = "amps.position";
    private static final String AMPS_POSITION_DOC = "Starting position for subscribe when no stored bookmark exists: " +
            "LATEST (new messages only, uses AMPS NOW bookmark) or " +
            "EPOCH (replay from beginning of transaction log). " +
            "Applies only to subscribe. sow_and_subscribe ignores it on every call: the first start " +
            "sends NOW as the SOW point and the bookmark option so live publishes carry bookmarks, and " +
            "later calls resume from the stored or in-memory bookmark. " +
            "sow also ignores it. " +
            "Once running, the connector tracks bookmarks and resumes from the last processed position on restart. " +
            "Requires AMPS transaction log to be enabled.";
    private static final String AMPS_POSITION_DEFAULT = "EPOCH";

    public static final String AMPS_START_BOOKMARK_CONF = "amps.start.bookmark";
    private static final String AMPS_START_BOOKMARK_DOC = "Optional: Specify a specific AMPS bookmark to start from. " +
            "This OVERRIDES both stored offsets and amps.position setting. " +
            "Use this to replay from a specific point in the transaction log. " +
            "Leave empty to use normal offset tracking behavior. " +
            "Example: 11160226617891463387|1774300784000000006|";
    private static final String AMPS_START_BOOKMARK_DEFAULT = "";

    // Subscription Configuration
    public static final String AMPS_SUBSCRIPTION_TYPE_CONF = "amps.subscription.type";
    private static final String AMPS_SUBSCRIPTION_TYPE_DOC = "How each poll() reads from AMPS: " +
            "subscribe — bookmark subscription (replay + live). " +
            "sow_and_subscribe — one State-of-World (SOW) snapshot, then live updates on the same subscription. " +
            "A later start that already has a bookmark resumes with a bookmark subscription and does not request another snapshot. " +
            "sow — SOW query only (no standing subscription); each poll issues a new SOW (see AMPS SOW semantics).";
    private static final String AMPS_SUBSCRIPTION_TYPE_DEFAULT = SubscriptionType.SUBSCRIBE.configValue();

    public static final String AMPS_FILTER_CONF = "amps.filter";
    private static final String AMPS_FILTER_DOC = "Optional AMPS content filter expression for all subscriptions.";
    private static final String AMPS_FILTER_DEFAULT = "";

    // Performance Configuration
    public static final String BATCH_SIZE_CONF = "batch.size";
    private static final String BATCH_SIZE_DOC = "Maximum number of messages to batch before returning from poll().";
    private static final int BATCH_SIZE_DEFAULT = 100;

    public static final String POLL_TIMEOUT_MS_CONF = "poll.timeout.ms";
    private static final String POLL_TIMEOUT_MS_DOC = "Maximum time in milliseconds for poll() to wait for the first AMPS record after subscribe, " +
            "or while draining a SOW snapshot. After the first record, subscribe / sow_and_subscribe polls return when AMPS has been quiet for " +
            "poll.idle.ms, or sooner if batch.size is reached. Empty polls wait this full duration because a fresh subscribe starts with zero records.";
    private static final int POLL_TIMEOUT_MS_DEFAULT = 1000;

    public static final String POLL_IDLE_MS_CONF = "poll.idle.ms";
    private static final String POLL_IDLE_MS_DOC = "After the first record in a subscribe / sow_and_subscribe poll, return when AMPS has been quiet " +
            "this many milliseconds (or sooner if batch.size is reached). Empty polls still wait up to poll.timeout.ms. Does not apply to sow mode.";
    private static final int POLL_IDLE_MS_DEFAULT = 200;

    public static final String AMPS_CONNECT_TIMEOUT_MS_CONF = "amps.connect.timeout.ms";
    private static final String AMPS_CONNECT_TIMEOUT_MS_DOC = "Maximum time in milliseconds for the AMPS client to connect and log on during task start. "
            + "HAClient otherwise retries a refused logon forever (auth failure, name in use, unreachable host). "
            + "When this budget is exceeded, start() fails with ConnectException instead of hanging.";
    private static final int AMPS_CONNECT_TIMEOUT_MS_DEFAULT = 15000;

    public static final String AMPS_SOW_QUERY_BATCH_SIZE_CONF = "amps.sow.query.batch.size";
    private static final String AMPS_SOW_QUERY_BATCH_SIZE_DOC = "Optional AMPS SOW query batch size. " +
            "Applies only when amps.subscription.type=sow. " +
            "Set to 0 to inherit batch.size (default). " +
            "When overridden, this controls AMPS-side SOW response batching independently from Kafka Connect poll batching. " +
            "AMPS supports up to 10000 records per SOW batch.";
    private static final int AMPS_SOW_QUERY_BATCH_SIZE_DEFAULT = 0;

    // Authentication Configuration
    public static final String AMPS_AUTH_TYPE_CONF = "amps.auth.type";
    private static final String AMPS_AUTH_TYPE_DOC = "Authentication mode used when connecting to AMPS. " +
            "NONE (default) connects without credentials. " +
            "PASSWORD sends amps.username/amps.password using the AMPS default authenticator " +
            "(works with any AMPS server-side password module: LDAP, file/htpasswd, custom). " +
            "KERBEROS_GSSAPI uses the vendored 60East AMPSKerberosGSSAPIAuthenticator. " +
            "Provide either amps.kerberos.jaas.config (path to a JAAS file) or both amps.kerberos.keytab and amps.kerberos.client.principal " +
            "(the connector materializes a JAAS file from the bundled template).";
    private static final String AMPS_AUTH_TYPE_DEFAULT = AuthType.NONE.name();

    public static final String AMPS_USERNAME_CONF = "amps.username";
    private static final String AMPS_USERNAME_DOC = "Username sent to AMPS when amps.auth.type=PASSWORD.";
    private static final String AMPS_USERNAME_DEFAULT = "";

    public static final String AMPS_PASSWORD_CONF = "amps.password";
    private static final String AMPS_PASSWORD_DOC = "Password sent to AMPS when amps.auth.type=PASSWORD. " +
            "Use a Kafka Connect ConfigProvider (e.g. ${file:/path:amps_password}) to avoid storing secrets in plaintext.";

    public static final String AMPS_KERBEROS_SPN_CONF = "amps.kerberos.spn";
    private static final String AMPS_KERBEROS_SPN_DOC = "Service Principal Name of the AMPS server, e.g. AMPS/amps-host.example.com. " +
            "Required when amps.auth.type=KERBEROS_GSSAPI.";
    private static final String AMPS_KERBEROS_SPN_DEFAULT = "";

    public static final String AMPS_KERBEROS_JAAS_ENTRY_CONF = "amps.kerberos.jaas.entry";
    private static final String AMPS_KERBEROS_JAAS_ENTRY_DOC = "JAAS login-context entry name used by the Kerberos authenticator.";
    private static final String AMPS_KERBEROS_JAAS_ENTRY_DEFAULT = "AMPSClient";

    public static final String AMPS_KERBEROS_JAAS_CONFIG_CONF = "amps.kerberos.jaas.config";
    private static final String AMPS_KERBEROS_JAAS_CONFIG_DOC = "Path to a JAAS configuration file. " +
            "When set, the connector sets java.security.auth.login.config to this path " +
            "(only if the property is not already set on the worker JVM). " +
            "Leave empty if you use amps.kerberos.keytab + amps.kerberos.client.principal instead; " +
            "the connector will materialize a JAAS file from the bundled template at runtime.";
    private static final String AMPS_KERBEROS_JAAS_CONFIG_DEFAULT = "";

    public static final String AMPS_KERBEROS_KEYTAB_CONF = "amps.kerberos.keytab";
    private static final String AMPS_KERBEROS_KEYTAB_DOC = "Absolute path to the Kerberos keytab for the client principal. " +
            "When amps.kerberos.jaas.config is empty, set this together with amps.kerberos.client.principal " +
            "so the connector can build a JAAS file automatically (no separate jaas.conf on disk required).";
    private static final String AMPS_KERBEROS_KEYTAB_DEFAULT = "";

    public static final String AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF = "amps.kerberos.client.principal";
    private static final String AMPS_KERBEROS_CLIENT_PRINCIPAL_DOC = "Client Kerberos principal stored in amps.kerberos.keytab, e.g. kafka-connect@CORP.EXAMPLE.COM. " +
            "Required together with amps.kerberos.keytab when amps.kerberos.jaas.config is empty.";
    private static final String AMPS_KERBEROS_CLIENT_PRINCIPAL_DEFAULT = "";

    public static final String AMPS_KERBEROS_LIFETIME_CONF = "amps.kerberos.lifetime.seconds";
    private static final String AMPS_KERBEROS_LIFETIME_DOC = "Credential lifetime (seconds) passed to AMPSKerberosGSSAPIAuthenticator. " +
            "Default is 8 hours (28800).";
    private static final int AMPS_KERBEROS_LIFETIME_DEFAULT = 8 * 3600;

    public static final String AMPS_KERBEROS_DEBUG_CONF = "amps.kerberos.debug";
    private static final String AMPS_KERBEROS_DEBUG_DOC = "When true, sets sun.security.krb5.debug=true (and sun.security.jgss.debug=true) "
            + "before the Kerberos logon. JDK Kerberos traces go to stderr, which SingleStore captures in the extractor log. "
            + "Leave false in production; the output is verbose and can include principal names and KDC hosts.";
    private static final boolean AMPS_KERBEROS_DEBUG_DEFAULT = false;

    public static final String AMPS_KERBEROS_FORCE_TCP_CONF = "amps.kerberos.force.tcp";
    private static final String AMPS_KERBEROS_FORCE_TCP_DOC = "When true (default), the connector copies the leaf krb5.conf to a temp file, "
            + "appends udp_preference_limit=1, and points java.security.krb5.conf at that file. "
            + "Java GSS otherwise stays on UDP/88 and will not fall back to TCP unless the KDC replies KRB_ERR_RESPONSE_TOO_BIG, "
            + "so a filtered UDP reply looks like a 90s hang. MIT kinit retries TCP on its own; this makes the extractor JVM match. "
            + "Does not edit /etc/krb5.conf on the host. Set false to use the host file unchanged. "
            + "Ignored if java.security.krb5.conf is already set on the JVM.";
    private static final boolean AMPS_KERBEROS_FORCE_TCP_DEFAULT = true;

    public static final String AMPS_KERBEROS_KRB5_CONF_CONF = "amps.kerberos.krb5.conf";
    private static final String AMPS_KERBEROS_KRB5_CONF_DOC = "Path to the host krb5.conf copied when amps.kerberos.force.tcp=true. "
            + "Empty (default) means /etc/krb5.conf.";
    private static final String AMPS_KERBEROS_KRB5_CONF_DEFAULT = "";

    public static final String AMPS_SSL_TRUST_CERTIFICATE_CONF = "amps.ssl.trust.certificate";
    private static final String AMPS_SSL_TRUST_CERTIFICATE_DOC = "Path to one or more PEM/DER CA or server certificates the extractor should trust "
            + "for tcps:// AMPS connections. Comma-separated if you have a chain split across files. "
            + "The connector loads them into an AMPS-only SSLContext (does not change the JVM default truststore).";
    private static final String AMPS_SSL_TRUST_CERTIFICATE_DEFAULT = "";

    public static final String AMPS_SSL_TRUSTSTORE_CONF = "amps.ssl.truststore";
    private static final String AMPS_SSL_TRUSTSTORE_DOC = "Path to a Java truststore (JKS or PKCS12) of CAs that signed the AMPS server certificate. "
            + "Use this instead of (or in addition to) amps.ssl.trust.certificate.";
    private static final String AMPS_SSL_TRUSTSTORE_DEFAULT = "";

    public static final String AMPS_SSL_TRUSTSTORE_PASSWORD_CONF = "amps.ssl.truststore.password";
    private static final String AMPS_SSL_TRUSTSTORE_PASSWORD_DOC = "Password for amps.ssl.truststore. Optional for some PKCS12 files.";

    public static final String AMPS_SSL_TRUSTSTORE_TYPE_CONF = "amps.ssl.truststore.type";
    private static final String AMPS_SSL_TRUSTSTORE_TYPE_DOC = "Truststore type: JKS or PKCS12. Empty means PKCS12 for .p12/.pfx, otherwise JKS.";
    private static final String AMPS_SSL_TRUSTSTORE_TYPE_DEFAULT = "";

    public static final String AMPS_SSL_KEYSTORE_CONF = "amps.ssl.keystore";
    private static final String AMPS_SSL_KEYSTORE_DOC = "Path to a Java keystore (JKS or PKCS12) with the client certificate and private key. "
            + "Required only when AMPS VerifyClient is true (mutual TLS).";
    private static final String AMPS_SSL_KEYSTORE_DEFAULT = "";

    public static final String AMPS_SSL_KEYSTORE_PASSWORD_CONF = "amps.ssl.keystore.password";
    private static final String AMPS_SSL_KEYSTORE_PASSWORD_DOC = "Password for amps.ssl.keystore (and the key entry, if it uses the same password).";

    public static final String AMPS_SSL_KEYSTORE_TYPE_CONF = "amps.ssl.keystore.type";
    private static final String AMPS_SSL_KEYSTORE_TYPE_DOC = "Keystore type: JKS or PKCS12. Empty means PKCS12 for .p12/.pfx, otherwise JKS.";
    private static final String AMPS_SSL_KEYSTORE_TYPE_DEFAULT = "";

    // Field accessors
    public final String ampsUri;
    public final String ampsClientName;
    public final String ampsTopic;
    public final String kafkaTopicPrefix;
    public final Position ampsPosition;
    public final String ampsStartBookmark;
    public final SubscriptionType ampsSubscriptionType;
    public final String ampsFilter;
    public final int batchSize;
    public final int pollTimeoutMs;
    public final int pollIdleMs;
    public final int ampsSowQueryBatchSize;
    public final int ampsConnectTimeoutMs;

    public final AuthType ampsAuthType;
    public final String ampsUsername;
    public final Password ampsPassword;
    public final String ampsKerberosSpn;
    public final String ampsKerberosJaasEntry;
    public final String ampsKerberosJaasConfig;
    public final String ampsKerberosKeytab;
    public final String ampsKerberosClientPrincipal;
    public final int ampsKerberosLifetimeSeconds;
    public final boolean ampsKerberosDebug;
    public final boolean ampsKerberosForceTcp;
    public final String ampsKerberosKrb5Conf;
    public final String ampsSslTrustCertificate;
    public final String ampsSslTruststore;
    public final Password ampsSslTruststorePassword;
    public final String ampsSslTruststoreType;
    public final String ampsSslKeystore;
    public final Password ampsSslKeystorePassword;
    public final String ampsSslKeystoreType;

    public AMPSSourceConnectorConfig(Map<String, String> props) {
        super(configDef(), props);

        this.ampsUri = getString(AMPS_URI_CONF);
        this.ampsClientName = getString(AMPS_CLIENT_NAME_CONF);
        this.ampsTopic = getString(AMPS_TOPIC_CONF);
        this.kafkaTopicPrefix = getString(KAFKA_TOPIC_PREFIX_CONF);
        this.ampsPosition = Position.fromString(getString(AMPS_POSITION_CONF));
        this.ampsStartBookmark = getString(AMPS_START_BOOKMARK_CONF);
        this.ampsSubscriptionType = SubscriptionType.fromString(getString(AMPS_SUBSCRIPTION_TYPE_CONF));
        this.ampsFilter = getString(AMPS_FILTER_CONF);
        this.batchSize = getInt(BATCH_SIZE_CONF);
        this.pollTimeoutMs = getInt(POLL_TIMEOUT_MS_CONF);
        this.pollIdleMs = getInt(POLL_IDLE_MS_CONF);
        this.ampsSowQueryBatchSize = getInt(AMPS_SOW_QUERY_BATCH_SIZE_CONF);
        this.ampsConnectTimeoutMs = getInt(AMPS_CONNECT_TIMEOUT_MS_CONF);

        this.ampsAuthType = AuthType.fromString(getString(AMPS_AUTH_TYPE_CONF));
        this.ampsUsername = getString(AMPS_USERNAME_CONF);
        this.ampsPassword = getPassword(AMPS_PASSWORD_CONF);
        this.ampsKerberosSpn = getString(AMPS_KERBEROS_SPN_CONF);
        this.ampsKerberosJaasEntry = getString(AMPS_KERBEROS_JAAS_ENTRY_CONF);
        this.ampsKerberosJaasConfig = getString(AMPS_KERBEROS_JAAS_CONFIG_CONF);
        this.ampsKerberosKeytab = getString(AMPS_KERBEROS_KEYTAB_CONF);
        this.ampsKerberosClientPrincipal = getString(AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF);
        this.ampsKerberosLifetimeSeconds = getInt(AMPS_KERBEROS_LIFETIME_CONF);
        this.ampsKerberosDebug = getBoolean(AMPS_KERBEROS_DEBUG_CONF);
        this.ampsKerberosForceTcp = getBoolean(AMPS_KERBEROS_FORCE_TCP_CONF);
        this.ampsKerberosKrb5Conf = getString(AMPS_KERBEROS_KRB5_CONF_CONF);
        this.ampsSslTrustCertificate = getString(AMPS_SSL_TRUST_CERTIFICATE_CONF);
        this.ampsSslTruststore = getString(AMPS_SSL_TRUSTSTORE_CONF);
        this.ampsSslTruststorePassword = getPassword(AMPS_SSL_TRUSTSTORE_PASSWORD_CONF);
        this.ampsSslTruststoreType = getString(AMPS_SSL_TRUSTSTORE_TYPE_CONF);
        this.ampsSslKeystore = getString(AMPS_SSL_KEYSTORE_CONF);
        this.ampsSslKeystorePassword = getPassword(AMPS_SSL_KEYSTORE_PASSWORD_CONF);
        this.ampsSslKeystoreType = getString(AMPS_SSL_KEYSTORE_TYPE_CONF);

        validateAuthConfig();
        validateSslConfig();
    }

    private void validateAuthConfig() {
        switch (ampsAuthType) {
            case PASSWORD:
                if (isBlank(ampsUsername)) {
                    throw new ConfigException(AMPS_USERNAME_CONF,
                            ampsUsername,
                            "Required when " + AMPS_AUTH_TYPE_CONF + "=PASSWORD");
                }
                if (ampsPassword == null || isBlank(ampsPassword.value())) {
                    throw new ConfigException(AMPS_PASSWORD_CONF,
                            "(hidden)",
                            "Required when " + AMPS_AUTH_TYPE_CONF + "=PASSWORD");
                }
                break;
            case KERBEROS_GSSAPI:
                if (isBlank(ampsKerberosSpn)) {
                    throw new ConfigException(AMPS_KERBEROS_SPN_CONF,
                            ampsKerberosSpn,
                            "Required when " + AMPS_AUTH_TYPE_CONF + "=KERBEROS_GSSAPI");
                }
                boolean hasJaasFile = !isBlank(ampsKerberosJaasConfig);
                boolean hasKeytabPair = !isBlank(ampsKerberosKeytab) && !isBlank(ampsKerberosClientPrincipal);
                if (!hasJaasFile && !hasKeytabPair) {
                    throw new ConfigException("Kerberos GSSAPI requires either "
                            + AMPS_KERBEROS_JAAS_CONFIG_CONF
                            + " (path to a JAAS file) or both "
                            + AMPS_KERBEROS_KEYTAB_CONF + " and "
                            + AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF
                            + " (built-in keytab JAAS materialization).");
                }
                break;
            case NONE:
            default:
                break;
        }
    }

    private void validateSslConfig() {
        boolean sslSet = !isBlank(ampsSslTrustCertificate) || !isBlank(ampsSslTruststore) || !isBlank(ampsSslKeystore);
        if (!sslSet) {
            return;
        }
        String scheme = uriScheme(ampsUri);
        if (scheme == null || !scheme.equalsIgnoreCase("tcps")) {
            throw new ConfigException(AMPS_URI_CONF, ampsUri,
                    "amps.ssl.* is set, so amps.uri must use tcps:// (TLS). "
                            + "tcp:// is plaintext and ignores the certificate files.");
        }
    }

    private static String uriScheme(String uri) {
        if (uri == null) {
            return null;
        }
        int colon = uri.indexOf(':');
        return colon <= 0 ? null : uri.substring(0, colon);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static ConfigDef configDef() {
        return new ConfigDef()
                // AMPS Connection
                .define(AMPS_URI_CONF, Type.STRING, Importance.HIGH, AMPS_URI_DOC)
                .define(AMPS_CLIENT_NAME_CONF, Type.STRING, AMPS_CLIENT_NAME_DEFAULT, Importance.MEDIUM, AMPS_CLIENT_NAME_DOC)

                // Topic Configuration
                .define(AMPS_TOPIC_CONF,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.CompositeValidator.of(
                            new ConfigDef.NonNullValidator(),
                            new ConfigDef.NonEmptyString()),
                        Importance.HIGH,
                        AMPS_TOPIC_DOC)

                // Kafka Configuration
                .define(KAFKA_TOPIC_PREFIX_CONF, Type.STRING, KAFKA_TOPIC_PREFIX_DEFAULT, Importance.MEDIUM, KAFKA_TOPIC_PREFIX_DOC)

                // Position/Offset Configuration
                .define(AMPS_POSITION_CONF, Type.STRING, AMPS_POSITION_DEFAULT, Position.VALIDATOR, Importance.MEDIUM, AMPS_POSITION_DOC)
                .define(AMPS_START_BOOKMARK_CONF, Type.STRING, AMPS_START_BOOKMARK_DEFAULT, Importance.LOW, AMPS_START_BOOKMARK_DOC)

                // Subscription Configuration
                .define(AMPS_SUBSCRIPTION_TYPE_CONF, Type.STRING, AMPS_SUBSCRIPTION_TYPE_DEFAULT, SubscriptionType.VALIDATOR, Importance.MEDIUM, AMPS_SUBSCRIPTION_TYPE_DOC)
                .define(AMPS_FILTER_CONF, Type.STRING, AMPS_FILTER_DEFAULT, Importance.LOW, AMPS_FILTER_DOC)

                // Performance Configuration
                .define(BATCH_SIZE_CONF, Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Range.between(1, 10000), Importance.MEDIUM, BATCH_SIZE_DOC)
                .define(POLL_TIMEOUT_MS_CONF, Type.INT, POLL_TIMEOUT_MS_DEFAULT, ConfigDef.Range.between(100, 60000), Importance.LOW, POLL_TIMEOUT_MS_DOC)
                .define(POLL_IDLE_MS_CONF, Type.INT, POLL_IDLE_MS_DEFAULT, ConfigDef.Range.between(10, 5000), Importance.LOW, POLL_IDLE_MS_DOC)
                .define(AMPS_SOW_QUERY_BATCH_SIZE_CONF, Type.INT, AMPS_SOW_QUERY_BATCH_SIZE_DEFAULT, ConfigDef.Range.between(0, 10000), Importance.LOW, AMPS_SOW_QUERY_BATCH_SIZE_DOC)
                .define(AMPS_CONNECT_TIMEOUT_MS_CONF, Type.INT, AMPS_CONNECT_TIMEOUT_MS_DEFAULT, ConfigDef.Range.between(1000, 120000), Importance.LOW, AMPS_CONNECT_TIMEOUT_MS_DOC)

                // Authentication Configuration
                .define(AMPS_AUTH_TYPE_CONF, Type.STRING, AMPS_AUTH_TYPE_DEFAULT, AuthType.VALIDATOR, Importance.MEDIUM, AMPS_AUTH_TYPE_DOC)
                .define(AMPS_USERNAME_CONF, Type.STRING, AMPS_USERNAME_DEFAULT, Importance.MEDIUM, AMPS_USERNAME_DOC)
                .define(AMPS_PASSWORD_CONF, Type.PASSWORD, null, Importance.MEDIUM, AMPS_PASSWORD_DOC)
                .define(AMPS_KERBEROS_SPN_CONF, Type.STRING, AMPS_KERBEROS_SPN_DEFAULT, Importance.MEDIUM, AMPS_KERBEROS_SPN_DOC)
                .define(AMPS_KERBEROS_JAAS_ENTRY_CONF, Type.STRING, AMPS_KERBEROS_JAAS_ENTRY_DEFAULT, Importance.LOW, AMPS_KERBEROS_JAAS_ENTRY_DOC)
                .define(AMPS_KERBEROS_JAAS_CONFIG_CONF, Type.STRING, AMPS_KERBEROS_JAAS_CONFIG_DEFAULT, Importance.LOW, AMPS_KERBEROS_JAAS_CONFIG_DOC)
                .define(AMPS_KERBEROS_KEYTAB_CONF, Type.STRING, AMPS_KERBEROS_KEYTAB_DEFAULT, Importance.LOW, AMPS_KERBEROS_KEYTAB_DOC)
                .define(AMPS_KERBEROS_CLIENT_PRINCIPAL_CONF, Type.STRING, AMPS_KERBEROS_CLIENT_PRINCIPAL_DEFAULT, Importance.LOW, AMPS_KERBEROS_CLIENT_PRINCIPAL_DOC)
                .define(AMPS_KERBEROS_LIFETIME_CONF, Type.INT, AMPS_KERBEROS_LIFETIME_DEFAULT, ConfigDef.Range.between(60, 2_592_000), Importance.LOW, AMPS_KERBEROS_LIFETIME_DOC)
                .define(AMPS_KERBEROS_DEBUG_CONF, Type.BOOLEAN, AMPS_KERBEROS_DEBUG_DEFAULT, Importance.LOW, AMPS_KERBEROS_DEBUG_DOC)
                .define(AMPS_KERBEROS_FORCE_TCP_CONF, Type.BOOLEAN, AMPS_KERBEROS_FORCE_TCP_DEFAULT, Importance.LOW, AMPS_KERBEROS_FORCE_TCP_DOC)
                .define(AMPS_KERBEROS_KRB5_CONF_CONF, Type.STRING, AMPS_KERBEROS_KRB5_CONF_DEFAULT, Importance.LOW, AMPS_KERBEROS_KRB5_CONF_DOC)
                .define(AMPS_SSL_TRUST_CERTIFICATE_CONF, Type.STRING, AMPS_SSL_TRUST_CERTIFICATE_DEFAULT, Importance.MEDIUM, AMPS_SSL_TRUST_CERTIFICATE_DOC)
                .define(AMPS_SSL_TRUSTSTORE_CONF, Type.STRING, AMPS_SSL_TRUSTSTORE_DEFAULT, Importance.MEDIUM, AMPS_SSL_TRUSTSTORE_DOC)
                .define(AMPS_SSL_TRUSTSTORE_PASSWORD_CONF, Type.PASSWORD, null, Importance.MEDIUM, AMPS_SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(AMPS_SSL_TRUSTSTORE_TYPE_CONF, Type.STRING, AMPS_SSL_TRUSTSTORE_TYPE_DEFAULT, Importance.LOW, AMPS_SSL_TRUSTSTORE_TYPE_DOC)
                .define(AMPS_SSL_KEYSTORE_CONF, Type.STRING, AMPS_SSL_KEYSTORE_DEFAULT, Importance.LOW, AMPS_SSL_KEYSTORE_DOC)
                .define(AMPS_SSL_KEYSTORE_PASSWORD_CONF, Type.PASSWORD, null, Importance.LOW, AMPS_SSL_KEYSTORE_PASSWORD_DOC)
                .define(AMPS_SSL_KEYSTORE_TYPE_CONF, Type.STRING, AMPS_SSL_KEYSTORE_TYPE_DEFAULT, Importance.LOW, AMPS_SSL_KEYSTORE_TYPE_DOC);
    }

    public String getKafkaTopic() {
        return kafkaTopicPrefix + ampsTopic;
    }

    public String getClientNameForTopic() {
        return ampsClientName + ampsTopic;
    }

    public int getEffectiveSowQueryBatchSize() {
        return ampsSowQueryBatchSize > 0 ? ampsSowQueryBatchSize : batchSize;
    }
}
