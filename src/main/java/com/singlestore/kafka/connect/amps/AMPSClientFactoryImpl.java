package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.authentication.kerberos.AMPSKerberosGSSAPIAuthenticator;
import com.crankuptheamps.client.Authenticator;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.ExponentialDelayStrategy;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.MemoryBookmarkStore;
import com.crankuptheamps.client.ServerChooser;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Default implementation of AMPSClientFactory that creates real AMPS connections.
 *
 * <p>Authentication is selected by {@code amps.auth.type}:
 * <ul>
 *   <li>{@code NONE}: connect using the URI as-is.</li>
 *   <li>{@code PASSWORD}: rewrite the URI to embed username/password
 *       ({@code tcp://user:password@host:port/...}); the AMPS default authenticator
 *       forwards them to the server, where the configured AMPS module
 *       (LDAP/file/htpasswd/etc.) validates them.</li>
 *   <li>{@code KERBEROS_GSSAPI}: install a {@link KerberosAuthServerChooser} that
 *       returns an {@link AMPSKerberosGSSAPIAuthenticator}. The HAClient consults
 *       the chooser on every (re)connect, so HA reconnects re-authenticate
 *       automatically.</li>
 * </ul>
 *
 * <p>Subscribe-on-demand polling uses a {@link MemoryBookmarkStore} so rapid
 * subscribe/unsubscribe cycles do not corrupt the default on-disk bookmark store.
 * Bookmarks are tracked by {@code AMPSSourceTask}.</p>
 */
public class AMPSClientFactoryImpl implements AMPSClientFactory {

    private static final Logger log = LoggerFactory.getLogger(AMPSClientFactoryImpl.class);

    static final String JAAS_SYSTEM_PROPERTY = "java.security.auth.login.config";
    static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
    static final String KRB5_DEBUG_PROPERTY = "sun.security.krb5.debug";
    static final String JGSS_DEBUG_PROPERTY = "sun.security.jgss.debug";

    @Override
    public HAClient create(AMPSSourceConnectorConfig config, String clientNameSuffix) {
        String clientName = config.getClientNameForTopic(clientNameSuffix);
        String uri = config.ampsUri;

        log.info("Creating AMPS client '{}' connecting to '{}' (auth={})",
                clientName, uri, config.ampsAuthType);

        applyKerberosForceTcp(config);
        applyKerberosDebug(config);
        AMPSSslConfigurator.apply(config);

        long startedAt = System.currentTimeMillis();
        ServerChooser chooser = null;
        try {
            HAClient client = new HAClient(clientName);

            chooser = buildServerChooser(config, uri);
            client.setServerChooser(chooser);

            // MemoryBookmarkStore avoids DefaultBookmarkStore corruption from subscribe/unsubscribe per poll.
            // AMPSSourceTask tracks bookmarks itself.
            client.setBookmarkStore(new MemoryBookmarkStore());
            log.info("Using MemoryBookmarkStore for subscribe-on-demand polling");

            configureConnectRetry(client, config.ampsConnectTimeoutMs);
            log.info("AMPS connect/logon timeout is {} ms", config.ampsConnectTimeoutMs);

            client.connectAndLogon();

            log.info("Successfully connected AMPS client '{}'", clientName);
            return client;

        } catch (Exception e) {
            throw failedToConnect(clientName, uri, config, chooser,
                    System.currentTimeMillis() - startedAt, e);
        }
    }

    /** Visible for testing. */
    static ServerChooser buildServerChooser(AMPSSourceConnectorConfig config, String uri) {
        switch (config.ampsAuthType) {
            case PASSWORD: {
                String authedUri = injectCredentials(uri, config.ampsUsername,
                        config.ampsPassword == null ? null : config.ampsPassword.value());
                LastErrorServerChooser chooser = new LastErrorServerChooser();
                chooser.add(authedUri);
                log.info("AMPS PASSWORD auth enabled (user='{}')", config.ampsUsername);
                return chooser;
            }
            case KERBEROS_GSSAPI: {
                prepareJaasForKerberos(config);
                Authenticator authenticator;
                try {
                    authenticator = new AMPSKerberosGSSAPIAuthenticator(
                            config.ampsKerberosSpn,
                            config.ampsKerberosJaasEntry,
                            config.ampsKerberosLifetimeSeconds);
                } catch (Exception e) {
                    throw new ConnectException(
                            "Failed to build Kerberos GSSAPI authenticator (spn=" + config.ampsKerberosSpn
                                    + ", jaas.entry=" + config.ampsKerberosJaasEntry + "): " + e.getMessage(), e);
                }
                // The AMPS client skips Authenticator.authenticate() entirely when the URI has no
                // user component, which sends a logon with no user_id and no GSS token.
                String kerberosUser = resolveKerberosUser(config);
                String userUri = injectUser(uri, kerberosUser);
                log.info("AMPS Kerberos GSSAPI auth enabled (spn={}, user={}, jaas.entry={}, lifetime={}s)",
                        config.ampsKerberosSpn, kerberosUser, config.ampsKerberosJaasEntry,
                        config.ampsKerberosLifetimeSeconds);
                return new KerberosAuthServerChooser(userUri, authenticator);
            }
            case NONE:
            default: {
                LastErrorServerChooser chooser = new LastErrorServerChooser();
                chooser.add(uri);
                return chooser;
            }
        }
    }

    /**
     * Picks the user name sent in the Kerberos logon. Prefers an explicit {@code amps.username},
     * otherwise uses the local part of {@code amps.kerberos.client.principal}
     * ({@code ampsusr@CORP.EXAMPLE.COM} becomes {@code ampsusr}).
     *
     * <p>Falls back to the JVM user when neither is set, which happens with an external JAAS file.
     * The GSS token carries the real identity; this value only populates the logon's user id, and
     * it must be non-empty or the AMPS client never invokes the authenticator at all.</p>
     */
    static String resolveKerberosUser(AMPSSourceConnectorConfig config) {
        if (!isBlank(config.ampsUsername)) {
            return config.ampsUsername.trim();
        }
        String principal = config.ampsKerberosClientPrincipal;
        if (!isBlank(principal)) {
            String local = principal.trim();
            int at = local.indexOf('@');
            return at > 0 ? local.substring(0, at) : local;
        }
        String jvmUser = System.getProperty("user.name");
        String fallback = isBlank(jvmUser) ? "amps" : jvmUser.trim();
        log.warn("No amps.username or amps.kerberos.client.principal set; using '{}' as the AMPS logon user id. "
                + "Set amps.username to match the principal in your JAAS file.", fallback);
        return fallback;
    }

    /**
     * Rewrites an AMPS URI to carry a bare user name ({@code scheme://user@host:port/path}), with no
     * password. Kerberos credentials travel in the GSS token, not the URI.
     */
    static String injectUser(String uri, String username) {
        if (uri == null || uri.isEmpty()) {
            throw new ConnectException("amps.uri must be set");
        }
        if (isBlank(username)) {
            throw new ConnectException("A user name is required to build the AMPS Kerberos logon URI");
        }
        try {
            URI parsed = new URI(uri);
            String scheme = parsed.getScheme();
            String host = parsed.getHost();
            if (scheme == null || host == null) {
                throw new ConnectException("amps.uri is not a well-formed AMPS URI: " + uri);
            }
            if (parsed.getRawUserInfo() != null && !parsed.getRawUserInfo().isEmpty()) {
                return uri;
            }
            int port = parsed.getPort();
            String path = parsed.getRawPath() == null ? "" : parsed.getRawPath();
            String query = parsed.getRawQuery() == null ? "" : "?" + parsed.getRawQuery();
            StringBuilder sb = new StringBuilder();
            sb.append(scheme).append("://")
                    .append(URLEncoder.encode(username, StandardCharsets.UTF_8.name()))
                    .append('@').append(host);
            if (port >= 0) {
                sb.append(':').append(port);
            }
            sb.append(path).append(query);
            return sb.toString();
        } catch (URISyntaxException e) {
            throw new ConnectException("amps.uri is not a valid URI: " + uri, e);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new ConnectException("UTF-8 encoding unsupported (should be impossible)", e);
        }
    }

    /**
     * Rewrites an AMPS URI to embed a userinfo segment ({@code scheme://user:password@host:port/path}).
     * AMPS's {@code DefaultAuthenticator} (used implicitly when no Authenticator is supplied to
     * {@code logon()}) extracts those credentials from the URI and forwards them to the server.
     *
     * <p>Any existing userinfo on the input URI is replaced. Username and password are URL-encoded
     * so that characters like {@code @}, {@code :}, and {@code /} survive.</p>
     */
    static String injectCredentials(String uri, String username, String password) {
        if (uri == null || uri.isEmpty()) {
            throw new ConnectException("amps.uri must be set when amps.auth.type=PASSWORD");
        }
        if (username == null || username.isEmpty()) {
            throw new ConnectException("amps.username must be set when amps.auth.type=PASSWORD");
        }
        if (password == null || password.isEmpty()) {
            throw new ConnectException("amps.password must be set when amps.auth.type=PASSWORD");
        }
        try {
            URI parsed = new URI(uri);
            String encodedUser = URLEncoder.encode(username, StandardCharsets.UTF_8.name());
            String encodedPwd = URLEncoder.encode(password, StandardCharsets.UTF_8.name());
            String userInfo = encodedUser + ":" + encodedPwd;

            // URI(String, String, String, int, String, String, String) builds a properly-formed URI;
            // we use the string ctor here because some AMPS URIs include path segments that
            // multi-arg URI ctors percent-encode unexpectedly.
            String scheme = parsed.getScheme();
            String host = parsed.getHost();
            int port = parsed.getPort();
            String path = parsed.getRawPath() == null ? "" : parsed.getRawPath();
            String query = parsed.getRawQuery() == null ? "" : "?" + parsed.getRawQuery();
            if (scheme == null || host == null) {
                throw new ConnectException("amps.uri is not a well-formed AMPS URI: " + uri);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(scheme).append("://").append(userInfo).append('@').append(host);
            if (port >= 0) {
                sb.append(':').append(port);
            }
            sb.append(path).append(query);
            return sb.toString();
        } catch (URISyntaxException e) {
            throw new ConnectException("amps.uri is not a valid URI: " + uri, e);
        } catch (java.io.UnsupportedEncodingException e) {
            // UTF-8 is always supported.
            throw new ConnectException("UTF-8 encoding unsupported (should be impossible)", e);
        }
    }

    /**
     * If {@code jaasConfigPath} is non-empty and the system property is not already set,
     * sets {@code java.security.auth.login.config} so the JDK's Krb5LoginModule picks it up.
     * Logs a warning if a worker-level value already exists.
     */
    static void maybeSetJaasSystemProperty(String jaasConfigPath) {
        if (isBlank(jaasConfigPath)) {
            return;
        }
        synchronized (AMPSClientFactoryImpl.class) {
            String current = System.getProperty(JAAS_SYSTEM_PROPERTY);
            if (current != null && !current.isEmpty()) {
                log.warn("{} is already set to '{}'; ignoring amps.kerberos.jaas.config='{}'",
                        JAAS_SYSTEM_PROPERTY, current, jaasConfigPath);
                return;
            }
            System.setProperty(JAAS_SYSTEM_PROPERTY, jaasConfigPath.trim());
            log.info("Set {}='{}' from amps.kerberos.jaas.config", JAAS_SYSTEM_PROPERTY, jaasConfigPath);
        }
    }

    /**
     * Points the JDK at a temp krb5.conf that prefers TCP to the KDC. Must run before
     * {@code sun.security.krb5.Config} loads.
     */
    static void applyKerberosForceTcp(AMPSSourceConnectorConfig config) {
        if (config.ampsAuthType != AuthType.KERBEROS_GSSAPI || !config.ampsKerberosForceTcp) {
            return;
        }
        synchronized (AMPSClientFactoryImpl.class) {
            String current = System.getProperty(KRB5_CONF_PROPERTY);
            if (current != null && !current.isEmpty()) {
                log.warn("{} is already '{}'; not wrapping krb5.conf for TCP. "
                                + "Add udp_preference_limit=1 to that file if UDP/88 is filtered.",
                        KRB5_CONF_PROPERTY, current);
                return;
            }
            String source = isBlank(config.ampsKerberosKrb5Conf)
                    ? KerberosKrb5Materializer.DEFAULT_UNIX_KRB5_CONF
                    : config.ampsKerberosKrb5Conf.trim();
            String path = KerberosKrb5Materializer.wrapForTcp(source);
            System.setProperty(KRB5_CONF_PROPERTY, path);
            log.info("Kerberos KDC transport forced to TCP via {}='{}' (source={})",
                    KRB5_CONF_PROPERTY, path, source);
        }
    }

    /**
     * Turns on JDK Kerberos tracing when {@code amps.kerberos.debug=true}. Must run before
     * {@code Krb5} / GSS classes load, because those classes read the flags as static finals.
     * Output goes to stderr, not SLF4J.
     */
    static void applyKerberosDebug(AMPSSourceConnectorConfig config) {
        if (!config.ampsKerberosDebug) {
            return;
        }
        System.setProperty(KRB5_DEBUG_PROPERTY, "true");
        System.setProperty(JGSS_DEBUG_PROPERTY, "true");
        log.info("Kerberos debug enabled ({}=true, {}=true). JDK traces go to stderr.",
                KRB5_DEBUG_PROPERTY, JGSS_DEBUG_PROPERTY);
    }

    static void prepareJaasForKerberos(AMPSSourceConnectorConfig config) {
        applyKerberosForceTcp(config);
        applyKerberosDebug(config);
        synchronized (AMPSClientFactoryImpl.class) {
            String current = System.getProperty(JAAS_SYSTEM_PROPERTY);
            if (current != null && !current.isEmpty()) {
                if (!isBlank(config.ampsKerberosJaasConfig)
                        && !current.equals(config.ampsKerberosJaasConfig.trim())) {
                    log.warn("{} is already '{}'; not applying amps.kerberos.jaas.config='{}'",
                            JAAS_SYSTEM_PROPERTY, current, config.ampsKerberosJaasConfig);
                }
                return;
            }
            if (!isBlank(config.ampsKerberosJaasConfig)) {
                System.setProperty(JAAS_SYSTEM_PROPERTY, config.ampsKerberosJaasConfig.trim());
                log.info("Set {}='{}' from amps.kerberos.jaas.config", JAAS_SYSTEM_PROPERTY,
                        config.ampsKerberosJaasConfig);
                return;
            }
            String path = KerberosJaasMaterializer.materializeToTempFile(
                    config.ampsKerberosJaasEntry,
                    config.ampsKerberosKeytab.trim(),
                    config.ampsKerberosClientPrincipal.trim());
            System.setProperty(JAAS_SYSTEM_PROPERTY, path);
            log.info("Set {} to materialized keytab JAAS at {}", JAAS_SYSTEM_PROPERTY, path);
        }
    }

    /**
     * Caps HAClient connect/logon retries. Without a maximum retry time the client
     * reconnects forever on a refused logon (empty password, Kerberos failure, name in use).
     */
    static void configureConnectRetry(HAClient client, int timeoutMs) {
        int bounded = Math.max(1000, timeoutMs);
        client.setTimeout(bounded);
        ExponentialDelayStrategy delay = new ExponentialDelayStrategy()
                .setInitialDelay(Math.min(200, bounded))
                .setMaximumDelay(Math.min(2000, bounded))
                .setMaximumRetryTime(bounded)
                .setJitter(0.0);
        client.setReconnectDelayStrategy(delay);
    }

    /**
     * Builds the start-up failure. The AMPS client's own exception says only "the last connection
     * error was:" with nothing after it, so the underlying cause chain and the chooser's recorded
     * error are spelled out here instead.
     */
    static ConnectException failedToConnect(String clientName, String uri,
            AMPSSourceConnectorConfig config, ServerChooser chooser, long elapsedMs,
            Throwable cause) {
        String detail = LastErrorServerChooser.describe(cause);
        if (detail.isEmpty()) {
            detail = "unknown";
        }
        String recorded = "";
        if (chooser != null) {
            try {
                String fromChooser = chooser.getError();
                if (fromChooser != null && !fromChooser.trim().isEmpty()) {
                    recorded = " Last attempt failed with: " + fromChooser.trim() + ".";
                }
            } catch (Exception ignored) {
                // getError() is diagnostic only; never let it mask the original failure.
            }
        }
        String message = "Failed to connect or log on to AMPS as '" + clientName + "' at " + uri
                + " (auth=" + config.ampsAuthType + ") after " + elapsedMs + " ms, budget "
                + config.ampsConnectTimeoutMs + " ms: " + detail + "." + recorded
                + " " + hint(config);
        log.error(message, cause);
        return new ConnectException(message, cause);
    }

    private static String hint(AMPSSourceConnectorConfig config) {
        switch (config.ampsAuthType) {
            case KERBEROS_GSSAPI:
                return "If the AMPS log shows no logon attempt at all, the client never got a service ticket: "
                        + "check that the leaf can reach the KDC on port 88, that " + config.ampsKerberosKeytab
                        + " is readable by the pipeline OS user, and that a ticket for " + config.ampsKerberosSpn
                        + " can be obtained (kinit -k -t <keytab> " + config.ampsKerberosClientPrincipal
                        + " && kvno " + config.ampsKerberosSpn + "). "
                        + "If the AMPS log does show a logon, check it for 29-0105 / 02-0018.";
            case PASSWORD:
                return "Check amps.username/amps.password against the AMPS password module, "
                        + "and check AMPS logs for 02-0018.";
            case NONE:
            default:
                return "AMPS may have rejected the logon (anonymous logon disabled, client name in use) "
                        + "or the host may be unreachable. Check AMPS logs for 02-0018 / 02-0025.";
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
