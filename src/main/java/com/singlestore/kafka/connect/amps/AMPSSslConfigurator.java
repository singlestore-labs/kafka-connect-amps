package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.TCPSTransport;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Locale;

/**
 * Builds an {@link SSLContext} from pipeline CONFIG (PEM certs and/or Java keystores)
 * and installs it on {@link TCPSTransport} so {@code tcps://} connections use those
 * files without JVM-wide {@code javax.net.ssl.trustStore} flags.
 */
public final class AMPSSslConfigurator {

    private static final Logger log = LoggerFactory.getLogger(AMPSSslConfigurator.class);

    private AMPSSslConfigurator() {
    }

    static boolean isConfigured(AMPSSourceConnectorConfig config) {
        return !isBlank(config.ampsSslTrustCertificate)
                || !isBlank(config.ampsSslTruststore)
                || !isBlank(config.ampsSslKeystore);
    }

    /**
     * Installs an AMPS-only SSLContext when any {@code amps.ssl.*} path is set.
     * Must run before {@code connectAndLogon()}.
     */
    static void apply(AMPSSourceConnectorConfig config) {
        if (!isConfigured(config)) {
            return;
        }
        try {
            SSLContext context = buildContext(config);
            TCPSTransport.setDefaultSSLContext(context);
            log.info("AMPS TLS enabled (trust.certificate={}, truststore={}, keystore={})",
                    blankToDash(config.ampsSslTrustCertificate),
                    blankToDash(config.ampsSslTruststore),
                    blankToDash(config.ampsSslKeystore));
        } catch (ConnectException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectException("Failed to load AMPS TLS material: " + e.getMessage(), e);
        }
    }

    static SSLContext buildContext(AMPSSourceConnectorConfig config) throws Exception {
        TrustManager[] trustManagers = null;
        KeyStore trustStore = loadTrustStore(config);
        if (trustStore != null) {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            trustManagers = tmf.getTrustManagers();
        }

        KeyManager[] keyManagers = null;
        if (!isBlank(config.ampsSslKeystore)) {
            KeyStore keyStore = loadJavaKeyStore(
                    config.ampsSslKeystore.trim(),
                    passwordChars(config.ampsSslKeystorePassword),
                    config.ampsSslKeystoreType,
                    true);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            char[] keyPassword = passwordChars(config.ampsSslKeystorePassword);
            kmf.init(keyStore, keyPassword);
            keyManagers = kmf.getKeyManagers();
        }

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers, trustManagers, null);
        return context;
    }

    /**
     * Trust comes from PEM/DER files ({@code amps.ssl.trust.certificate}, comma-separated)
     * and/or a Java truststore. Both may be set; entries are merged.
     */
    static KeyStore loadTrustStore(AMPSSourceConnectorConfig config) throws Exception {
        if (isBlank(config.ampsSslTrustCertificate) && isBlank(config.ampsSslTruststore)) {
            return null;
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        int aliases = 0;
        if (!isBlank(config.ampsSslTrustCertificate)) {
            for (String rawPath : config.ampsSslTrustCertificate.split(",")) {
                String path = rawPath.trim();
                if (path.isEmpty()) {
                    continue;
                }
                aliases += importCertificates(store, path, aliases);
            }
        }
        if (!isBlank(config.ampsSslTruststore)) {
            KeyStore extra = loadJavaKeyStore(
                    config.ampsSslTruststore.trim(),
                    passwordChars(config.ampsSslTruststorePassword),
                    config.ampsSslTruststoreType,
                    false);
            java.util.Enumeration<String> names = extra.aliases();
            while (names.hasMoreElements()) {
                String alias = names.nextElement();
                Certificate cert = extra.getCertificate(alias);
                if (cert != null) {
                    store.setCertificateEntry("imported-" + aliases++, cert);
                }
            }
        }
        if (aliases == 0) {
            throw new ConnectException("No certificates found in amps.ssl.trust.certificate / amps.ssl.truststore");
        }
        return store;
    }

    static int importCertificates(KeyStore store, String path, int startIndex) throws Exception {
        Path file = Path.of(path);
        if (!Files.isRegularFile(file) || !Files.isReadable(file)) {
            throw new ConnectException("Cannot read amps.ssl.trust.certificate '" + path + "'");
        }
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certs;
        try (InputStream in = Files.newInputStream(file)) {
            certs = factory.generateCertificates(in);
        }
        if (certs == null || certs.isEmpty()) {
            throw new ConnectException("No X.509 certificates in " + path);
        }
        int i = startIndex;
        for (Certificate cert : certs) {
            store.setCertificateEntry("amps-trust-" + i++, cert);
        }
        return i - startIndex;
    }

    static KeyStore loadJavaKeyStore(String path, char[] password, String type, boolean requiredForClient) throws Exception {
        Path file = Path.of(path);
        if (!Files.isRegularFile(file) || !Files.isReadable(file)) {
            throw new ConnectException("Cannot read " + (requiredForClient ? "amps.ssl.keystore" : "amps.ssl.truststore")
                    + " '" + path + "'");
        }
        String storeType = inferStoreType(path, type);
        KeyStore store = KeyStore.getInstance(storeType);
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, password);
        }
        return store;
    }

    static String inferStoreType(String path, String configured) {
        if (configured != null && !configured.trim().isEmpty()) {
            return configured.trim().toUpperCase(Locale.ROOT);
        }
        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".p12") || lower.endsWith(".pfx") || lower.endsWith(".pkcs12")) {
            return "PKCS12";
        }
        return "JKS";
    }

    private static char[] passwordChars(org.apache.kafka.common.config.types.Password password) {
        if (password == null || password.value() == null || password.value().isEmpty()) {
            return new char[0];
        }
        return password.value().toCharArray();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String blankToDash(String s) {
        return isBlank(s) ? "-" : s.trim();
    }
}
