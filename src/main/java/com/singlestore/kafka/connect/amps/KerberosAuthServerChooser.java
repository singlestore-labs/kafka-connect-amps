package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.Authenticator;
import com.crankuptheamps.client.DefaultServerChooser;

import java.util.Objects;

/**
 * A {@link LastErrorServerChooser} that always returns a fixed {@link Authenticator}
 * from {@link #getCurrentAuthenticator()}.
 *
 * <p>The {@link com.crankuptheamps.client.HAClient} consults the server chooser on the
 * initial logon and on every reconnect, so this is the supported way to plug a
 * Kerberos {@link Authenticator} into HA reconnect-aware logon flows.</p>
 */
public class KerberosAuthServerChooser extends LastErrorServerChooser {

    private final Authenticator authenticator;

    public KerberosAuthServerChooser(String uri, Authenticator authenticator) {
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(authenticator, "authenticator");
        add(uri);
        this.authenticator = authenticator;
    }

    @Override
    public Authenticator getCurrentAuthenticator() {
        return authenticator;
    }
}
