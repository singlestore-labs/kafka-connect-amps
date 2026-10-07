package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.Authenticator;
import com.crankuptheamps.client.DefaultAuthenticator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KerberosAuthServerChooserTest {

    @Test
    void returnsConfiguredAuthenticator() {
        Authenticator auth = new DefaultAuthenticator();
        KerberosAuthServerChooser chooser = new KerberosAuthServerChooser(
                "tcp://amps-host.example.com:9007/amps/json", auth);

        assertSame(auth, chooser.getCurrentAuthenticator());
        assertEquals("tcp://amps-host.example.com:9007/amps/json", chooser.getCurrentURI());
    }

    @Test
    void returnsSameAuthenticatorAcrossCalls() {
        Authenticator auth = new DefaultAuthenticator();
        KerberosAuthServerChooser chooser = new KerberosAuthServerChooser(
                "tcp://h:9007/amps/json", auth);

        Authenticator first = chooser.getCurrentAuthenticator();
        Authenticator second = chooser.getCurrentAuthenticator();
        assertSame(first, second, "Same authenticator should be returned on every reconnect");
    }

    @Test
    void rejectsNullArguments() {
        assertThrows(NullPointerException.class,
                () -> new KerberosAuthServerChooser(null, new DefaultAuthenticator()));
        assertThrows(NullPointerException.class,
                () -> new KerberosAuthServerChooser("tcp://h:9007/amps/json", null));
    }
}
