package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.ConnectionInfo;
import com.crankuptheamps.client.DefaultServerChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DefaultServerChooser} that remembers why the last connect/logon attempt failed.
 *
 * <p>{@code DefaultServerChooser.getError()} always returns an empty string and its
 * {@code reportFailure} discards the exception, so a failed {@code connectAndLogon()} surfaces only
 * "The last connection error was: " with nothing after it. That hides the distinction between an
 * unreachable host, a Kerberos ticket problem, and an AMPS-side auth rejection.</p>
 */
public class LastErrorServerChooser extends DefaultServerChooser {

    private static final Logger log = LoggerFactory.getLogger(LastErrorServerChooser.class);

    private volatile String lastError = "";

    @Override
    public void reportFailure(Exception exception, ConnectionInfo connectionInfo) throws Exception {
        lastError = describe(exception);
        log.warn("AMPS connect/logon attempt failed: {}", lastError);
        log.debug("AMPS connect/logon failure detail", exception);
        super.reportFailure(exception, connectionInfo);
    }

    @Override
    public String getError() {
        return lastError;
    }

    /**
     * Renders the whole cause chain, since the AMPS client wraps the interesting failure (GSS,
     * socket, auth ack) several layers down and only the innermost message names the real problem.
     */
    static String describe(Throwable throwable) {
        if (throwable == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Throwable current = throwable;
        int depth = 0;
        while (current != null && depth < 10) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(current.getClass().getSimpleName());
            String message = current.getMessage();
            if (message != null && !message.trim().isEmpty()) {
                sb.append(": ").append(message.trim());
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
            depth++;
        }
        return sb.toString();
    }
}
