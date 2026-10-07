package com.singlestore.kafka.connect.amps;

/**
 * Immutable POJO that holds a copy of AMPS message data.
 * 
 * This class is used to immediately copy data from AMPS Message objects
 * before they are reused by the AMPS client. AMPS reuses Message objects
 * as buffers, so data must be copied before queuing for later processing.
 */
public class AMPSMessageData {

    private final String data;
    private final String bookmark;
    private final String correlationId;
    private final String sowKey;
    private final int command;

    /**
     * Creates a new AMPSMessageData with copied values.
     *
     * @param data          The message payload data
     * @param bookmark      The AMPS bookmark for offset tracking
     * @param correlationId The correlation ID if present
     * @param sowKey        The SOW key if present
     * @param command       The AMPS command type
     */
    public AMPSMessageData(String data, String bookmark, String correlationId, String sowKey, int command) {
        this.data = data;
        this.bookmark = bookmark;
        this.correlationId = correlationId;
        this.sowKey = sowKey;
        this.command = command;
    }

    public String getData() {
        return data;
    }

    public String getBookmark() {
        return bookmark;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getSowKey() {
        return sowKey;
    }

    public int getCommand() {
        return command;
    }

    @Override
    public String toString() {
        return "AMPSMessageData{" +
                "bookmark='" + bookmark + '\'' +
                ", command=" + command +
                ", dataLength=" + (data != null ? data.length() : 0) +
                '}';
    }
}
