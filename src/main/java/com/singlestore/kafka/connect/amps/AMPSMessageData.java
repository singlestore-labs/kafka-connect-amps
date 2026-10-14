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

    /**
     * Creates a new AMPSMessageData with copied values.
     *
     * @param data          The message payload data
     * @param bookmark      The AMPS bookmark for offset tracking
     */
    public AMPSMessageData(String data, String bookmark) {
        this.data = data;
        this.bookmark = bookmark;
    }

    public String getData() {
        return data;
    }

    public String getBookmark() {
        return bookmark;
    }

    @Override
    public String toString() {
        return "AMPSMessageData{" +
                "bookmark='" + bookmark + '\'' +
                ", dataLength=" + (data != null ? data.length() : 0) +
                '}';
    }
}
