package com.singlestore.kafka.connect.amps;

import static com.singlestore.kafka.connect.amps.StringUtils.isNullOrEmpty;

/**
 * Immutable POJO that holds a copy of AMPS message data.
 * 
 * This class is used to immediately copy data from AMPS Message objects
 * before they are reused by the AMPS client. AMPS reuses Message objects
 * as buffers, so data must be copied before queuing for later processing.
 */
public class AMPSMessageData {

    private final String data;
    /** The message's own bookmark. Null when the message has none. */
    private final String bookmark;
    /**
     * Bookmark written to the source offset. The message's own bookmark when it has one,
     * otherwise {@code newestBookmark}. Null when neither exists.
     */
    private final String offsetBookmark;
    private final SnapshotState snapshotState;

    /**
     * @param bookmark       the message's own bookmark. Empty is stored as null.
     * @param newestBookmark bookmark already seen. Used for the offset when {@code bookmark} is empty.
     * @param snapshotState  where the row sits relative to the sow_and_subscribe GroupEnd.
     *                       {@link SnapshotState#INCOMPLETE} marks a row emitted before GroupEnd,
     *                       so a restart cannot resume from it and skip the rows never delivered.
     */
    public AMPSMessageData(String data, String bookmark, String newestBookmark, SnapshotState snapshotState) {
        boolean ownBookmark = !isNullOrEmpty(bookmark);
        this.data = data;
        this.bookmark = ownBookmark ? bookmark : null;
        this.offsetBookmark = ownBookmark ? bookmark : isNullOrEmpty(newestBookmark) ? null : newestBookmark;
        this.snapshotState = snapshotState;
    }

    public String getData() {
        return data;
    }

    public String getBookmark() {
        return bookmark;
    }

    /** Bookmark written to the source offset. */
    public String getOffsetBookmark() {
        return offsetBookmark;
    }

    public SnapshotState getSnapshotState() {
        return snapshotState;
    }

    public boolean isSnapshotIncomplete() {
        return snapshotState == SnapshotState.INCOMPLETE;
    }

    /**
     * Returns the row as it looks after GroupEnd. The offset bookmark was chosen when the row was
     * created. The offset is {@link SnapshotState#FINISHED_WITHOUT_BOOKMARK} only when that bookmark
     * is missing, so it stays distinguishable from "no offset", which would request another SOW.
     */
    public AMPSMessageData asSnapshotComplete() {
        if (snapshotState != SnapshotState.INCOMPLETE) {
            return this;
        }
        if (!isNullOrEmpty(offsetBookmark)) {
            return new AMPSMessageData(data, bookmark, offsetBookmark, SnapshotState.NONE);
        }
        return new AMPSMessageData(data, null, null, SnapshotState.FINISHED_WITHOUT_BOOKMARK);
    }

    @Override
    public String toString() {
        return "AMPSMessageData{" +
                "bookmark='" + bookmark + '\'' +
                ", dataLength=" + (data != null ? data.length() : 0) +
                ", snapshotState=" + snapshotState +
                '}';
    }
}
