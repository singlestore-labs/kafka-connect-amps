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
     * Bookmark written to the source offset when this row has none of its own.
     * Null means the offset uses {@link #bookmark}.
     */
    private final String offsetBookmark;
    private final SnapshotState snapshotState;

    /**
     * Creates a new AMPSMessageData with copied values.
     *
     * @param data          The message payload data
     * @param bookmark      The AMPS bookmark for offset tracking
     */
    public AMPSMessageData(String data, String bookmark) {
        this(data, bookmark, null, SnapshotState.NONE);
    }

    /**
     * @param snapshotState where the row sits relative to the sow_and_subscribe GroupEnd.
     *                      {@link SnapshotState#INCOMPLETE} marks a row emitted before GroupEnd,
     *                      so a restart cannot resume from it and skip the rows never delivered.
     */
    public AMPSMessageData(String data, String bookmark, SnapshotState snapshotState) {
        this(data, bookmark, null, snapshotState);
    }

    /**
     * @param offsetBookmark bookmark stored in the source offset when {@code bookmark} is empty.
     *                       Null stores {@code bookmark} itself.
     */
    public AMPSMessageData(String data, String bookmark, String offsetBookmark, SnapshotState snapshotState) {
        this.data = data;
        this.bookmark = bookmark;
        this.offsetBookmark = offsetBookmark;
        this.snapshotState = snapshotState;
    }

    public String getData() {
        return data;
    }

    public String getBookmark() {
        return bookmark;
    }

    /**
     * Bookmark written to the source offset. A row with no bookmark of its own keeps the newest
     * bookmark already seen, so the stored offset is not an empty map.
     */
    public String getOffsetBookmark() {
        return offsetBookmark != null ? offsetBookmark : bookmark;
    }

    public SnapshotState getSnapshotState() {
        return snapshotState;
    }

    public boolean isSnapshotIncomplete() {
        return snapshotState == SnapshotState.INCOMPLETE;
    }

    /**
     * Returns the row as it looks after GroupEnd. A finished snapshot row with no bookmark of its
     * own keeps {@code newestBookmark} when the snapshot already had one. The offset is
     * {@link SnapshotState#FINISHED_WITHOUT_BOOKMARK} only when the snapshot contained none, so it
     * stays distinguishable from "no offset", which would request another snapshot.
     */
    public AMPSMessageData asSnapshotComplete() {
        return asSnapshotComplete(null);
    }

    public AMPSMessageData asSnapshotComplete(String newestBookmark) {
        if (snapshotState != SnapshotState.INCOMPLETE) {
            return this;
        }
        if (bookmark != null && !bookmark.isEmpty()) {
            return new AMPSMessageData(data, bookmark, null, SnapshotState.NONE);
        }
        if (newestBookmark != null && !newestBookmark.isEmpty()) {
            return new AMPSMessageData(data, null, newestBookmark, SnapshotState.NONE);
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
