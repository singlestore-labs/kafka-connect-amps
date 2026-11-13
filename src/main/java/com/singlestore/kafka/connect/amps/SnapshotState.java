package com.singlestore.kafka.connect.amps;

/**
 * Where a {@code sow_and_subscribe} message sits relative to the snapshot's GroupEnd.
 * The offset writer maps each state to one stored value, so a message can never carry
 * two conflicting markers.
 */
public enum SnapshotState {
    /** Not a snapshot row, or a finished snapshot row that has a bookmark to resume from. */
    NONE,
    /** A snapshot row emitted before GroupEnd. The rows after it were never delivered. */
    INCOMPLETE,
    /** A finished snapshot whose last row has no bookmark, so the log cannot be resumed. */
    FINISHED_WITHOUT_BOOKMARK
}
