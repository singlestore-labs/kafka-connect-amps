package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.exception.AMPSException;
import com.crankuptheamps.client.exception.InvalidTopicException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;

/**
 * The one {@code sow_and_subscribe} command for a task. AMPS delivers messages on its own
 * thread into {@link #buffer}. {@code poll()} drains that buffer and commits the batch.
 * <p>
 * {@link #phase} is the lifecycle. {@link #resumeBookmark} is the newest bookmark accepted
 * from the command still in effect. A replay {@code GroupBegin} after the snapshot has
 * finished freezes it. Bookmarks on the resent command are past the discarded snapshot and
 * must not move it.
 */
class StandingSubscription {

    private static final Logger log = LoggerFactory.getLogger(StandingSubscription.class);

    enum Phase {
        NEED_SNAPSHOT,
        SNAPSHOT,
        SNAPSHOT_COMMITTED,
        LIVE_ON_SOW,
        LIVE_ON_SOW_REPLAY,
        LIVE,
        FAILED
    }

    enum FailureReason {
        /** A row was already returned and the connection dropped, or the snapshot was resent. */
        INTERRUPTED,
        /** The snapshot had finished and the replay arrived with no bookmark to resume from. */
        UNRECOVERABLE
    }

    /**
     * Rows taken out of the buffer for one poll, plus the snapshot generation they belong to.
     * {@link #keep(PolledBatch)} returns false when a replacement snapshot discarded them.
     */
    static final class PolledBatch {
        final List<AMPSMessageData> rows;
        final int generation;

        PolledBatch(List<AMPSMessageData> rows, int generation) {
            this.rows = rows;
            this.generation = generation;
        }
    }

    private final String topic;
    private final String filter;
    private final int batchSize;
    private final long pollTimeoutMs;
    private final long pollIdleMs;
    private final int bufferLimit;
    private final Consumer<Message> discard;
    private final Consumer<String> logIncomplete;

    private final Object lock = new Object();
    private final Deque<AMPSMessageData> buffer = new ArrayDeque<>();

    private HAClient client;
    private volatile boolean running;
    private Phase phase = Phase.NEED_SNAPSHOT;
    private FailureReason failureReason;
    /** The current snapshot has already seen its first GroupBegin. */
    private boolean sawSnapshotGroupBegin;
    /** Incremented when an uncommitted snapshot is thrown away and replaced. */
    private int snapshotGeneration;
    private String resumeBookmark;
    private int handlerGeneration;
    private CommandId commandId;
    private long lastMessageAtMs;

    StandingSubscription(String topic, String filter, int batchSize, long pollTimeoutMs, long pollIdleMs,
                         Consumer<Message> discard, Consumer<String> logIncomplete) {
        this.topic = topic;
        this.filter = filter == null ? "" : filter;
        this.batchSize = batchSize;
        this.pollTimeoutMs = pollTimeoutMs;
        this.pollIdleMs = pollIdleMs;
        this.bufferLimit = batchSize * 2;
        this.discard = discard;
        this.logIncomplete = logIncomplete;
    }

    void setClient(HAClient client) {
        this.client = client;
    }

    void setRunning(boolean running) {
        this.running = running;
        wake();
    }

    void wake() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * A stored or configured bookmark. The phase stays {@link Phase#NEED_SNAPSHOT}.
     * {@link #open()} calls {@code bookmarkSubscribe} when {@link #resumeBookmark} is set,
     * and {@code sowAndSubscribe} when it is empty.
     */
    void resumeFrom(String bookmark) {
        synchronized (lock) {
            resumeBookmark = bookmark;
        }
    }

    boolean isSnapshotIncomplete() {
        synchronized (lock) {
            if (phase == Phase.SNAPSHOT || phase == Phase.SNAPSHOT_COMMITTED) {
                return true;
            }
            return phase == Phase.FAILED && failureReason == FailureReason.INTERRUPTED;
        }
    }

    void onDisconnected(int state) {
        if (state != ConnectionStateListener.Disconnected) {
            return;
        }
        synchronized (lock) {
            if (!running || phase != Phase.SNAPSHOT_COMMITTED) {
                return;
            }
            log.warn("AMPS connection lost during the sow_and_subscribe snapshot for topic '{}'", topic);
            fail(FailureReason.INTERRUPTED);
            lock.notifyAll();
        }
    }

    /**
     * Opens the standing command when there is not one already. A rejected bookmark subscription
     * fails the call. The plain {@code subscribe} fallback belongs to the short-lived path only.
     */
    void open() throws AMPSException {
        synchronized (lock) {
            if (commandId != null || !running || client == null) {
                return;
            }
        }
        final boolean resume;
        final String bookmark;
        synchronized (lock) {
            // resumeBookmark is also updated by snapshot rows, so this check has to happen
            // before the phase leaves NEED_SNAPSHOT. A failed snapshot clears it again below.
            resume = phase == Phase.NEED_SNAPSHOT && hasBookmark(resumeBookmark);
            bookmark = resumeBookmark;
            phase = resume ? Phase.LIVE : Phase.SNAPSHOT;
        }
        MessageHandler handler = createHandler();
        CommandId opened;
        try {
            opened = resume ? openBookmarkSubscribe(handler, bookmark) : openSowAndSubscribe(handler);
        } catch (AMPSException e) {
            synchronized (lock) {
                if (commandId == null) {
                    phase = Phase.NEED_SNAPSHOT;
                    if (!resume) {
                        resumeBookmark = null;
                    }
                    buffer.clear();
                    sawSnapshotGroupBegin = false;
                }
            }
            if (resume && e instanceof InvalidTopicException) {
                throw new ConnectException("AMPS topic '" + topic + "' rejected bookmarkSubscribe, so the "
                        + "transaction log cannot be resumed.", e);
            }
            throw e;
        }
        synchronized (lock) {
            commandId = opened;
            lock.notifyAll();
        }
        if (resume) {
            log.info("Opened standing bookmark subscription for topic '{}' from bookmark '{}'", topic, resumeBookmark);
        } else {
            log.info("Opened standing sow_and_subscribe subscription for topic '{}' at the current snapshot", topic);
        }
    }

    /**
     * Copies up to {@code batchSize} rows out of the buffer. While the snapshot is open the newest
     * row stays buffered so GroupEnd can finish its offset.
     */
    PolledBatch drain() throws InterruptedException {
        List<AMPSMessageData> batch = new ArrayList<>();
        long startMs = System.currentTimeMillis();
        int generation;
        synchronized (lock) {
            failIf();
            generation = snapshotGeneration;
            while (running && phase != Phase.FAILED && batch.size() < batchSize) {
                if (discardIfReplaced(batch, generation)) {
                    generation = snapshotGeneration;
                }
                drainBuffer(batch);
                lock.notifyAll();
                if (batch.size() >= batchSize) {
                    break;
                }
                long now = System.currentTimeMillis();
                long remainingMs = batch.isEmpty()
                        ? pollTimeoutMs - (now - startMs)
                        : pollIdleMs - (now - lastMessageAtMs);
                if (remainingMs <= 0) {
                    break;
                }
                try {
                    lock.wait(remainingMs);
                } catch (InterruptedException e) {
                    throw e;
                }
            }
            if (discardIfReplaced(batch, generation)) {
                generation = snapshotGeneration;
            }
            if (phase != Phase.FAILED) {
                drainBuffer(batch);
            }
        }
        return new PolledBatch(batch, generation);
    }

    /**
     * @return false when a replacement snapshot discarded this batch. The rows were not returned,
     *         so the phase stays {@link Phase#SNAPSHOT} for the replay.
     */
    boolean keep(PolledBatch batch) {
        synchronized (lock) {
            if (batch.generation != snapshotGeneration) {
                return false;
            }
            if (phase == Phase.SNAPSHOT && hasIncomplete(batch.rows)) {
                phase = Phase.SNAPSHOT_COMMITTED;
            }
            return true;
        }
    }

    void restore(PolledBatch batch) {
        synchronized (lock) {
            if (batch.generation == snapshotGeneration) {
                for (int i = batch.rows.size() - 1; i >= 0; i--) {
                    buffer.addFirst(batch.rows.get(i));
                }
            }
            lock.notifyAll();
        }
    }

    void failIf() {
        Phase failedPhase;
        FailureReason reason;
        synchronized (lock) {
            failedPhase = phase;
            reason = failureReason;
        }
        if (failedPhase != Phase.FAILED) {
            return;
        }
        if (reason == FailureReason.INTERRUPTED) {
            logIncomplete.accept("connection lost before GroupEnd");
            throw new ConnectException("The AMPS connection was lost before the sow_and_subscribe snapshot "
                    + "for topic '" + topic + "' finished. The snapshot is incomplete; truncate the "
                    + "target table before starting the pipeline again.");
        }
        log.error("AMPS resent sow_and_subscribe for topic '{}' after the snapshot finished, and no "
                + "bookmark is available to resume the transaction log. Updates published while "
                + "disconnected cannot be recovered.", topic);
        throw new ConnectException("AMPS reconnected after the sow_and_subscribe snapshot for topic '"
                + topic + "' finished, but no bookmark was received to resume from. Updates "
                + "published while disconnected may be missing; truncate the target table before "
                + "starting the pipeline again.");
    }

    /**
     * Opens {@code bookmarkSubscribe} from {@link #resumeBookmark}, then unsubscribes the snapshot
     * command. The new command is published only after both calls succeed. A failure leaves the
     * snapshot command id in place and fails this poll.
     */
    void promote() {
        CommandId previous;
        String bookmark;
        synchronized (lock) {
            if ((phase != Phase.LIVE_ON_SOW && phase != Phase.LIVE_ON_SOW_REPLAY) || !running || client == null) {
                return;
            }
            if (!hasBookmark(resumeBookmark)) {
                return;
            }
            previous = commandId;
            bookmark = resumeBookmark;
            // Wake a handler parked for buffer space before bookmarkSubscribe waits for its ack.
            handlerGeneration++;
            lock.notifyAll();
        }

        MessageHandler handler = createHandler();
        CommandId opened;
        try {
            opened = openBookmarkSubscribe(handler, bookmark);
        } catch (AMPSException e) {
            throw new ConnectException("Bookmark resume after sow_and_subscribe failed for topic '" + topic + "'", e);
        }
        if (previous != null) {
            try {
                client.unsubscribe(previous);
            } catch (Exception e) {
                throw new ConnectException("Unsubscribing sow_and_subscribe failed for topic '" + topic
                        + "' after the bookmark subscription was opened. The snapshot command may still be "
                        + "registered.", e);
            }
        }
        synchronized (lock) {
            if (running) {
                commandId = opened;
                phase = Phase.LIVE;
                lock.notifyAll();
            }
        }
        log.info("Replaced sow_and_subscribe with a bookmark subscription for topic '{}' from bookmark '{}' "
                + "so a reconnect does not repeat the snapshot", topic, bookmark);
    }

    /**
     * Drops the command and the buffer. Safe before the client exists.
     */
    void reset() {
        CommandId toUnsub;
        HAClient current;
        synchronized (lock) {
            toUnsub = commandId;
            commandId = null;
            current = client;
            phase = Phase.NEED_SNAPSHOT;
            failureReason = null;
            sawSnapshotGroupBegin = false;
            snapshotGeneration++;
            resumeBookmark = null;
            handlerGeneration++;
            buffer.clear();
            lastMessageAtMs = 0L;
            lock.notifyAll();
        }
        if (toUnsub != null && current != null) {
            try {
                current.unsubscribe(toUnsub);
                log.info("Unsubscribed standing subscription for topic '{}'", topic);
            } catch (Exception e) {
                log.warn("Error unsubscribing standing subscription for topic '{}': {}", topic, e.getMessage());
            }
        }
    }

    private void fail(FailureReason reason) {
        phase = Phase.FAILED;
        failureReason = reason;
    }

    private boolean discardIfReplaced(List<AMPSMessageData> batch, int generation) {
        if (snapshotGeneration == generation) {
            return false;
        }
        batch.clear();
        return true;
    }

    private void drainBuffer(List<AMPSMessageData> batch) {
        int holdBack = (phase == Phase.SNAPSHOT || phase == Phase.SNAPSHOT_COMMITTED) ? 1 : 0;
        while (buffer.size() > holdBack && batch.size() < batchSize) {
            batch.add(buffer.removeFirst());
        }
    }

    private void onGroupBegin() {
        if (phase == Phase.LIVE_ON_SOW || phase == Phase.LIVE || phase == Phase.LIVE_ON_SOW_REPLAY) {
            if (phase == Phase.LIVE_ON_SOW_REPLAY) {
                return;
            }
            if (!hasBookmark(resumeBookmark)) {
                fail(FailureReason.UNRECOVERABLE);
            } else {
                // Freeze resumeBookmark. Later bookmarks on this command are past the discarded snapshot.
                phase = Phase.LIVE_ON_SOW_REPLAY;
            }
            return;
        }
        if (phase != Phase.SNAPSHOT && phase != Phase.SNAPSHOT_COMMITTED) {
            return;
        }
        if (!sawSnapshotGroupBegin) {
            sawSnapshotGroupBegin = true;
            return;
        }
        if (phase == Phase.SNAPSHOT_COMMITTED) {
            fail(FailureReason.INTERRUPTED);
            return;
        }
        buffer.clear();
        resumeBookmark = null;
        snapshotGeneration++;
    }

    private void onGroupEnd() {
        if (phase == Phase.LIVE_ON_SOW_REPLAY || phase == Phase.FAILED) {
            return;
        }
        if (phase != Phase.SNAPSHOT && phase != Phase.SNAPSHOT_COMMITTED) {
            return;
        }
        AMPSMessageData last = buffer.peekLast();
        if (last != null && last.isSnapshotIncomplete()) {
            buffer.pollLast();
            buffer.addLast(last.asSnapshotComplete(resumeBookmark));
        }
        phase = Phase.LIVE_ON_SOW;
    }

    private void accept(Message message) {
        String bookmark = message.getBookmark();
        boolean ownBookmark = hasBookmark(bookmark);
        if (ownBookmark) {
            resumeBookmark = bookmark;
        }
        boolean snapshotOpen = phase == Phase.SNAPSHOT || phase == Phase.SNAPSHOT_COMMITTED;
        String offsetBookmark = ownBookmark ? null : (hasBookmark(resumeBookmark) ? resumeBookmark : null);
        SnapshotState state;
        if (snapshotOpen) {
            state = SnapshotState.INCOMPLETE;
        } else if (!ownBookmark && !hasBookmark(resumeBookmark)) {
            state = SnapshotState.FINISHED_WITHOUT_BOOKMARK;
        } else {
            state = SnapshotState.NONE;
        }
        buffer.addLast(new AMPSMessageData(
                message.getData(),
                ownBookmark ? bookmark : null,
                offsetBookmark,
                state));
        lastMessageAtMs = System.currentTimeMillis();
    }

    private MessageHandler createHandler() {
        final int generation = handlerGeneration;
        return message -> {
            boolean discardMessage = false;
            synchronized (lock) {
                if (!running || generation != handlerGeneration || phase == Phase.FAILED) {
                    discardMessage = true;
                } else if (message.getCommand() == Message.Command.GroupBegin) {
                    onGroupBegin();
                    lock.notifyAll();
                } else if (message.getCommand() == Message.Command.GroupEnd) {
                    onGroupEnd();
                    lock.notifyAll();
                } else if (phase == Phase.LIVE_ON_SOW_REPLAY) {
                    discardMessage = true;
                } else {
                    try {
                        while (running && generation == handlerGeneration && phase != Phase.FAILED
                                && buffer.size() >= bufferLimit) {
                            lock.wait();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!running || generation != handlerGeneration || phase == Phase.FAILED) {
                        discardMessage = true;
                    } else if (phase == Phase.LIVE_ON_SOW_REPLAY) {
                        discardMessage = true;
                    } else {
                        accept(message);
                        lock.notifyAll();
                        discardMessage = true;
                    }
                }
            }
            if (discardMessage) {
                discard.accept(message);
            }
        };
    }

    private CommandId openSowAndSubscribe(MessageHandler handler) throws AMPSException {
        // (handler, topic, filter, orderBy, bookmark, batchSize, topN, options, timeout).
        // NOW is the current snapshot. EPOCH ("0") is a historical SOW at the start of the log.
        // The bookmark option, not the NOW header, puts bookmarks on publishes.
        return client.sowAndSubscribe(handler, topic, filter, null,
                Client.Bookmarks.NOW, 0, -1, "bookmark", 0L);
    }

    private CommandId openBookmarkSubscribe(MessageHandler handler, String bookmark) throws AMPSException {
        return bookmarkSubscribe(client, handler, topic, filter, bookmark, false);
    }

    /**
     * @param allowPlainSubscribeFallback true only for the short-lived {@code subscribe} path,
     *                                     where a topic may have no transaction log
     */
    static CommandId bookmarkSubscribe(HAClient client, MessageHandler handler, String topic, String filter,
                                       String bookmark, boolean allowPlainSubscribeFallback) throws AMPSException {
        try {
            return client.bookmarkSubscribe(handler, topic, filter, null, bookmark, "", 0L);
        } catch (InvalidTopicException e) {
            if (!allowPlainSubscribeFallback) {
                throw e;
            }
            log.warn("Bookmark subscription failed, falling back to regular subscribe: {}", e.getMessage());
            if (filter == null || filter.isEmpty()) {
                return client.subscribe(handler, topic, 0L);
            }
            return client.subscribe(handler, topic, filter, 0L);
        }
    }

    private static boolean hasIncomplete(List<AMPSMessageData> rows) {
        for (AMPSMessageData row : rows) {
            if (row.isSnapshotIncomplete()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasBookmark(String bookmark) {
        return bookmark != null && !bookmark.isEmpty();
    }
}
