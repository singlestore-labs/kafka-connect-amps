package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.exception.AMPSException;
import com.crankuptheamps.client.exception.InvalidTopicException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AMPS Source Task.
 *
 * {@code subscribe} subscribes on each poll, collects up to batch.size messages, then
 * unsubscribes. {@code sow_and_subscribe} keeps one subscription open and drains it.
 * {@code sow} streams one snapshot across polls.
 */
public class AMPSSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(AMPSSourceTask.class);

    private AMPSSourceConnectorConfig config;
    private String ampsTopic;

    private HAClient ampsClient;
    private RecordConverter recordConverter;

    private volatile boolean running;
    
    // In-memory bookmark tracking (updated after each successful poll)
    private volatile String currentBookmark;

    private final Object pollWaitLock = new Object();
    private final Object sowStateLock = new Object();
    private final Deque<AMPSMessageData> sowBuffer = new ArrayDeque<>();
    private volatile int sowRecordsAccepted;
    private volatile int sowRecordsIgnored;
    private volatile CommandId currentSowCommandId;
    private volatile SowState sowState;

    /**
     * sow_and_subscribe keeps one command open for the life of the task.
     * A stored bookmark or {@code amps.start.bookmark} resumes with bookmarkSubscribe
     * so a later task start does not request another State of the World.
     */
    private volatile boolean resumeWithoutSow;
    private volatile boolean sowAndSubscribeSnapshotOpen;
    /**
     * True while the command registered with HAClient is still {@code sow_and_subscribe}.
     * HAClient resubscribes that command after a disconnect, which delivers another SOW.
     */
    private volatile boolean standingCommandIsSow;
    private volatile boolean sawStandingGroupBegin;
    private volatile boolean initialSnapshotFinished;
    private volatile boolean suppressSowReplay;
    /**
     * Bookmark already queued, drained into the current poll, or emitted when a finished
     * snapshot is replayed. The resent command's bookmarks are past the discarded SOW, so
     * promotion must resume from this one or the gap updates are skipped.
     */
    private volatile String replayRecoveryBookmark;
    /**
     * Newest bookmark removed from {@code standingBuffer} into the current poll and not yet
     * copied to {@code currentBookmark}. A replay can start in that window. The recovery
     * point has to include it, or bookmarkSubscribe delivers those records again.
     */
    private String drainedOriginalBookmark;
    /**
     * True once {@code poll()} has returned at least one open-snapshot row.
     * Rows still in {@code standingBuffer}, or only in the current poll's batch, are not
     * delivered. A disconnect before the return can still accept a replacement snapshot.
     */
    private volatile boolean standingSnapshotRowsReceived;
    /**
     * Incomplete snapshot rows moved into the current poll's batch and not yet returned.
     * {@link #onConnectionStateChanged} does not treat these as delivered.
     */
    private boolean inFlightSnapshotRows;
    /** Bumped when a replay replaces a snapshot {@code poll()} has not returned. */
    private int snapshotAttempt;
    /**
     * The connection dropped, or sow_and_subscribe was replayed, before the initial snapshot
     * reached GroupEnd. Rows already delivered cannot be told apart from the replay, and the
     * keys that had not arrived yet are only in the replay. poll() fails instead of continuing
     * on a partial snapshot.
     */
    private volatile boolean standingSnapshotInterrupted;
    /**
     * sow_and_subscribe was replayed after the initial snapshot finished, but no bookmark was
     * seen to resume the transaction log from. Suppressing the replay would lose the updates
     * published while disconnected, and emitting it would duplicate the table, so poll() fails.
     */
    private volatile boolean standingReplayUnrecoverable;
    private volatile int standingHandlerGeneration;
    private volatile CommandId standingCommandId;
    private final Deque<AMPSMessageData> standingBuffer = new ArrayDeque<>();
    private final Object standingLock = new Object();
    private final AtomicBoolean incompleteSnapshotLogged = new AtomicBoolean();
    private volatile long standingLastMessageAtMs;

    /**
     * Prefetch cap for the standing SOW query. AMPS delivers the snapshot on a
     * background reader faster than poll() can return {@code batch.size} records;
     * without a bound, {@code sowBuffer} holds the rest of the topic in heap.
     */
    private int sowBufferLimit;

    private enum SowState {
        NONE,
        STARTED,
        COMPLETED,
        CLIENT_RELEASED,
        EOF_RETURNED
    }

    AMPSClientFactory clientFactory = new AMPSClientFactoryImpl();

    /**
     * Runs after a standing poll drains and releases {@code standingLock}, before the
     * recovery bookmark is read. Tests deliver a replay in that window.
     */
    Runnable afterStandingDrain;

    private void runAfterStandingDrain() {
        Runnable hook = afterStandingDrain;
        if (hook != null) {
            hook.run();
        }
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting AMPS Source Task");

        this.config = new AMPSSourceConnectorConfig(props);
        this.ampsTopic = config.ampsTopic;
        this.recordConverter = new RecordConverter(config);
        this.running = true;
        this.sowBufferLimit = config.batchSize * 2;

        resetSowSnapshotLifecycleState("task start");
        resetStandingSubscriptionState("task start");

        log.info("Task configuration:");
        log.info("  AMPS Topic: {}", ampsTopic);
        log.info("  Kafka Topic: {}", config.getKafkaTopic());
        log.info("  Batch Size: {}", config.batchSize);
        log.info("  Poll Timeout: {} ms", config.pollTimeoutMs);
        log.info("  Poll Idle: {} ms", config.pollIdleMs);
        log.info("  Subscription Type: {}", config.ampsSubscriptionType.configValue());
        if (config.ampsSubscriptionType == SubscriptionType.SOW) {
            log.info("  SOW Query Batch Size: {}", config.getEffectiveSowQueryBatchSize());
        }

        try {
            initializeClient();
            if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE) {
                ampsClient.addConnectionStateListener(this::onConnectionStateChanged);
            }
            initializeBookmark();
            if (config.ampsSubscriptionType == SubscriptionType.SOW) {
                log.info("AMPS Source Task started successfully for topic '{}' in full-SOW snapshot mode", ampsTopic);
            } else {
                log.info("AMPS Source Task started successfully for topic '{}', starting bookmark: '{}'",
                        ampsTopic, currentBookmark);
            }
        } catch (Exception e) {
            log.error("Failed to start AMPS Source Task for topic '{}': {}", ampsTopic, e.getMessage(), e);
            // Logon already succeeded. Leaving the client open holds the AMPS client name, so the
            // worker's next start fails with name-in-use and this error stays hidden until exit.
            closeClientAfterFailedStart();
            throw new ConnectException("Failed to start AMPS Source Task", e);
        }
    }

    /**
     * Closes the client when {@link #start(Map)} fails after {@link #initializeClient()}.
     * A stored incomplete {@code sow_and_subscribe} offset is refused only after logon.
     * Closing here frees the client name so the next start can log on and report that error.
     * A close failure is logged and does not replace the original start exception.
     */
    private void closeClientAfterFailedStart() {
        HAClient toClose = ampsClient;
        ampsClient = null;
        if (toClose == null) {
            return;
        }
        try {
            toClose.close();
            log.info("AMPS client closed for topic '{}' after start failed; client name released", ampsTopic);
        } catch (Exception closeError) {
            log.warn("Error closing AMPS client for topic '{}' after start failed: {}",
                    ampsTopic, closeError.getMessage());
        }
    }

    /**
     * Clears SOW streaming state and cancels any in-flight SOW command. Invoked from
     * {@link #start(Map)} on every task start, from {@link #stop()} after closing the client,
     * and when a completed snapshot is re-armed after its EOF poll.
     */
    private void resetSowSnapshotLifecycleState(String reason) {
        if (config == null || config.ampsSubscriptionType != SubscriptionType.SOW) {
            return;
        }

        CommandId toUnsub;
        synchronized (sowStateLock) {
            toUnsub = currentSowCommandId;
            currentSowCommandId = null;
            sowState = SowState.NONE;
            sowRecordsAccepted = 0;
            sowRecordsIgnored = 0;
            sowBuffer.clear();
            sowStateLock.notifyAll();
        }

        if (toUnsub != null && ampsClient != null) {
            try {
                ampsClient.unsubscribe(toUnsub);
                log.info("Unsubscribed active SOW query for topic '{}' ({})", ampsTopic, reason);
            } catch (Exception e) {
                log.warn("Error unsubscribing SOW query for topic '{}' ({}): {}", ampsTopic, reason, e.getMessage());
            }
        }
    }

    /**
     * Drops standing sow_and_subscribe state. {@link #stop()} nulls the command id
     * only as part of this reset; the buffer, snapshot flag, idle timestamp, and
     * one-shot incomplete-snapshot log must not survive into the next {@link #start(Map)}.
     */
    private void resetStandingSubscriptionState(String reason) {
        CommandId toUnsub;
        synchronized (standingLock) {
            toUnsub = standingCommandId;
            standingCommandId = null;
            standingCommandIsSow = false;
            sawStandingGroupBegin = false;
            initialSnapshotFinished = false;
            suppressSowReplay = false;
            replayRecoveryBookmark = null;
            drainedOriginalBookmark = null;
            standingSnapshotRowsReceived = false;
            inFlightSnapshotRows = false;
            snapshotAttempt = 0;
            standingSnapshotInterrupted = false;
            standingReplayUnrecoverable = false;
            standingHandlerGeneration++;
            standingBuffer.clear();
            sowAndSubscribeSnapshotOpen = false;
            standingLastMessageAtMs = 0L;
            incompleteSnapshotLogged.set(false);
            standingLock.notifyAll();
        }

        if (toUnsub != null && ampsClient != null) {
            try {
                ampsClient.unsubscribe(toUnsub);
                log.info("Unsubscribed standing subscription for topic '{}' ({})", ampsTopic, reason);
            } catch (Exception e) {
                log.warn("Error unsubscribing standing subscription for topic '{}' ({}): {}",
                        ampsTopic, reason, e.getMessage());
            }
        }
    }

    /**
     * Initialize AMPS client connection (no subscription yet).
     */
    private void initializeClient() {
        ampsClient = clientFactory.create(config);
        log.info("AMPS client connected for topic '{}'", ampsTopic);
    }

    /**
     * Initialize the starting bookmark from config or stored offset.
     */
    private void initializeBookmark() {
        if (config.ampsSubscriptionType == SubscriptionType.SOW) {
            currentBookmark = null;
            log.info("SOW mode ignores bookmark-based offsets and reads the current AMPS snapshot from the beginning.");
            return;
        }

        // Check if a specific start bookmark is configured (overrides everything)
        if (config.ampsStartBookmark != null && !config.ampsStartBookmark.isEmpty()) {
            currentBookmark = config.ampsStartBookmark;
            resumeWithoutSow = config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE;
            log.info("Using configured start bookmark for topic '{}': bookmark='{}'", ampsTopic, currentBookmark);
            return;
        }

        // Check for stored offset
        Map<String, Object> sourcePartition = Collections.singletonMap(RecordConverter.FIELD_TOPIC, ampsTopic);
        Map<String, Object> lastOffset = context.offsetStorageReader().offset(sourcePartition);

        if (lastOffset != null && !lastOffset.isEmpty()) {
            if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE
                    && lastOffset.containsKey(RecordConverter.FIELD_SNAPSHOT_STATE)) {
                String recovery = " Truncate the target table and reset the stored offsets to request a "
                        + "new snapshot. Setting " + AMPSSourceConnectorConfig.AMPS_START_BOOKMARK_CONF
                        + " resumes from that bookmark and does not recover keys that were not delivered.";
                if (RecordConverter.SNAPSHOT_STATE_FINISHED_WITHOUT_BOOKMARK.equals(
                        lastOffset.get(RecordConverter.FIELD_SNAPSHOT_STATE))) {
                    throw new ConnectException("The stored offset for topic '" + ampsTopic + "' is a finished "
                            + "sow_and_subscribe snapshot with no bookmark, so the transaction log cannot be "
                            + "resumed." + recovery);
                }
                throw new ConnectException("The stored offset for topic '" + ampsTopic + "' was written "
                        + "before the sow_and_subscribe snapshot finished, so the target table is missing "
                        + "snapshot rows." + recovery);
            }
            currentBookmark = (String) lastOffset.get(RecordConverter.FIELD_BOOKMARK);
            resumeWithoutSow = config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE
                    && currentBookmark != null && !currentBookmark.isEmpty();
            log.info("Found stored offset for topic '{}': bookmark='{}'", ampsTopic, currentBookmark);
        } else {
            resumeWithoutSow = false;
            if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE) {
                // No recovery bookmark. Leave the cursor empty so the first command
                // is a current SOW. amps.position does not apply here: EPOCH on
                // sow_and_subscribe is a historical SOW point.
                currentBookmark = null;
                log.info("No stored offset for topic '{}'. Starting sow_and_subscribe at the current snapshot",
                        ampsTopic);
            } else if (config.ampsPosition == Position.EPOCH) {
                currentBookmark = Client.Bookmarks.EPOCH;
                log.info("No stored offset for topic '{}'. Starting from EPOCH (beginning of transaction log)", ampsTopic);
            } else {
                currentBookmark = Client.Bookmarks.NOW;
                log.info("No stored offset for topic '{}'. Starting from NOW (latest messages only)", ampsTopic);
            }
        }
    }

    /**
     * Subscribe-on-demand poll: subscribe, collect up to batch.size messages, unsubscribe.
     * Returns when the batch is full, when AMPS has been quiet for poll.idle.ms after at
     * least one record, or when poll.timeout.ms elapses with no records.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!running) {
            return Collections.emptyList();
        }

        if (config.ampsSubscriptionType == SubscriptionType.SOW) {
            try {
                return pollFullSowSnapshot();
            } catch (InterruptedException e) {
                logIncompleteSnapshot("poll interrupted");
                throw e;
            } catch (RuntimeException e) {
                logIncompleteSnapshot("poll failed");
                throw e;
            }
        }

        if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE) {
            return pollStandingSubscription();
        }

        return pollShortLivedSubscription();
    }

    private List<SourceRecord> pollShortLivedSubscription() throws InterruptedException {
        List<AMPSMessageData> batch = new ArrayList<>();
        AtomicReference<String> lastBookmarkRef = new AtomicReference<>(currentBookmark);
        AtomicLong lastMessageAtMs = new AtomicLong(0L);

        MessageHandler handler = new MessageHandler() {
            @Override
            public void invoke(Message message) {
                synchronized (pollWaitLock) {
                    if (batch.size() < config.batchSize) {
                        // Copy data immediately (AMPS reuses Message objects)
                        AMPSMessageData data = copyMessage(message);
                        batch.add(data);
                        String bookmark = data.getBookmark();
                        if (bookmark != null) {
                            lastBookmarkRef.set(bookmark);
                        }
                        lastMessageAtMs.set(System.currentTimeMillis());

                        log.trace("Collected message {} of {} from topic '{}'",
                                batch.size(), config.batchSize, ampsTopic);
                    }
                    // The client logs every delivered message into the bookmark store, including
                    // ones past batch.size. Nothing replays from it, so discard immediately or the
                    // store grows for the life of the task.
                    discardFromBookmarkStore(message);
                    pollWaitLock.notifyAll();
                }
            }
        };

        CommandId subId = null;
        try {
            subId = subscribeFromBookmark(currentBookmark, handler);
            waitForShortLivedBatch(batch, lastMessageAtMs);

            if (batch.size() >= config.batchSize) {
                log.debug("Batch complete: collected {} messages from topic '{}'", batch.size(), ampsTopic);
            } else if (!batch.isEmpty()) {
                log.debug("Idle quiet: collected {} messages from topic '{}' (batch.size={})",
                        batch.size(), ampsTopic, config.batchSize);
            } else {
                log.debug("Timeout reached: collected 0 messages from topic '{}' (batch.size={})",
                        ampsTopic, config.batchSize);
            }

        } catch (AMPSException e) {
            log.error("Error subscribing to AMPS topic '{}': {}", ampsTopic, e.getMessage(), e);
            throw new RetriableException("Error subscribing to AMPS topic '" + ampsTopic + "'", e);
        } finally {
            // Always unsubscribe after collecting batch
            if (subId != null) {
                try {
                    ampsClient.unsubscribe(subId);
                    log.trace("Unsubscribed from AMPS topic '{}' after poll", ampsTopic);
                } catch (Exception e) {
                    log.warn("Error unsubscribing from AMPS topic '{}': {}", ampsTopic, e.getMessage());
                }
            }
            // Every poll subscribes under a new subscription id, so without this the store keeps
            // one entry per poll for the life of the task.
            purgeBookmarkStore();
        }

        List<SourceRecord> records = convertBatch(batch);
        if (!records.isEmpty()) {
            updateCurrentBookmark(lastBookmarkRef.get());
            log.debug("Polled {} records from AMPS topic '{}'", records.size(), ampsTopic);
        }

        return records;
    }

    /**
     * Releases a delivered message from the bookmark store. Bookmark subscriptions log every
     * message they deliver; this connector tracks its own bookmark and never replays from the
     * store, so an undiscarded message is a permanent allocation.
     */
    private void discardFromBookmarkStore(Message message) {
        HAClient client = ampsClient;
        if (client == null) {
            return;
        }
        try {
            BookmarkStore store = client.getBookmarkStore();
            if (store != null) {
                store.discard(message);
            }
        } catch (Exception e) {
            // Logged at trace: this runs per message, and a failure here is recovered by the
            // purge that follows every poll.
            log.trace("Could not discard bookmark for topic '{}': {}", ampsTopic, e.getMessage());
        }
    }

    /**
     * Drops all bookmark-store subscription state after a poll's subscription is closed. Safe
     * because the next poll re-subscribes from {@link #currentBookmark}, which the task owns.
     */
    private void purgeBookmarkStore() {
        HAClient client = ampsClient;
        if (client == null) {
            return;
        }
        try {
            BookmarkStore store = client.getBookmarkStore();
            if (store != null) {
                store.purge();
            }
        } catch (Exception e) {
            log.warn("Error purging AMPS bookmark store for topic '{}': {}", ampsTopic, e.getMessage());
        }
    }

    /**
     * Wait until the batch is full, AMPS has been quiet for poll.idle.ms after at least
     * one record, poll.timeout.ms elapses with no records, or the task is stopped.
     */
    private void waitForShortLivedBatch(List<AMPSMessageData> batch, AtomicLong lastMessageAtMs)
            throws InterruptedException {
        long startMs = System.currentTimeMillis();
        synchronized (pollWaitLock) {
            while (running && batch.size() < config.batchSize) {
                long now = System.currentTimeMillis();
                long remainingTimeout = batch.isEmpty() ?
                        config.pollTimeoutMs - (now - startMs) :
                        config.pollIdleMs - (now - lastMessageAtMs.get());

                if (remainingTimeout <= 0) {
                    break;
                }
                pollWaitLock.wait(remainingTimeout);
            }
        }
    }

    private List<SourceRecord> pollFullSowSnapshot() throws InterruptedException {
        if (sowState == SowState.CLIENT_RELEASED)
        {
            sowState = SowState.EOF_RETURNED;
            log.info("SOW snapshot EOF for topic '{}'. The next poll starts a new snapshot.", ampsTopic);
            return Collections.emptyList();
        }

        if (sowState == SowState.EOF_RETURNED) {
            rearmSowSnapshotAfterEof();
        }

        List<AMPSMessageData> batch = new ArrayList<>(config.batchSize);

        try {
            startSowQueryIfNeeded();
        } catch (AMPSException e) {
            log.error("Error starting SOW query for topic '{}': {}", ampsTopic, e.getMessage(), e);
            throw new RetriableException("Error starting SOW query for topic '" + ampsTopic + "'", e);
        }

        long deadlineMs = System.currentTimeMillis() + config.pollTimeoutMs;
        synchronized (sowStateLock) {
            while (running && batch.size() < config.batchSize) {
                drainSowBuffer(batch);
                sowStateLock.notifyAll();

                if (batch.size() >= config.batchSize) {
                    break;
                }

                if (sowState == SowState.COMPLETED && sowBuffer.isEmpty()) {
                    break;
                }

                long remainingMs = deadlineMs - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    break;
                }

                sowStateLock.wait(remainingMs);
            }
        }

        List<SourceRecord> records;
        try {
            records = convertBatch(batch);
        } catch (Exception e) {
            synchronized (sowStateLock) {
                for (int i = batch.size() - 1; i >= 0; i--) {
                    sowBuffer.addFirst(batch.get(i));
                }
                sowStateLock.notifyAll();
            }
            throw e;
        }
        if (!records.isEmpty()) {
            updateCurrentBookmark(batch.get(batch.size() - 1).getBookmark());
            log.info("SOW poll returned {} record(s) from topic '{}' (snapshot state={}, buffered={}, accepted={}, ignored={})",
                    records.size(), ampsTopic, sowState, sowBuffer.size(), sowRecordsAccepted, sowRecordsIgnored);
        } else if (sowState == SowState.COMPLETED) {
            log.info("SOW snapshot complete for topic '{}' (accepted={}, ignored={})",
                    ampsTopic, sowRecordsAccepted, sowRecordsIgnored);
        }

        boolean snapshotFullyDrained;
        synchronized (sowStateLock) {
            snapshotFullyDrained = sowState == SowState.COMPLETED && sowBuffer.isEmpty();
        }
        if (snapshotFullyDrained) {
            releaseClientAfterSnapshot();
            if (records.isEmpty()) {
                // This poll is already the empty batch FOREGROUND stops on.
                sowState = SowState.EOF_RETURNED;
                log.info("SOW snapshot EOF for topic '{}' (no rows in the completing poll). "
                        + "The next poll starts a new snapshot.", ampsTopic);
            }
        }

        return records;
    }

    /**
     * Reconnects and clears snapshot state so the caller can issue another {@code sow()}.
     * Connect runs before the reset. The factory reports a failed logon as {@link ConnectException};
     * that is retried so a later poll can rearm, and the snapshot stays {@link SowState#EOF_RETURNED}.
     */
    private void rearmSowSnapshotAfterEof() {
        log.info("SOW EOF already delivered for topic '{}'. Starting a new snapshot.", ampsTopic);
        try {
            initializeClient();
        } catch (ConnectException e) {
            log.error("Failed to reconnect AMPS client for a new SOW snapshot on topic '{}': {}",
                    ampsTopic, e.getMessage(), e);
            throw new RetriableException(
                    "Failed to reconnect AMPS client for a new SOW snapshot on topic '" + ampsTopic + "'", e);
        }
        resetSowSnapshotLifecycleState("re-arm after SOW EOF");
        this.recordConverter = new RecordConverter(config);
    }

    /**
     * Closes the AMPS client once the snapshot has been fully delivered. Holding the
     * connection open keeps the client name in use and blocks the next logon. The following
     * poll returns empty so a FOREGROUND extract can stop; the poll after that starts another snapshot.
     */
    private void releaseClientAfterSnapshot() {
        HAClient toClose;
        synchronized (sowStateLock) {
            if (sowState != SowState.COMPLETED || ampsClient == null) {
                return;
            }
            sowState = SowState.CLIENT_RELEASED;
            currentSowCommandId = null;
            toClose = ampsClient;
            ampsClient = null;
            sowStateLock.notifyAll();
        }

        try {
            toClose.close();
            log.info("SOW snapshot delivered for topic '{}' (accepted={}, ignored={}); AMPS client closed and "
                            + "client name released. The next poll returns empty; the poll after that starts another snapshot.",
                    ampsTopic, sowRecordsAccepted, sowRecordsIgnored);
        } catch (Exception e) {
            log.warn("Error closing AMPS client after SOW snapshot for topic '{}': {}", ampsTopic, e.getMessage(), e);
        }
    }

    private void startSowQueryIfNeeded() throws AMPSException {
        HAClient client;
        synchronized (sowStateLock) {
            if (sowState != SowState.NONE || !running || ampsClient == null) {
                return;
            }
            client = ampsClient;
            sowState = SowState.STARTED;
            sowRecordsAccepted = 0;
            sowRecordsIgnored = 0;
            currentSowCommandId = null;
            sowBuffer.clear();
        }

        boolean started = false;
        try {
            CommandId sowId = client.sow(createSowMessageHandler(),
                    ampsTopic,
                    getFilterForUse(),
                    config.getEffectiveSowQueryBatchSize(),
                    0L);
            synchronized (sowStateLock) {
                currentSowCommandId = sowId;
                sowStateLock.notifyAll();
            }
            started = true;
            log.info("Started streaming SOW query for topic '{}' with AMPS batch size {}",
                    ampsTopic, config.getEffectiveSowQueryBatchSize());
        } finally {
            if (!started) {
                synchronized (sowStateLock) {
                    sowState = SowState.NONE;
                    sowRecordsAccepted = 0;
                    sowRecordsIgnored = 0;
                    currentSowCommandId = null;
                    sowBuffer.clear();
                    sowStateLock.notifyAll();
                }
            }
        }
    }

    private MessageHandler createSowMessageHandler() {
        return new MessageHandler() {
            @Override
            public void invoke(Message message) {
                int command = message.getCommand();
                synchronized (sowStateLock) {
                    if (!running) {
                        return;
                    }

                    if (command == Message.Command.GroupEnd) {
                        if (sowState == SowState.STARTED) {
                            sowState = SowState.COMPLETED;
                            currentSowCommandId = null;
                            log.info("SOW GroupEnd for topic '{}' (accepted={}, ignored={}, still buffered={})",
                                    ampsTopic, sowRecordsAccepted, sowRecordsIgnored, sowBuffer.size());
                        }
                        sowStateLock.notifyAll();
                        return;
                    }

                    if (!isSowSnapshotRecord(message)) {
                        sowRecordsIgnored++;
                        if (log.isDebugEnabled()) {
                            log.debug("Ignoring command {} while draining SOW snapshot for topic '{}'",
                                    command, ampsTopic);
                        }
                        sowStateLock.notifyAll();
                        return;
                    }

                    try {
                        while (running && sowBuffer.size() >= sowBufferLimit) {
                            sowStateLock.wait();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!running) {
                        return;
                    }

                    sowRecordsAccepted++;
                    sowBuffer.addLast(copyMessage(message));
                    sowStateLock.notifyAll();
                }
            }
        };
    }

    private boolean isSowSnapshotRecord(Message message) {
        String data = message.getData();
        if (data == null || data.isEmpty()) {
            return false;
        }

        int command = message.getCommand();
        if (command == Message.Command.SOW
                || command == Message.Command.Publish
                || command == Message.Command.DeltaPublish) {
            return true;
        }

        return sowState == SowState.STARTED;
    }

    private void drainSowBuffer(List<AMPSMessageData> batch) {
        while (!sowBuffer.isEmpty() && batch.size() < config.batchSize) {
            batch.add(sowBuffer.removeFirst());
        }
    }

    private String getFilterForUse() {
        return (config.ampsFilter != null && !config.ampsFilter.isEmpty()) ? config.ampsFilter : "";
    }

    private AMPSMessageData copyMessage(Message message) {
        return new AMPSMessageData(
                message.getData(),
                message.getBookmark()
        );
    }

    private List<SourceRecord> convertBatch(List<AMPSMessageData> batch) {
        List<SourceRecord> records = new ArrayList<>(batch.size());
        for (AMPSMessageData messageData : batch) {
            records.add(recordConverter.convert(messageData));
        }
        return records;
    }

    private void updateCurrentBookmark(String newBookmark) {
        if (newBookmark != null && !newBookmark.equals(currentBookmark)) {
            log.debug("Updating bookmark from '{}' to '{}'", currentBookmark, newBookmark);
            currentBookmark = newBookmark;
        }
    }

    /**
     * sow_and_subscribe keeps one AMPS command open. The first poll subscribes.
     * Later polls drain the buffer and do not subscribe again.
     */
    private List<SourceRecord> pollStandingSubscription() throws InterruptedException {
        try {
            ensureStandingSubscription();
        } catch (AMPSException e) {
            log.error("Error subscribing to AMPS topic '{}': {}", ampsTopic, e.getMessage(), e);
            logIncompleteSnapshot("subscribe failed");
            throw new RetriableException("Error subscribing to AMPS topic '" + ampsTopic + "'", e);
        }
        failIfSnapshotInterrupted();

        List<AMPSMessageData> batch = new ArrayList<>();
        long startMs = System.currentTimeMillis();
        int attempt;
        synchronized (standingLock) {
            attempt = snapshotAttempt;
            while (running && !standingSnapshotInterrupted && batch.size() < config.batchSize) {
                if (snapshotAttempt != attempt) {
                    batch.clear();
                    inFlightSnapshotRows = false;
                    attempt = snapshotAttempt;
                }
                drainStandingBuffer(batch);
                markDrainedSnapshotRows(batch);
                standingLock.notifyAll();
                if (batch.size() >= config.batchSize) {
                    break;
                }
                long now = System.currentTimeMillis();
                long remainingMs = batch.isEmpty()
                        ? config.pollTimeoutMs - (now - startMs)
                        : config.pollIdleMs - (now - standingLastMessageAtMs);
                if (remainingMs <= 0) {
                    break;
                }
                try {
                    standingLock.wait(remainingMs);
                } catch (InterruptedException e) {
                    // The batch was not returned. A disconnect can still replace the snapshot.
                    inFlightSnapshotRows = false;
                    // wait() already cleared the interrupt flag. Log, then rethrow the same
                    // exception so the host still sees the interruption.
                    logIncompleteSnapshot("poll interrupted");
                    throw e;
                }
            }
            if (snapshotAttempt != attempt) {
                batch.clear();
                inFlightSnapshotRows = false;
                attempt = snapshotAttempt;
            }
            if (!standingSnapshotInterrupted) {
                drainStandingBuffer(batch);
                markDrainedSnapshotRows(batch);
            }
        }
        // A replay GroupBegin can run here, after the drain and before currentBookmark moves.
        runAfterStandingDrain();
        synchronized (standingLock) {
            if (snapshotAttempt != attempt) {
                batch.clear();
                inFlightSnapshotRows = false;
                attempt = snapshotAttempt;
            }
        }
        // Rows drained above were never converted, so dropping them here loses nothing that was committed.
        failIfSnapshotInterrupted();
        if (batch.isEmpty()) {
            promoteStandingSubscription();
            return Collections.emptyList();
        }

        List<SourceRecord> records;
        try {
            records = convertBatch(batch);
        } catch (RuntimeException e) {
            synchronized (standingLock) {
                if (snapshotAttempt == attempt) {
                    for (int i = batch.size() - 1; i >= 0; i--) {
                        standingBuffer.addFirst(batch.get(i));
                    }
                }
                inFlightSnapshotRows = false;
                standingLock.notifyAll();
            }
            logIncompleteSnapshot("conversion failed");
            throw e;
        }

        synchronized (standingLock) {
            if (snapshotAttempt != attempt) {
                // Returned nothing. onConnectionStateChanged can still accept the replacement.
                inFlightSnapshotRows = false;
                records = Collections.emptyList();
            } else if (batchHasIncompleteSnapshot(batch)) {
                standingSnapshotRowsReceived = true;
                inFlightSnapshotRows = false;
            } else {
                inFlightSnapshotRows = false;
            }
        }
        if (records.isEmpty()) {
            promoteStandingSubscription();
            return records;
        }

        String bookmark = batch.get(batch.size() - 1).getBookmark();
        updateCurrentBookmark(bookmark);
        // Only after GroupEnd. Resuming from a bookmark while the snapshot is still open
        // would skip the keys that have not arrived yet.
        if (bookmark != null && !bookmark.isEmpty() && !sowAndSubscribeSnapshotOpen) {
            // A later reconnect must not repeat the SOW. Promotion below replaces
            // the command HAClient would otherwise resubscribe.
            resumeWithoutSow = true;
        }
        log.debug("Polled {} records from standing subscription on topic '{}'", records.size(), ampsTopic);
        promoteStandingSubscription();
        return records;
    }

    /**
     * Fails the poll when the connection dropped before the initial snapshot finished, or when
     * a finished snapshot was replayed with no bookmark to resume from. Promoting to
     * bookmarkSubscribe mid-snapshot would drop the keys still missing, and emitting a replay
     * would duplicate the rows already written. The table has to be truncated and the pipeline
     * started again, the same as any other incomplete snapshot.
     */
    private void failIfSnapshotInterrupted() {
        if (standingSnapshotInterrupted) {
            logIncompleteSnapshot("connection lost before GroupEnd");
            throw new ConnectException("The AMPS connection was lost before the sow_and_subscribe snapshot "
                    + "for topic '" + ampsTopic + "' finished. The snapshot is incomplete; truncate the "
                    + "target table before starting the pipeline again.");
        }
        if (standingReplayUnrecoverable) {
            log.error("AMPS resent sow_and_subscribe for topic '{}' after the snapshot finished, and no "
                    + "bookmark is available to resume the transaction log. Updates published while "
                    + "disconnected cannot be recovered.", ampsTopic);
            throw new ConnectException("AMPS reconnected after the sow_and_subscribe snapshot for topic '"
                    + ampsTopic + "' finished, but no bookmark was received to resume from. Updates "
                    + "published while disconnected may be missing; truncate the target table before "
                    + "starting the pipeline again.");
        }
    }

    /**
     * HAClient reconnects and replays sow_and_subscribe on its own. Catching the disconnect here
     * fails a partial snapshot even if the replayed GroupBegin never reaches the handler.
     */
    private void onConnectionStateChanged(int state) {
        if (state != ConnectionStateListener.Disconnected) {
            return;
        }
        synchronized (standingLock) {
            if (!running || !sowAndSubscribeSnapshotOpen || !standingSnapshotRowsReceived
                    || standingSnapshotInterrupted) {
                return;
            }
            log.warn("AMPS connection lost during the sow_and_subscribe snapshot for topic '{}'", ampsTopic);
            standingSnapshotInterrupted = true;
            suppressSowReplay = true;
            standingLock.notifyAll();
        }
    }

    private void ensureStandingSubscription() throws AMPSException {
        synchronized (standingLock) {
            if (standingCommandId != null || !running || ampsClient == null) {
                return;
            }
        }

        // No stored or configured bookmark: current snapshot, then live.
        // A stored bookmark would be a historical SOW point, so the snapshot
        // path sends NOW instead of currentBookmark.
        boolean snapshot = !resumeWithoutSow;
        synchronized (standingLock) {
            sowAndSubscribeSnapshotOpen = snapshot;
        }

        CommandId commandId;
        try {
            MessageHandler handler = createStandingMessageHandler();
            commandId = snapshot ? openSowAndSubscribe(handler) : openBookmarkSubscribe(handler, currentBookmark);
        } catch (AMPSException e) {
            synchronized (standingLock) {
                sowAndSubscribeSnapshotOpen = false;
            }
            throw e;
        }

        synchronized (standingLock) {
            standingCommandId = commandId;
            standingCommandIsSow = snapshot;
            standingLock.notifyAll();
        }
        if (snapshot) {
            log.info("Opened standing sow_and_subscribe subscription for topic '{}' at the current snapshot",
                    ampsTopic);
        } else {
            log.info("Opened standing bookmark subscription for topic '{}' from bookmark '{}'",
                    ampsTopic, currentBookmark);
        }
    }

    private CommandId openSowAndSubscribe(MessageHandler handler) throws AMPSException {
        String filter = getFilterForUse();
        // (handler, topic, filter, orderBy, bookmark, batchSize, topN, options, timeout).
        // NOW is the current snapshot. EPOCH ("0") is a historical SOW at the start of
        // the log. The bookmark header does not request bookmarks on publishes. The
        // bookmark option does, including when the SOW rows themselves have none, so
        // promotion can resume from a live message.
        return ampsClient.sowAndSubscribe(handler, ampsTopic, filter, null,
                Client.Bookmarks.NOW, 0, -1, "bookmark", 0L);
    }

    /**
     * Replaces a finished {@code sow_and_subscribe} with {@code bookmarkSubscribe}.
     * HAClient's subscription manager replays the original command on reconnect.
     * Replaying {@code sow_and_subscribe} delivers another full snapshot. The rows
     * are ordinary records, so the target table is duplicated. {@code resumeWithoutSow}
     * alone does not help: {@link #ensureStandingSubscription()} returns while
     * {@code standingCommandId} is still set.
     */
    private void promoteStandingSubscription() {
        if (!standingCommandIsSow || !running || ampsClient == null) {
            return;
        }
        // Never while the snapshot is open: a bookmark resume would skip the remaining keys.
        if (sowAndSubscribeSnapshotOpen || standingSnapshotInterrupted || standingReplayUnrecoverable) {
            return;
        }

        CommandId previous;
        String bookmark;
        synchronized (standingLock) {
            if (!standingCommandIsSow || sowAndSubscribeSnapshotOpen || standingSnapshotInterrupted
                    || standingReplayUnrecoverable) {
                return;
            }
            // A replay keeps the bookmark captured at its GroupBegin. A bookmark
            // read off the resent command would skip the discarded SOW updates.
            bookmark = replayRecoveryBookmark != null ? replayRecoveryBookmark : latestStandingBookmark();
            if (bookmark == null || bookmark.isEmpty()) {
                return;
            }
            previous = standingCommandId;
            standingCommandId = null;
            standingCommandIsSow = false;
            // Drop anything still delivered on the sow command, including a replay
            // that arrives while this subscribe is in flight.
            standingHandlerGeneration++;
            resumeWithoutSow = true;
            currentBookmark = bookmark;
            replayRecoveryBookmark = null;
            // The replacement subscription is the log resume, not another SOW replay.
            suppressSowReplay = false;
            // Wake a handler parked for buffer space so it sees the new generation and returns.
            // bookmarkSubscribe below waits for its ack, which only that receive thread can read.
            standingLock.notifyAll();
        }

        if (previous != null) {
            try {
                ampsClient.unsubscribe(previous);
            } catch (Exception e) {
                log.warn("Error unsubscribing sow_and_subscribe for topic '{}' before bookmark resume: {}",
                        ampsTopic, e.getMessage());
            }
        }

        try {
            MessageHandler handler = createStandingMessageHandler();
            CommandId commandId = openBookmarkSubscribe(handler, bookmark);
            synchronized (standingLock) {
                if (standingCommandId == null && running) {
                    standingCommandId = commandId;
                    standingLock.notifyAll();
                }
            }
            log.info("Replaced sow_and_subscribe with a bookmark subscription for topic '{}' from bookmark '{}' "
                    + "so a reconnect does not repeat the snapshot", ampsTopic, bookmark);
        } catch (AMPSException e) {
            log.warn("Bookmark resume after sow_and_subscribe failed for topic '{}': {}",
                    ampsTopic, e.getMessage());
        }
    }

    private String latestStandingBookmark() {
        synchronized (standingLock) {
            // Already returned, then drained but not yet stored, then still buffered.
            // Buffer order is delivery order, so a later buffer bookmark is newer than
            // one already drained. Resent-command rows never enter the buffer.
            String bookmark = currentBookmark;
            if (hasBookmark(drainedOriginalBookmark)) {
                bookmark = drainedOriginalBookmark;
            }
            for (AMPSMessageData data : standingBuffer) {
                String candidate = data.getBookmark();
                if (hasBookmark(candidate)) {
                    bookmark = candidate;
                }
            }
            return bookmark;
        }
    }

    private static boolean hasBookmark(String bookmark) {
        return bookmark != null && !bookmark.isEmpty();
    }

    private CommandId openBookmarkSubscribe(MessageHandler handler, String bookmark) throws AMPSException {
        String filter = getFilterForUse();
        try {
            return ampsClient.bookmarkSubscribe(handler, ampsTopic, filter, null, bookmark, "", 0L);
        } catch (InvalidTopicException e) {
            log.warn("Bookmark subscription failed, falling back to regular subscribe: {}", e.getMessage());
            if (filter.isEmpty()) {
                return ampsClient.subscribe(handler, ampsTopic, 0L);
            }
            return ampsClient.subscribe(handler, ampsTopic, filter, 0L);
        }
    }

    private MessageHandler createStandingMessageHandler() {
        final int generation = standingHandlerGeneration;
        return new MessageHandler() {
            @Override
            public void invoke(Message message) {
                int command = message.getCommand();
                synchronized (standingLock) {
                    if (!running || generation != standingHandlerGeneration
                            || standingSnapshotInterrupted || standingReplayUnrecoverable) {
                        discardFromBookmarkStore(message);
                        return;
                    }
                    if (command == Message.Command.GroupBegin) {
                        // A second GroupBegin is HAClient resubscribing the original sow_and_subscribe.
                        if (initialSnapshotFinished) {
                            // Whole snapshot already delivered: the replay only duplicates it.
                            // Keep the bookmark from before this replay. Bookmarks on the
                            // resent command are past the discarded SOW and would skip the gap.
                            String bookmark = latestStandingBookmark();
                            if (bookmark == null || bookmark.isEmpty()) {
                                standingReplayUnrecoverable = true;
                            } else if (replayRecoveryBookmark == null) {
                                // Freeze before any resent row can advance it. Later bookmarks
                                // on this command are past the discarded SOW.
                                replayRecoveryBookmark = bookmark;
                            }
                            suppressSowReplay = true;
                        } else if (sawStandingGroupBegin) {
                            if (standingSnapshotRowsReceived) {
                                // poll() already returned some rows. The rest only exist in this replay.
                                // Neither emitting nor dropping it is safe: fail the poll.
                                standingSnapshotInterrupted = true;
                                suppressSowReplay = true;
                            } else {
                                // Nothing has been returned, including rows only in the current
                                // poll's batch. Drop them and let this replay be the snapshot.
                                standingBuffer.clear();
                                inFlightSnapshotRows = false;
                                drainedOriginalBookmark = null;
                                snapshotAttempt++;
                            }
                        } else {
                            sawStandingGroupBegin = true;
                        }
                        discardFromBookmarkStore(message);
                        standingLock.notifyAll();
                        return;
                    }
                    if (command == Message.Command.GroupEnd) {
                        if (suppressSowReplay) {
                            // Stay suppressed through the rest of the resent command.
                            // Clearing it here lets a later bookmark into the buffer, and
                            // promotion would then resume past the discarded SOW.
                        } else {
                            sowAndSubscribeSnapshotOpen = false;
                            initialSnapshotFinished = true;
                            // drainStandingBuffer() held this row back; its offset now
                            // records a finished snapshot.
                            AMPSMessageData last = standingBuffer.peekLast();
                            if (last != null && last.isSnapshotIncomplete()) {
                                standingBuffer.pollLast();
                                standingBuffer.addLast(last.asSnapshotComplete());
                            }
                        }
                        discardFromBookmarkStore(message);
                        standingLock.notifyAll();
                        return;
                    }
                    if (suppressSowReplay) {
                        discardFromBookmarkStore(message);
                        return;
                    }
                    try {
                        // Also leave when this handler has been replaced. Promotion's
                        // bookmarkSubscribe waits for an ack that only this receive thread
                        // can read, so a handler parked here would hang the poll thread.
                        while (running && generation == standingHandlerGeneration
                                && standingBuffer.size() >= sowBufferLimit) {
                            standingLock.wait();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!running || generation != standingHandlerGeneration) {
                        discardFromBookmarkStore(message);
                        return;
                    }
                    AMPSMessageData data = copyMessage(message);
                    if (sowAndSubscribeSnapshotOpen && !initialSnapshotFinished) {
                        data = new AMPSMessageData(data.getData(), data.getBookmark(), true);
                    }
                    standingBuffer.addLast(data);
                    standingLastMessageAtMs = System.currentTimeMillis();
                    discardFromBookmarkStore(message);
                    standingLock.notifyAll();
                }
            }
        };
    }

    /**
     * While the snapshot is open the newest row stays buffered, so the row emitted last
     * before GroupEnd can carry a finished-snapshot offset. Otherwise a snapshot followed
     * by no live traffic would leave an incomplete-snapshot offset and block the restart.
     */
    private void drainStandingBuffer(List<AMPSMessageData> batch) {
        int holdBack = sowAndSubscribeSnapshotOpen ? 1 : 0;
        while (standingBuffer.size() > holdBack && batch.size() < config.batchSize) {
            AMPSMessageData data = standingBuffer.removeFirst();
            batch.add(data);
            if (hasBookmark(data.getBookmark())) {
                drainedOriginalBookmark = data.getBookmark();
            }
        }
    }

    /**
     * Called with {@code standingLock} held, after {@link #drainStandingBuffer(List)}.
     * Rows in the batch are in flight until {@code poll()} returns them. Setting
     * {@link #standingSnapshotRowsReceived} here would make {@link #onConnectionStateChanged}
     * reject a replacement snapshot while the table does not yet have those rows.
     */
    private void markDrainedSnapshotRows(List<AMPSMessageData> batch) {
        if (standingSnapshotRowsReceived || inFlightSnapshotRows) {
            return;
        }
        if (batchHasIncompleteSnapshot(batch)) {
            inFlightSnapshotRows = true;
        }
    }

    private static boolean batchHasIncompleteSnapshot(List<AMPSMessageData> batch) {
        for (AMPSMessageData data : batch) {
            if (data.isSnapshotIncomplete()) {
                return true;
            }
        }
        return false;
    }

    /**
     * One error line when a snapshot stops before AMPS sends GroupEnd.
     * The pipeline has to truncate the target table before the next start.
     */
    private void logIncompleteSnapshot(String reason) {
        if (!isSnapshotIncomplete()) {
            return;
        }
        if (!incompleteSnapshotLogged.compareAndSet(false, true)) {
            return;
        }
        log.error("SOW snapshot incomplete for topic '{}'; truncate the target table before starting the pipeline again ({})",
                ampsTopic, reason);
    }

    private boolean isSnapshotIncomplete() {
        if (config == null) {
            return false;
        }
        if (config.ampsSubscriptionType == SubscriptionType.SOW) {
            return sowState == SowState.STARTED;
        }
        return sowAndSubscribeSnapshotOpen;
    }

    /**
     * Subscribe to AMPS topic starting from the given bookmark.
     */
    private CommandId subscribeFromBookmark(String bookmark, MessageHandler handler) throws AMPSException {
        String filter = config.ampsFilter;
        SubscriptionType subscriptionType = config.ampsSubscriptionType;
        String filterToUse = (filter != null && !filter.isEmpty()) ? filter : "";

        log.trace("Subscribing to AMPS topic '{}' from bookmark '{}', filter: '{}', mode: {}", 
                ampsTopic, bookmark, filterToUse.isEmpty() ? "(none)" : filterToUse,
                subscriptionType.configValue());

        switch (subscriptionType) {
            case SUBSCRIBE:
            default:
                try {
                    return ampsClient.bookmarkSubscribe(handler, ampsTopic, filterToUse, null, bookmark, "", 0L);
                } catch (InvalidTopicException e) {
                    log.warn("Bookmark subscription failed, falling back to regular subscribe: {}", e.getMessage());
                    if (filterToUse.isEmpty()) {
                        return ampsClient.subscribe(handler, ampsTopic, 0L);
                    } else {
                        return ampsClient.subscribe(handler, ampsTopic, filterToUse, 0L);
                    }
                }
        }
    }

    @Override
    public void stop() {
        log.info("Stopping AMPS Source Task for topic '{}'", ampsTopic);
        running = false;
        synchronized (pollWaitLock) {
            pollWaitLock.notifyAll();
        }
        synchronized (sowStateLock) {
            sowStateLock.notifyAll();
        }
        synchronized (standingLock) {
            standingLock.notifyAll();
        }

        logIncompleteSnapshot("task stop");
        resetStandingSubscriptionState("task stop");

        if (ampsClient != null) {
            try {
                if (currentSowCommandId != null) {
                    try {
                        ampsClient.unsubscribe(currentSowCommandId);
                    } catch (Exception e) {
                        log.warn("Error unsubscribing active SOW query for topic '{}': {}", ampsTopic, e.getMessage());
                    } finally {
                        currentSowCommandId = null;
                    }
                }
                ampsClient.close();
                log.info("AMPS client closed for topic '{}'", ampsTopic);
            } catch (Exception e) {
                log.error("Error closing AMPS client for topic '{}': {}", ampsTopic, e.getMessage(), e);
            }
        }

        resetSowSnapshotLifecycleState("task stop");

        log.info("AMPS Source Task stopped for topic '{}'. Last bookmark: '{}'", ampsTopic, currentBookmark);
    }
}
