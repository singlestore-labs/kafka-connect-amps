package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.exception.AMPSException;
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
     * The one sow_and_subscribe command for this task. It holds the snapshot phase, the buffer,
     * and the resume bookmark. Null for subscribe and sow.
     */
    StandingSubscription standing;

    /** Set once the "SOW snapshot incomplete" error is logged, so it is logged once per start. */
    private final AtomicBoolean incompleteSnapshotLogged = new AtomicBoolean();

    /**
     * A promotion that failed after this poll already had records to return.
     * The records go back to the host first so their offsets are stored.
     * The next poll throws this exception.
     */
    private ConnectException promotionFailure;

    /**
     * Prefetch cap for the sow query. AMPS delivers the snapshot on a
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
        this.incompleteSnapshotLogged.set(false);
        this.promotionFailure = null;

        resetSowSnapshotLifecycleState("task start");
        if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE) {
            if (standing != null) {
                standing.reset();
            }
            String filter = config.ampsFilter == null || config.ampsFilter.isEmpty() ? "" : config.ampsFilter;
            standing = new StandingSubscription(config.ampsTopic, filter, config.batchSize,
                    config.pollTimeoutMs, config.pollIdleMs, this::discardFromBookmarkStore,
                    this::logIncompleteSnapshot);
            standing.setRunning(true);
        } else {
            standing = null;
        }

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
            if (standing != null) {
                standing.setClient(ampsClient);
                ampsClient.addConnectionStateListener(standing::onDisconnected);
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
            if (standing != null) {
                standing.resumeFrom(currentBookmark);
            }
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
            if (standing != null && currentBookmark != null && !currentBookmark.isEmpty()) {
                standing.resumeFrom(currentBookmark);
            }
            log.info("Found stored offset for topic '{}': bookmark='{}'", ampsTopic, currentBookmark);
        } else {
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
                        // Copy data immediately (AMPS reuses Message objects).
                        // A row with no bookmark keeps the previous one so its offset is not empty.
                        String bookmark = message.getBookmark();
                        boolean ownBookmark = bookmark != null && !bookmark.isEmpty();
                        String previous = lastBookmarkRef.get();
                        AMPSMessageData data = ownBookmark
                                ? new AMPSMessageData(message.getData(), bookmark)
                                : new AMPSMessageData(message.getData(), null,
                                        previous != null && !previous.isEmpty() ? previous : null,
                                        SnapshotState.NONE);
                        batch.add(data);
                        if (ownBookmark) {
                            lastBookmarkRef.set(bookmark);
                        }
                        lastMessageAtMs.set(System.currentTimeMillis());

                        log.trace("Collected message {} of {} from topic '{}'",
                                batch.size(), config.batchSize, ampsTopic);
                    }
                    pollWaitLock.notifyAll();
                }
                // The client logs every delivered message into the bookmark store, including
                // ones past batch.size. Nothing replays from it, so discard before invoke()
                // returns. Doing it outside pollWaitLock keeps a slow client call off the poll thread.
                discardFromBookmarkStore(message);
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
        if (promotionFailure != null) {
            throw promotionFailure;
        }
        try {
            standing.open();
        } catch (ConnectException e) {
            throw e;
        } catch (AMPSException e) {
            log.error("Error subscribing to AMPS topic '{}': {}", ampsTopic, e.getMessage(), e);
            logIncompleteSnapshot("subscribe failed");
            throw new RetriableException("Error subscribing to AMPS topic '" + ampsTopic + "'", e);
        }

        StandingSubscription.PolledBatch batch;
        try {
            batch = standing.drain();
        } catch (InterruptedException e) {
            // wait() already cleared the interrupt flag. Log, then rethrow the same
            // exception so the host still sees the interruption.
            logIncompleteSnapshot("poll interrupted");
            throw e;
        }
        standing.failIf();
        if (batch.rows.isEmpty()) {
            standing.promote();
            return Collections.emptyList();
        }

        List<SourceRecord> records;
        try {
            records = convertBatch(batch.rows);
        } catch (RuntimeException e) {
            standing.restore(batch);
            logIncompleteSnapshot("conversion failed");
            throw e;
        }
        if (!standing.keep(batch)) {
            standing.promote();
            return Collections.emptyList();
        }

        updateCurrentBookmark(batch.rows.get(batch.rows.size() - 1).getOffsetBookmark());
        log.debug("Polled {} records from standing subscription on topic '{}'", records.size(), ampsTopic);
        try {
            standing.promote();
        } catch (ConnectException e) {
            // The host stores offsets for a returned batch before the next poll.
            // Throwing here would drop the GroupEnd bookmark and make the next start refuse.
            promotionFailure = e;
        }
        return records;
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
        return standing != null && standing.isSnapshotIncomplete();
    }

    /**
     * Subscribe to AMPS topic starting from the given bookmark.
     * A topic with no transaction log falls back to a plain subscribe. That fallback is only
     * for this short-lived path.
     */
    private CommandId subscribeFromBookmark(String bookmark, MessageHandler handler) throws AMPSException {
        String filterToUse = getFilterForUse();
        log.trace("Subscribing to AMPS topic '{}' from bookmark '{}', filter: '{}', mode: {}",
                ampsTopic, bookmark, filterToUse.isEmpty() ? "(none)" : filterToUse,
                config.ampsSubscriptionType.configValue());
        return StandingSubscription.bookmarkSubscribe(
                ampsClient, handler, ampsTopic, filterToUse, bookmark, true);
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
        if (standing != null) {
            standing.setRunning(false);
        }

        logIncompleteSnapshot("task stop");
        if (standing != null) {
            standing.reset();
        }

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
