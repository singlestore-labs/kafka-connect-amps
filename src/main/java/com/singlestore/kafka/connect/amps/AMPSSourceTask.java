package com.singlestore.kafka.connect.amps;

import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.CommandId;
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
            initializeBookmark();
            if (config.ampsSubscriptionType == SubscriptionType.SOW) {
                log.info("AMPS Source Task started successfully for topic '{}' in full-SOW snapshot mode", ampsTopic);
            } else {
                log.info("AMPS Source Task started successfully for topic '{}', starting bookmark: '{}'",
                        ampsTopic, currentBookmark);
            }
        } catch (Exception e) {
            log.error("Failed to start AMPS Source Task for topic '{}': {}", ampsTopic, e.getMessage(), e);
            throw new ConnectException("Failed to start AMPS Source Task", e);
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
            resumeWithoutSow = config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE;
            log.info("Using configured start bookmark for topic '{}': bookmark='{}'", ampsTopic, currentBookmark);
            return;
        }

        // Check for stored offset
        Map<String, Object> sourcePartition = Collections.singletonMap(RecordConverter.FIELD_TOPIC, ampsTopic);
        Map<String, Object> lastOffset = context.offsetStorageReader().offset(sourcePartition);

        if (lastOffset != null && !lastOffset.isEmpty()) {
            currentBookmark = (String) lastOffset.get(RecordConverter.FIELD_BOOKMARK);
            resumeWithoutSow = config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE
                    && currentBookmark != null && !currentBookmark.isEmpty();
            log.info("Found stored offset for topic '{}': bookmark='{}'", ampsTopic, currentBookmark);
        } else {
            resumeWithoutSow = false;
            // No stored offset - use configured position
            if (config.ampsPosition == Position.EPOCH) {
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

        List<AMPSMessageData> batch = new ArrayList<>();
        long startMs = System.currentTimeMillis();
        synchronized (standingLock) {
            while (running && batch.size() < config.batchSize) {
                drainStandingBuffer(batch);
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
                standingLock.wait(remainingMs);
            }
            drainStandingBuffer(batch);
        }

        List<SourceRecord> records;
        try {
            records = convertBatch(batch);
        } catch (RuntimeException e) {
            synchronized (standingLock) {
                for (int i = batch.size() - 1; i >= 0; i--) {
                    standingBuffer.addFirst(batch.get(i));
                }
                standingLock.notifyAll();
            }
            logIncompleteSnapshot("conversion failed");
            throw e;
        }

        if (!records.isEmpty()) {
            String bookmark = batch.get(batch.size() - 1).getBookmark();
            updateCurrentBookmark(bookmark);
            if (bookmark != null && !bookmark.isEmpty()) {
                // A later reconnect in this task resumes the log instead of repeating the SOW.
                resumeWithoutSow = true;
            }
            log.debug("Polled {} records from standing subscription on topic '{}'", records.size(), ampsTopic);
        }
        return records;
    }

    private void ensureStandingSubscription() throws AMPSException {
        synchronized (standingLock) {
            if (standingCommandId != null || !running || ampsClient == null) {
                return;
            }
        }

        boolean snapshot = !resumeWithoutSow;
        synchronized (standingLock) {
            sowAndSubscribeSnapshotOpen = snapshot;
        }

        CommandId commandId;
        try {
            MessageHandler handler = createStandingMessageHandler();
            commandId = snapshot ? openSowAndSubscribe(handler) : openBookmarkSubscribe(handler);
        } catch (AMPSException e) {
            synchronized (standingLock) {
                sowAndSubscribeSnapshotOpen = false;
            }
            throw e;
        }

        synchronized (standingLock) {
            standingCommandId = commandId;
            standingLock.notifyAll();
        }
        log.info("Opened standing {} subscription for topic '{}' from bookmark '{}'",
                snapshot ? "sow_and_subscribe" : "bookmark", ampsTopic, currentBookmark);
    }

    private CommandId openSowAndSubscribe(MessageHandler handler) throws AMPSException {
        String filter = getFilterForUse();
        try {
            // Bookmark is a command header. AMPS rejects it inside the options string.
            return ampsClient.sowAndSubscribe(handler, ampsTopic, filter,
                    null, currentBookmark, 0, -1, null, 0L);
        } catch (InvalidTopicException e) {
            log.warn("SOW+subscribe with bookmark failed, falling back to sowAndSubscribe without a bookmark: {}",
                    e.getMessage());
            return ampsClient.sowAndSubscribe(handler, ampsTopic, filter, 0, 0L);
        }
    }

    private CommandId openBookmarkSubscribe(MessageHandler handler) throws AMPSException {
        String filter = getFilterForUse();
        try {
            return ampsClient.bookmarkSubscribe(handler, ampsTopic, filter, null, currentBookmark, "", 0L);
        } catch (InvalidTopicException e) {
            log.warn("Bookmark subscription failed, falling back to regular subscribe: {}", e.getMessage());
            if (filter.isEmpty()) {
                return ampsClient.subscribe(handler, ampsTopic, 0L);
            }
            return ampsClient.subscribe(handler, ampsTopic, filter, 0L);
        }
    }

    private MessageHandler createStandingMessageHandler() {
        return new MessageHandler() {
            @Override
            public void invoke(Message message) {
                int command = message.getCommand();
                synchronized (standingLock) {
                    if (!running) {
                        return;
                    }
                    if (command == Message.Command.GroupEnd) {
                        sowAndSubscribeSnapshotOpen = false;
                        discardFromBookmarkStore(message);
                        standingLock.notifyAll();
                        return;
                    }
                    if (command == Message.Command.GroupBegin) {
                        discardFromBookmarkStore(message);
                        return;
                    }
                    try {
                        while (running && standingBuffer.size() >= sowBufferLimit) {
                            standingLock.wait();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (!running) {
                        return;
                    }
                    standingBuffer.addLast(copyMessage(message));
                    standingLastMessageAtMs = System.currentTimeMillis();
                    discardFromBookmarkStore(message);
                    standingLock.notifyAll();
                }
            }
        };
    }

    private void drainStandingBuffer(List<AMPSMessageData> batch) {
        while (!standingBuffer.isEmpty() && batch.size() < config.batchSize) {
            batch.add(standingBuffer.removeFirst());
        }
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

        if (ampsClient != null) {
            try {
                CommandId standingId = standingCommandId;
                standingCommandId = null;
                if (standingId != null) {
                    try {
                        ampsClient.unsubscribe(standingId);
                    } catch (Exception e) {
                        log.warn("Error unsubscribing standing subscription for topic '{}': {}", ampsTopic, e.getMessage());
                    }
                }
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
