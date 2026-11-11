package com.singlestore.kafka.connect.amps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.crankuptheamps.client.BookmarkStore;
import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.MessageHandler;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.exception.AMPSException;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for AMPSSourceTask subscribe-on-demand polling.
 * 
 * Key behaviors:
 * - start() only initializes client and reads bookmark (no subscription)
 * - poll() subscribes, collects messages, unsubscribes each call
 * - stop() only closes client (no subscription to unsubscribe)
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AMPSSourceTaskTest {

    @Mock
    private AMPSClientFactory clientFactory;

    @Mock
    private HAClient ampsClient;

    @Mock
    private SourceTaskContext context;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @Mock
    private CommandId commandId;

    private AMPSSourceTask task;
    private Map<String, String> props;

    @BeforeEach
    void setUp() throws Exception {
        task = new AMPSSourceTask();
        task.clientFactory = clientFactory;

        props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_CLIENT_NAME_CONF, "test-client");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "test-topic");
        props.put(AMPSSourceConnectorConfig.KAFKA_TOPIC_PREFIX_CONF, "amps-");
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_POSITION_CONF, "LATEST");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "100");
        props.put(AMPSSourceConnectorConfig.POLL_IDLE_MS_CONF, "50");

        when(clientFactory.create(any())).thenReturn(ampsClient);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(any())).thenReturn(null);
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(), 
                isNull(), anyString(), anyString(), anyLong()))
                .thenReturn(commandId);
    }

    private Message mockMessage(String data, String bookmark, int command) {
        Message message = mock(Message.class);
        when(message.getData()).thenReturn(data);
        when(message.getBookmark()).thenReturn(bookmark);
        when(message.getTopic()).thenReturn("test-topic");
        when(message.getCommand()).thenReturn(command);
        return message;
    }

    @Test
    void testVersion() {
        assertEquals("0.1.0", task.version());
    }

    @Test
    void testStartInitializesClientButDoesNotSubscribe() throws Exception {
        task.initialize(context);
        task.start(props);

        // Client should be created
        verify(clientFactory).create(any());
        
        // No subscription should happen in start() - subscription happens in poll()
        verify(ampsClient, never()).bookmarkSubscribe(any(), any(), any(), any(), any(), any(), anyLong());
        verify(ampsClient, never()).subscribe(any(MessageHandler.class), anyString(), anyLong());
    }

    @Test
    void testStartWithStoredBookmarkInitializesBookmark() throws Exception {
        String storedBookmark = "15|1711224137000123456|0";
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, storedBookmark);
        when(offsetStorageReader.offset(any())).thenReturn(storedOffset);

        task.initialize(context);
        task.start(props);

        // Verify client created but no subscription yet
        verify(clientFactory).create(any());
        verify(ampsClient, never()).bookmarkSubscribe(any(), any(), any(), any(), any(), any(), anyLong());
    }

    @Test
    void testPollSubscribesAndUnsubscribes() throws Exception {
        task.initialize(context);
        task.start(props);

        // First poll - should subscribe with NOW bookmark (no stored offset)
        List<SourceRecord> records = task.poll();

        // Verify subscribe was called during poll
        verify(ampsClient).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(""), anyLong());
        
        // Verify unsubscribe was called after poll
        verify(ampsClient).unsubscribe(commandId);
        
        assertNotNull(records);
    }

    @Test
    void testPollWithStoredBookmarkSubscribesFromBookmark() throws Exception {
        String storedBookmark = "15|1711224137000123456|0";
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, storedBookmark);
        when(offsetStorageReader.offset(any())).thenReturn(storedOffset);

        task.initialize(context);
        task.start(props);
        task.poll();

        // Verify subscribe was called with stored bookmark
        verify(ampsClient).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(storedBookmark), eq(""), anyLong());
    }

    @Test
    void testPollWithEpochPositionSubscribesFromEpoch() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_POSITION_CONF, "EPOCH");

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.EPOCH), eq(""), anyLong());
    }

    @Test
    void testPollReturnsEmptyWhenNoMessages() throws Exception {
        task.initialize(context);
        task.start(props);

        long started = System.nanoTime();
        List<SourceRecord> records = task.poll();
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

        assertNotNull(records);
        assertTrue(records.isEmpty());
        assertTrue(elapsedMs >= 80, "empty poll should wait poll.timeout.ms, took " + elapsedMs + "ms");
        assertTrue(elapsedMs < 500, "empty poll waited too long: " + elapsedMs + "ms");
    }

    @Test
    void testPollAfterStopReturnsEmptyListWithoutBlocking() throws Exception {
        task.initialize(context);
        task.start(props);
        task.stop();

        long started = System.nanoTime();
        List<SourceRecord> records = task.poll();
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

        assertNotNull(records);
        assertTrue(records.isEmpty());
        assertTrue(elapsedMs < 50, "poll() after stop should return immediately, took " + elapsedMs + "ms");
    }

    @Test
    void testPollCollectsMessagesAndUpdatesBookmark() throws Exception {
        // Setup mock to invoke the handler with a message when subscribe is called
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(), 
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    
                    // Simulate AMPS sending messages
                    Message mockMessage = mock(Message.class);
                    when(mockMessage.getData()).thenReturn("{\"test\":\"data\"}");
                    when(mockMessage.getBookmark()).thenReturn("bookmark-1");
                    when(mockMessage.getTopic()).thenReturn("test-topic");
                    
                    // Invoke handler in a separate thread (simulating AMPS async behavior)
                    new Thread(() -> handler.invoke(mockMessage)).start();
                    
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> records = task.poll();

        // Should have collected the message
        assertEquals(1, records.size());
        assertEquals("amps-test-topic", records.get(0).topic());
    }

    @Test
    void testSubscribeFailureIsRetriableAndKeepsBookmark() throws Exception {
        String storedBookmark = "15|1711224137000123456|0";
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, storedBookmark);
        when(offsetStorageReader.offset(any())).thenReturn(storedOffset);
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenThrow(new AMPSException("connection reset"))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);

        RetriableException failure = assertThrows(RetriableException.class, () -> task.poll());
        assertInstanceOf(AMPSException.class, failure.getCause());

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        verify(ampsClient, times(2)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(storedBookmark), eq(""), anyLong());
    }

    @Test
    void testConversionFailureDoesNotAdvanceBookmark() throws Exception {
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> handler.invoke(mockMessage("{\"id\":1}", "bookmark-1", 0))).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);
        installConverter(new RecordConverter(new AMPSSourceConnectorConfig(props)) {
            @Override
            public SourceRecord convert(AMPSMessageData messageData) {
                throw new IllegalStateException("bad payload");
            }
        });

        assertThrows(IllegalStateException.class, () -> task.poll());

        installConverter(new RecordConverter(new AMPSSourceConnectorConfig(props)));
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        verify(ampsClient, times(2)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(""), anyLong());
    }

    @Test
    void testSowStartFailureIsRetriable() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "100");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenThrow(new AMPSException("sow failed"))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);

        RetriableException failure = assertThrows(RetriableException.class, () -> task.poll());
        assertInstanceOf(AMPSException.class, failure.getCause());

        assertTrue(task.poll().isEmpty());
        verify(ampsClient, times(2)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                anyInt(), eq(0L));
    }

    private void installConverter(RecordConverter converter) throws Exception {
        Field field = AMPSSourceTask.class.getDeclaredField("recordConverter");
        field.setAccessible(true);
        field.set(task, converter);
    }

    @Test
    void testPollRespectsBatchSize() throws Exception {
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "3");
        
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(), 
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    
                    // Send 5 messages, but batch size is 3
                    new Thread(() -> {
                        for (int i = 1; i <= 5; i++) {
                            Message mockMessage = mock(Message.class);
                            when(mockMessage.getData()).thenReturn("{\"id\":" + i + "}");
                            when(mockMessage.getBookmark()).thenReturn("bookmark-" + i);
                            when(mockMessage.getTopic()).thenReturn("test-topic");
                            handler.invoke(mockMessage);
                        }
                    }).start();
                    
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> records = task.poll();

        // Should only collect batch.size (3) messages
        assertEquals(3, records.size());
    }

    @Test
    void testPollReturnsPartialBatchAfterIdleWithoutWaitingTimeout() throws Exception {
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "2000");
        props.put(AMPSSourceConnectorConfig.POLL_IDLE_MS_CONF, "50");

        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        for (int i = 1; i <= 3; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", "bookmark-" + i, 0));
                        }
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        long started = System.nanoTime();
        List<SourceRecord> records = task.poll();
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

        assertEquals(3, records.size());
        assertTrue(elapsedMs < 800, "partial batch should return after poll.idle.ms, took " + elapsedMs + "ms");
        verify(ampsClient).unsubscribe(commandId);
    }

    @Test
    void testPollReturnsFullBatchWithoutWaitingIdle() throws Exception {
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "3");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "2000");
        props.put(AMPSSourceConnectorConfig.POLL_IDLE_MS_CONF, "800");

        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        for (int i = 1; i <= 3; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", "bookmark-" + i, 0));
                        }
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        long started = System.nanoTime();
        List<SourceRecord> records = task.poll();
        long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

        assertEquals(3, records.size());
        assertTrue(elapsedMs < 400, "full batch should return immediately, took " + elapsedMs + "ms");
        verify(ampsClient).unsubscribe(commandId);
    }

    @Test
    void testStopClosesClientOnly() throws Exception {
        task.initialize(context);
        task.start(props);
        task.stop();

        // Only close should be called (no persistent subscription to unsubscribe)
        verify(ampsClient).close();
        verify(ampsClient, never()).unsubscribe(any(CommandId.class));
    }

    @Test
    void testStartFailsWithNoTopic() {
        props.remove(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF);
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "");

        task.initialize(context);

        assertThrows(RuntimeException.class, () -> task.start(props));
    }

    @Test
    void testPollWithFilter() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_FILTER_CONF, "/price > 100");
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq("/price > 100"), 
                isNull(), anyString(), anyString(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq("/price > 100"), 
                isNull(), eq(Client.Bookmarks.NOW), eq(""), anyLong());
    }

    @Test
    void testPollWithSowAndSubscribeStaysOpenAcrossPolls() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_POSITION_CONF, "EPOCH");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> first = task.poll();
        List<SourceRecord> second = task.poll();

        // No GroupEnd yet, so the newest snapshot row stays buffered.
        assertEquals(1, first.size());
        assertEquals("bm-1", first.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        assertTrue(second.isEmpty());
        // Default EPOCH must not be sent. That header is a historical SOW point.
        verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), eq(0L));
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                any(), eq(Client.Bookmarks.EPOCH), anyInt(), anyInt(), any(), anyLong());
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                anyInt(), anyLong());
        verify(ampsClient, never()).unsubscribe(any(CommandId.class));
    }

    @Test
    void testSowAndSubscribeLatestAlsoRequestsBookmarksWithNow() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_POSITION_CONF, "LATEST");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), eq(0L));
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                anyInt(), anyLong());
    }

    @Test
    void testSowAndSubscribeRestartDropsBufferAndOpensAgain() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "1");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "200");

        CommandId firstCommand = mock(CommandId.class);
        CommandId secondCommand = mock(CommandId.class);
        AtomicInteger calls = new AtomicInteger();
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    int call = calls.incrementAndGet();
                    if (call == 1) {
                        handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                        handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                        return firstCommand;
                    }
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":3}", "bm-3", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return secondCommand;
                });

        task.initialize(context);
        task.start(props);
        List<SourceRecord> first = task.poll();
        assertEquals(1, first.size());
        assertEquals("bm-1", first.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));

        // Reuse the task without stop(), so the previous command id and buffer are still set.
        task.start(props);
        List<SourceRecord> afterRestart = task.poll();

        assertEquals(1, afterRestart.size());
        assertEquals("bm-3", afterRestart.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        verify(ampsClient, times(2)).sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), eq(0L));
        verify(ampsClient).unsubscribe(firstCommand);
    }

    @Test
    void testSowAndSubscribeRestartLogsIncompleteSnapshotAgain() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return commandId;
                });
        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            assertEquals(1, task.poll().size());
            task.stop();

            task.start(props);
            assertEquals(1, task.poll().size());
            task.stop();

            long incomplete = logs.list.stream()
                    .filter(event -> event.getLevel() == Level.ERROR
                            && event.getFormattedMessage().contains(
                            "SOW snapshot incomplete for topic 'test-topic'"))
                    .count();
            assertEquals(2, incomplete);
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testSowAndSubscribeAfterGroupEndResumesWithBookmark() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        CommandId sowCommand = mock(CommandId.class);
        CommandId bookmarkCommand = mock(CommandId.class);
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return sowCommand;
                });
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-1"), eq(""), anyLong()))
                .thenReturn(bookmarkCommand);

        task.initialize(context);
        task.start(props);
        List<SourceRecord> first = task.poll();
        List<SourceRecord> second = task.poll();

        assertEquals(1, first.size());
        assertEquals("bm-1", first.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        assertTrue(second.isEmpty());
        verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), eq(0L));
        verify(ampsClient).unsubscribe(sowCommand);
        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-1"), eq(""), anyLong());
    }

    @Test
    void testReplayedSowAfterCompletedSnapshotIsNotEmitted() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        CommandId sowCommand = mock(CommandId.class);
        CommandId bookmarkCommand = mock(CommandId.class);
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    // HAClient resubscribed the same command after a reconnect.
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-dup", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return sowCommand;
                });
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-1"), eq(""), anyLong()))
                .thenReturn(bookmarkCommand);

        task.initialize(context);
        task.start(props);
        List<SourceRecord> records = task.poll();

        assertEquals(1, records.size());
        assertEquals("bm-1", records.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        verify(ampsClient).unsubscribe(sowCommand);
        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-1"), eq(""), anyLong());
        verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                any(), any(), anyInt(), anyInt(), any(), anyLong());
    }

    @Test
    void testPromotionDoesNotHangWhenHandlerIsBlockedOnFullBuffer() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        // Buffer limit is 2 x batch.size = 2, so the AMPS receive thread blocks quickly.
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "1");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");
        CommandId sowCommand = mock(CommandId.class);
        CommandId bookmarkCommand = mock(CommandId.class);
        AtomicReference<Thread> receiveThread = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean receiveThreadReleased =
                new java.util.concurrent.atomic.AtomicBoolean();

        // Build the messages up front so the receive thread reaches the full buffer quickly.
        Message groupBegin = mockMessage(null, null, Message.Command.GroupBegin);
        Message firstRow = mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW);
        Message groupEnd = mockMessage(null, null, Message.Command.GroupEnd);
        List<Message> liveRows = new java.util.ArrayList<>();
        for (int i = 2; i <= 8; i++) {
            liveRows.add(mockMessage("{\"id\":" + i + "}", "bm-" + i, Message.Command.Publish));
        }
        java.util.concurrent.CountDownLatch snapshotFinishes = new java.util.concurrent.CountDownLatch(1);

        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    Thread thread = new Thread(() -> {
                        handler.invoke(groupBegin);
                        handler.invoke(firstRow);
                        try {
                            snapshotFinishes.await();
                        } catch (InterruptedException e) {
                            return;
                        }
                        handler.invoke(groupEnd);
                        // More live messages than the buffer holds.
                        for (Message row : liveRows) {
                            handler.invoke(row);
                        }
                    }, "fake-amps-receive-thread");
                    receiveThread.set(thread);
                    thread.start();
                    return sowCommand;
                });
        // bookmarkSubscribe waits for its ack, which only the receive thread can read.
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), anyString(), eq(""), anyLong()))
                .thenAnswer(invocation -> {
                    Thread thread = receiveThread.get();
                    thread.join(3000);
                    receiveThreadReleased.set(!thread.isAlive());
                    return bookmarkCommand;
                });

        task.initialize(context);
        task.start(props);

        // The snapshot is still open here, so this poll cannot promote. Its only row is held
        // back until GroupEnd.
        assertTrue(task.poll().isEmpty());

        // GroupEnd and the live rows arrive. The receive thread fills the 2-row buffer and
        // blocks waiting for space.
        snapshotFinishes.countDown();
        long deadline = System.currentTimeMillis() + 2000;
        while (receiveThread.get().getState() != Thread.State.WAITING
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(Thread.State.WAITING, receiveThread.get().getState(),
                "receive thread should be blocked on the full buffer");

        // Pause the poll thread after it drains a row and before it promotes. The woken receive
        // thread then refills the buffer and blocks again, which is what a busy AMPS feed does.
        Logger taskLogger = (Logger) LoggerFactory.getLogger(AMPSSourceTask.class);
        Level previousLevel = taskLogger.getLevel();
        ch.qos.logback.core.AppenderBase<ILoggingEvent> pause = new ch.qos.logback.core.AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains("from standing subscription on topic")) {
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
        pause.start();
        taskLogger.setLevel(Level.DEBUG);
        taskLogger.addAppender(pause);

        long startedAt;
        long elapsedMs;
        try {
            // This poll ends by promoting to bookmarkSubscribe. It has to wake the blocked
            // receive thread first or the subscribe ack is never read.
            startedAt = System.nanoTime();
            task.poll();
            elapsedMs = (System.nanoTime() - startedAt) / 1_000_000L;
        } finally {
            taskLogger.detachAppender(pause);
            taskLogger.setLevel(previousLevel);
        }

        verify(ampsClient).unsubscribe(sowCommand);
        assertTrue(receiveThreadReleased.get(),
                "bookmarkSubscribe waited on a receive thread that was still blocked on the standing buffer");
        assertTrue(elapsedMs < 2500, "poll stalled for " + elapsedMs + "ms waiting for the subscribe ack");
        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), anyString(), eq(""), anyLong());
    }

    @Test
    void testReconnectMidSnapshotFailsInsteadOfResumingFromBookmark() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        CommandId sowCommand = mock(CommandId.class);
        AtomicReference<MessageHandler> sowHandler = new AtomicReference<>();
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    sowHandler.set(handler);
                    // Snapshot starts, two keys arrive, GroupEnd never does.
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return sowCommand;
                });

        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            assertEquals(1, task.poll().size());

            // HAClient reconnects and resends the original command: the keys still missing
            // exist only in this replay.
            sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupBegin));
            sowHandler.get().invoke(mockMessage("{\"id\":1}", "bm-dup-1", Message.Command.SOW));
            sowHandler.get().invoke(mockMessage("{\"id\":2}", "bm-dup-2", Message.Command.SOW));
            sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupEnd));

            ConnectException failure = assertThrows(ConnectException.class, () -> task.poll());
            assertFalse(failure instanceof RetriableException);
            assertIncompleteSnapshotLogged(logs, true);

            // Must not silently continue on a partial snapshot.
            verify(ampsClient, never()).bookmarkSubscribe(any(MessageHandler.class), anyString(), anyString(),
                    any(), anyString(), anyString(), anyLong());
            verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                    any(), any(), anyInt(), anyInt(), any(), anyLong());
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testReconnectBeforeAnyRowIsTreatedAsTheSnapshot() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "100");
        CommandId sowCommand = mock(CommandId.class);
        CommandId bookmarkCommand = mock(CommandId.class);
        AtomicReference<MessageHandler> sowHandler = new AtomicReference<>();
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    sowHandler.set(handler);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    return sowCommand;
                });
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), anyString(), eq(""), anyLong()))
                .thenReturn(bookmarkCommand);

        task.initialize(context);
        task.start(props);
        assertTrue(task.poll().isEmpty());

        // Nothing was delivered before the reconnect, so the replay is the whole snapshot.
        sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupBegin));
        sowHandler.get().invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
        sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupEnd));

        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals("bm-1", records.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
    }

    @Test
    void testLiveBookmarkDrivesPromotionWhenSnapshotRowsHaveNone() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        CommandId sowCommand = mock(CommandId.class);
        CommandId bookmarkCommand = mock(CommandId.class);
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", null, Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-live", Message.Command.Publish));
                    return sowCommand;
                });
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-live"), eq(""), anyLong()))
                .thenReturn(bookmarkCommand);

        task.initialize(context);
        task.start(props);
        List<SourceRecord> records = task.poll();

        assertEquals(2, records.size());
        assertNull(records.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        assertEquals("bm-live", records.get(1).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        verify(ampsClient).unsubscribe(sowCommand);
        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("bm-live"), eq(""), anyLong());
    }

    @Test
    void testSnapshotRowsCarryIncompleteMarkerUntilGroupEnd() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "100");
        AtomicReference<MessageHandler> sowHandler = new AtomicReference<>();
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    sowHandler.set(handler);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> first = task.poll();
        assertEquals(1, first.size());
        assertEquals(RecordConverter.SNAPSHOT_STATE_INCOMPLETE,
                first.get(0).sourceOffset().get(RecordConverter.FIELD_SNAPSHOT_STATE));

        sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupEnd));
        List<SourceRecord> second = task.poll();
        assertEquals(1, second.size());
        assertEquals("bm-2", second.get(0).sourceOffset().get(RecordConverter.FIELD_BOOKMARK));
        assertFalse(second.get(0).sourceOffset().containsKey(RecordConverter.FIELD_SNAPSHOT_STATE));
    }

    @Test
    void testStartRefusesOffsetFromIncompleteSnapshot() {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, "bm-1");
        storedOffset.put(RecordConverter.FIELD_SNAPSHOT_STATE, RecordConverter.SNAPSHOT_STATE_INCOMPLETE);
        when(offsetStorageReader.offset(any())).thenReturn(storedOffset);

        task.initialize(context);
        ConnectException failure = assertThrows(ConnectException.class, () -> task.start(props));
        assertTrue(failure.getCause().getMessage().contains("before the sow_and_subscribe snapshot finished"));
    }

    @Test
    void testStartBookmarkOverridesIncompleteSnapshotOffset() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_START_BOOKMARK_CONF, "explicit-bookmark");
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, "bm-1");
        storedOffset.put(RecordConverter.FIELD_SNAPSHOT_STATE, RecordConverter.SNAPSHOT_STATE_INCOMPLETE);
        lenient().when(offsetStorageReader.offset(any())).thenReturn(storedOffset);
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("explicit-bookmark"), eq(""), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("explicit-bookmark"), eq(""), anyLong());
    }

    @Test
    void testReplayAfterFinishedSnapshotWithoutBookmarkFails() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        AtomicReference<MessageHandler> sowHandler = new AtomicReference<>();
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    sowHandler.set(handler);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", null, Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);
        assertEquals(1, task.poll().size());

        // HAClient replays the snapshot, but no bookmark exists to resume the log from.
        sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupBegin));
        sowHandler.get().invoke(mockMessage("{\"id\":1}", null, Message.Command.SOW));
        sowHandler.get().invoke(mockMessage(null, null, Message.Command.GroupEnd));

        ConnectException failure = assertThrows(ConnectException.class, () -> task.poll());
        assertFalse(failure instanceof RetriableException);
        verify(ampsClient, never()).bookmarkSubscribe(any(MessageHandler.class), anyString(), anyString(),
                any(), anyString(), anyString(), anyLong());
    }

    @Test
    void testDisconnectMidSnapshotFailsPoll() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return commandId;
                });

        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            ArgumentCaptor<ConnectionStateListener> listener =
                    ArgumentCaptor.forClass(ConnectionStateListener.class);
            verify(ampsClient).addConnectionStateListener(listener.capture());

            assertEquals(1, task.poll().size());
            listener.getValue().connectionStateChanged(ConnectionStateListener.Disconnected);

            ConnectException failure = assertThrows(ConnectException.class, () -> task.poll());
            assertFalse(failure instanceof RetriableException);
            assertIncompleteSnapshotLogged(logs, true);
            verify(ampsClient, never()).bookmarkSubscribe(any(MessageHandler.class), anyString(), anyString(),
                    any(), anyString(), anyString(), anyLong());
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testSowAndSubscribeWithStoredBookmarkDoesNotSow() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        String storedBookmark = "15|1711224137000123456|0";
        Map<String, Object> storedOffset = new HashMap<>();
        storedOffset.put(RecordConverter.FIELD_BOOKMARK, storedBookmark);
        when(offsetStorageReader.offset(any())).thenReturn(storedOffset);
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(storedBookmark), eq(""), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-2", Message.Command.Publish));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);
        assertEquals(1, task.poll().size());
        assertTrue(task.poll().isEmpty());

        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq(storedBookmark), eq(""), anyLong());
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                any(), any(), anyInt(), anyInt(), any(), anyLong());
        verify(ampsClient, never()).unsubscribe(any(CommandId.class));
    }

    @Test
    void testSowAndSubscribeWithStartBookmarkDoesNotSow() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_START_BOOKMARK_CONF, "explicit-bookmark");
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("explicit-bookmark"), eq(""), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();
        task.poll();

        verify(ampsClient, times(1)).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), eq(""),
                isNull(), eq("explicit-bookmark"), eq(""), anyLong());
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                any(), any(), anyInt(), anyInt(), any(), anyLong());
    }

    @Test
    void testPollWithSowAndSubscribePreservesFilter() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        props.put(AMPSSourceConnectorConfig.AMPS_FILTER_CONF, "/price > 100");
        props.put(AMPSSourceConnectorConfig.AMPS_POSITION_CONF, "EPOCH");

        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq("/price > 100"),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.SOW));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);
        assertEquals(1, task.poll().size());
        assertTrue(task.poll().isEmpty());

        verify(ampsClient, times(1)).sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), eq("/price > 100"),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), eq(0L));
        verify(ampsClient, never()).sowAndSubscribe(any(MessageHandler.class), anyString(), anyString(),
                anyInt(), anyLong());
        verify(ampsClient, never()).unsubscribe(any(CommandId.class));
    }

    @Test
    void testStopUnsubscribesStandingSowAndSubscribe() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();
        task.stop();

        verify(ampsClient).unsubscribe(commandId);
        verify(ampsClient).close();
    }

    @Test
    void testStopDuringSowAndSubscribeSnapshotLogsIncomplete() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    return commandId;
                });
        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            task.poll();
            task.stop();
            assertIncompleteSnapshotLogged(logs, true);
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testStopAfterSowAndSubscribeGroupEndDoesNotLogIncomplete() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow_and_subscribe");
        when(ampsClient.sowAndSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq(Client.Bookmarks.NOW), eq(0), eq(-1), isNull(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return commandId;
                });
        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            assertEquals(1, task.poll().size());
            task.stop();
            assertIncompleteSnapshotLogged(logs, false);
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testStopDuringSowSnapshotLogsIncomplete() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    return commandId;
                });
        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            assertEquals(1, task.poll().size());
            task.stop();
            assertIncompleteSnapshotLogged(logs, true);
        } finally {
            detachTaskLogs(logs);
        }
    }

    @Test
    void testStopAfterCompletedSowDoesNotLogIncomplete() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer(invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return commandId;
                });
        ListAppender<ILoggingEvent> logs = attachTaskLogs();
        try {
            task.initialize(context);
            task.start(props);
            assertEquals(1, task.poll().size());
            task.stop();
            assertIncompleteSnapshotLogged(logs, false);
        } finally {
            detachTaskLogs(logs);
        }
    }

    private ListAppender<ILoggingEvent> attachTaskLogs() {
        Logger logger = (Logger) LoggerFactory.getLogger(AMPSSourceTask.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private void detachTaskLogs(ListAppender<ILoggingEvent> appender) {
        Logger logger = (Logger) LoggerFactory.getLogger(AMPSSourceTask.class);
        logger.detachAppender(appender);
    }

    private void assertIncompleteSnapshotLogged(ListAppender<ILoggingEvent> logs, boolean expected) {
        boolean found = logs.list.stream().anyMatch(event ->
                event.getLevel() == Level.ERROR
                        && event.getFormattedMessage().contains(
                        "SOW snapshot incomplete for topic 'test-topic'"));
        assertEquals(expected, found);
    }

    @Test
    void testPollWithSowOnly() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(10), eq(0L));
        verify(ampsClient, never()).unsubscribe(commandId);
    }

    @Test
    void testPollWithSowOnlyAndFilter() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.AMPS_FILTER_CONF, "/region = 'US'");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient).sow(any(MessageHandler.class), eq("test-topic"), eq("/region = 'US'"),
                eq(10), eq(0L));
    }

    @Test
    void testPollWithSowOnlyAndSeparateQueryBatchSize() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.AMPS_SOW_QUERY_BATCH_SIZE_CONF, "7");
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);
        task.poll();

        verify(ampsClient).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(7), eq(0L));
    }

    @Test
    void testPollWithSowOnlyStreamsSnapshotAcrossMultiplePolls() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= 5; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", "bookmark-" + i, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> firstPoll = task.poll();
        List<SourceRecord> secondPoll = task.poll();
        List<SourceRecord> thirdPoll = task.poll();
        List<SourceRecord> fourthPoll = task.poll();

        assertEquals(2, firstPoll.size());
        assertEquals(2, secondPoll.size());
        assertEquals(1, thirdPoll.size());
        assertTrue(fourthPoll.isEmpty());

        verify(ampsClient, times(1)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(2), eq(0L));
    }

    @Test
    void testSowEofThenNextPollStartsAnotherSnapshot() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        AtomicInteger sowCalls = new AtomicInteger();
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    int run = sowCalls.incrementAndGet();
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= 5; i++) {
                            handler.invoke(mockMessage("{\"run\":" + run + ",\"id\":" + i + "}",
                                    "bookmark-" + run + "-" + i, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> firstPoll = task.poll();
        List<SourceRecord> secondPoll = task.poll();
        List<SourceRecord> thirdPoll = task.poll();
        assertEquals(2, firstPoll.size());
        assertEquals(2, secondPoll.size());
        assertEquals(1, thirdPoll.size());
        verify(ampsClient, times(1)).close();

        assertTrue(task.poll().isEmpty(), "the poll after the last data batch is EOF");
        verify(ampsClient, times(1)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(2), eq(0L));
        verify(clientFactory, times(1)).create(any());

        Thread.sleep(5);
        List<SourceRecord> rearmed = task.poll();
        assertEquals(2, rearmed.size());
        verify(ampsClient, times(2)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(2), eq(0L));
        verify(clientFactory, times(2)).create(any());

        long firstSnapshot = (Long) firstPoll.get(0).sourceOffset().get(RecordConverter.FIELD_SNAPSHOT);
        long secondSnapshot = (Long) rearmed.get(0).sourceOffset().get(RecordConverter.FIELD_SNAPSHOT);
        assertTrue(secondSnapshot > firstSnapshot, "a re-armed snapshot needs a later snapshot id");
    }

    @Test
    void testSowRearmRetriesWhenReconnectFails() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        AtomicInteger creates = new AtomicInteger();
        when(clientFactory.create(any())).thenAnswer(invocation -> {
            if (creates.incrementAndGet() == 2) {
                throw new ConnectException("logon failed");
            }
            return ampsClient;
        });

        AtomicInteger sowCalls = new AtomicInteger();
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    int run = sowCalls.incrementAndGet();
                    handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                    handler.invoke(mockMessage("{\"run\":" + run + "}", "bookmark-" + run, Message.Command.SOW));
                    handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        assertEquals(1, task.poll().size());
        assertTrue(task.poll().isEmpty());
        RetriableException reconnectFailure = assertThrows(RetriableException.class, () -> task.poll());
        assertInstanceOf(ConnectException.class, reconnectFailure.getCause());
        verify(ampsClient, times(1)).sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong());

        assertEquals(1, task.poll().size(), "a failed reconnect must not drop the re-arm");
        verify(ampsClient, times(2)).sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong());
        verify(clientFactory, times(3)).create(any());
    }

    @Test
    void testSowReaderBlocksWhenPrefetchBufferIsFull() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        java.util.concurrent.CountDownLatch allDelivered = new java.util.concurrent.CountDownLatch(1);
        int total = 20;
        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= total; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", "bookmark-" + i, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                        allDelivered.countDown();
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        int received = 0;
        for (int i = 0; i < 20; i++) {
            List<SourceRecord> batch = task.poll();
            received += batch.size();
            if (received >= total) {
                break;
            }
        }

        assertEquals(total, received);
        assertTrue(allDelivered.await(5, java.util.concurrent.TimeUnit.SECONDS),
                "AMPS reader must resume after poll drains the bounded SOW buffer");
        assertTrue(task.poll().isEmpty());
    }

    @Test
    void testSowStopThenStartOnSameTaskIssuesSecondSnapshot() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        CommandId sowCmd1 = mock(CommandId.class);
        CommandId sowCmd2 = mock(CommandId.class);
        AtomicInteger sowCalls = new AtomicInteger();

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    int n = sowCalls.incrementAndGet();
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        handler.invoke(mockMessage("{\"run\":" + n + "}", "bookmark-run-" + n, Message.Command.SOW));
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return n == 1 ? sowCmd1 : sowCmd2;
                });

        task.initialize(context);
        task.start(props);
        assertEquals(1, task.poll().size());
        assertTrue(task.poll().isEmpty());

        task.stop();

        task.start(props);
        List<SourceRecord> afterRestart = task.poll();
        assertEquals(1, afterRestart.size());
        assertTrue(task.poll().isEmpty());

        verify(ampsClient, times(2)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(10), eq(0L));
    }

    @Test
    void testSowClosesClientOnceSnapshotIsFullyDrained() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= 3; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", null, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        assertEquals(2, task.poll().size());
        verify(ampsClient, never()).close();

        assertEquals(1, task.poll().size());
        verify(ampsClient, times(1)).close();

        assertTrue(task.poll().isEmpty());
        verify(ampsClient, times(1)).close();
        verify(ampsClient, times(1)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(2), eq(0L));
    }

    @Test
    void testSowRecordsCarryUniqueOffsetsWithoutBookmarks() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= 3; i++) {
                            handler.invoke(mockMessage("{\"id\":" + i + "}", null, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> records = task.poll();
        assertEquals(3, records.size());

        Set<Map<String, ?>> offsets = new HashSet<>();
        long expectedSequence = 0L;
        Long snapshotId = null;
        for (SourceRecord record : records) {
            Map<String, ?> offset = record.sourceOffset();
            assertFalse(offset.isEmpty(), "SOW records must carry a non-empty offset");

            long snapshot = (Long) offset.get(RecordConverter.FIELD_SNAPSHOT);
            long sequence = (Long) offset.get(RecordConverter.FIELD_SEQUENCE);
            if (snapshotId == null) {
                snapshotId = snapshot;
            }
            assertEquals(snapshotId, snapshot, "rows in one snapshot share a snapshot id");
            assertEquals(expectedSequence, sequence, "sequence restarts at 0 for each snapshot");
            expectedSequence++;
            offsets.add(offset);
        }
        assertEquals(3, offsets.size(), "each snapshot row needs a distinct offset");
    }

    @Test
    void testSubscribeModeOffsetRemainsBookmarkOnly() throws Exception {
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bookmark-1", Message.Command.Publish));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(Collections.singletonMap(RecordConverter.FIELD_BOOKMARK, "bookmark-1"),
                records.get(0).sourceOffset());
    }

    @Test
    void testSowPauseThenResumeContinuesSameSnapshot() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        for (int i = 1; i <= 3; i++) {
                            handler.invoke(mockMessage("{\"cycle\":" + i + "}", "bm-" + i, Message.Command.SOW));
                        }
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);
        assertEquals(2, task.poll().size());

        assertEquals(1, task.poll().size());
        assertTrue(task.poll().isEmpty());

        verify(ampsClient, times(1)).sow(any(MessageHandler.class), eq("test-topic"), eq(""),
                eq(2), eq(0L));
    }

    @Test
    void testSowSnapshotAcceptsPublishCommandRecords() throws Exception {
        props.put(AMPSSourceConnectorConfig.AMPS_SUBSCRIPTION_TYPE_CONF, "sow");
        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "10");
        props.put(AMPSSourceConnectorConfig.POLL_TIMEOUT_MS_CONF, "500");

        when(ampsClient.sow(any(MessageHandler.class), eq("test-topic"), anyString(), anyInt(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        handler.invoke(mockMessage(null, null, Message.Command.GroupBegin));
                        handler.invoke(mockMessage("{\"id\":1}", "bm-1", Message.Command.Publish));
                        handler.invoke(mockMessage("{\"id\":2}", "bm-2", Message.Command.Publish));
                        handler.invoke(mockMessage(null, null, Message.Command.GroupEnd));
                    }).start();
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        assertEquals(2, task.poll().size());
    }

    @Test
    void testMultiplePollsUpdateBookmark() throws Exception {
        // First poll returns message with bookmark-1
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(), 
                isNull(), eq(Client.Bookmarks.NOW), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    new Thread(() -> {
                        Message mockMessage = mock(Message.class);
                        when(mockMessage.getData()).thenReturn("{\"id\":1}");
                        when(mockMessage.getBookmark()).thenReturn("bookmark-1");
                        handler.invoke(mockMessage);
                    }).start();
                    return commandId;
                });

        // Second poll should use bookmark-1 (updated from first poll)
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(), 
                isNull(), eq("bookmark-1"), anyString(), anyLong()))
                .thenReturn(commandId);

        task.initialize(context);
        task.start(props);

        // First poll
        List<SourceRecord> records1 = task.poll();
        assertEquals(1, records1.size());

        // Second poll should use updated bookmark
        task.poll();

        // Verify second poll used bookmark-1
        verify(ampsClient).bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), eq("bookmark-1"), anyString(), anyLong());
    }

    /**
     * The bookmark store logs every delivered message and each poll subscribes under a new
     * subscription id, so without discard+purge the store grows for the life of the task and
     * eventually exhausts the extractor heap.
     */
    @Test
    void testPollDiscardsAndPurgesBookmarkStore() throws Exception {
        BookmarkStore bookmarkStore = mock(BookmarkStore.class);
        when(ampsClient.getBookmarkStore()).thenReturn(bookmarkStore);

        props.put(AMPSSourceConnectorConfig.BATCH_SIZE_CONF, "2");
        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    // Third message arrives past batch.size and is dropped, but AMPS still
                    // logged it, so it must be discarded too.
                    handler.invoke(mockMessage("{\"id\":1}", "bookmark-1", Message.Command.Publish));
                    handler.invoke(mockMessage("{\"id\":2}", "bookmark-2", Message.Command.Publish));
                    handler.invoke(mockMessage("{\"id\":3}", "bookmark-3", Message.Command.Publish));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        task.poll();
        verify(bookmarkStore, times(3)).discard(any(Message.class));
        verify(bookmarkStore, times(1)).purge();

        task.poll();
        verify(bookmarkStore, times(2)).purge();
    }

    /**
     * A bookmark store that rejects discard must not fail the poll; the per-poll purge recovers.
     */
    @Test
    void testPollSurvivesBookmarkStoreDiscardFailure() throws Exception {
        BookmarkStore bookmarkStore = mock(BookmarkStore.class);
        when(ampsClient.getBookmarkStore()).thenReturn(bookmarkStore);
        doThrow(new RuntimeException("store unavailable")).when(bookmarkStore).discard(any(Message.class));

        when(ampsClient.bookmarkSubscribe(any(MessageHandler.class), eq("test-topic"), anyString(),
                isNull(), anyString(), anyString(), anyLong()))
                .thenAnswer((Answer<CommandId>) invocation -> {
                    MessageHandler handler = invocation.getArgument(0);
                    handler.invoke(mockMessage("{\"id\":1}", "bookmark-1", Message.Command.Publish));
                    return commandId;
                });

        task.initialize(context);
        task.start(props);

        List<SourceRecord> records = task.poll();

        assertEquals(1, records.size(), "discard failure must not drop the record");
        verify(bookmarkStore).purge();
    }
}
