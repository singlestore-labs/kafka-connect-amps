package com.singlestore.kafka.connect.amps;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordConverterTest {

    private RecordConverter converter;
    private AMPSSourceConnectorConfig config;

    @BeforeEach
    void setUp() {
        Map<String, String> props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, "tcp://localhost:9007/amps/json");
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "test-topic");
        props.put(AMPSSourceConnectorConfig.KAFKA_TOPIC_PREFIX_CONF, "amps-");
        
        config = new AMPSSourceConnectorConfig(props);
        converter = new RecordConverter(config);
    }

    private RecordConverter createConverter(String uri) {
        Map<String, String> props = new HashMap<>();
        props.put(AMPSSourceConnectorConfig.AMPS_URI_CONF, uri);
        props.put(AMPSSourceConnectorConfig.AMPS_TOPIC_CONF, "test-topic");
        props.put(AMPSSourceConnectorConfig.KAFKA_TOPIC_PREFIX_CONF, "amps-");
        return new RecordConverter(new AMPSSourceConnectorConfig(props));
    }

    @Test
    void testConvertCreatesValidSourceRecord() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        assertNotNull(record);
        assertEquals("amps-test-topic", record.topic());
        assertNull(record.kafkaPartition());
        assertEquals("com.singlestore.kafka.connect.amps.AMPSValue", record.valueSchema().name());
        assertEquals(2, record.valueSchema().version());
    }

    @Test
    void testConvertSetsCorrectKey() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct key = (Struct) record.key();
        assertEquals("test-topic", key.getString(RecordConverter.FIELD_TOPIC));
    }

    @Test
    void testConvertSetsCorrectValue() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123,\"name\":\"test\"}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();

        // Check metadata fields (correlationId, sowKey, command removed)
        assertEquals("bookmark-abc", value.getString(RecordConverter.FIELD_BOOKMARK));
        assertEquals("test-topic", value.getString(RecordConverter.FIELD_TOPIC));
        assertNotNull(value.getInt64(RecordConverter.FIELD_TIMESTAMP));

        assertArrayEquals(
                "{\"id\":123,\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConvertSetsSourcePartition() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Map<String, Object> partition = (Map<String, Object>) record.sourcePartition();
        assertEquals("test-topic", partition.get(RecordConverter.FIELD_TOPIC));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConvertSetsSourceOffset() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        assertEquals("bookmark-abc", offset.get(RecordConverter.FIELD_BOOKMARK));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConvertHandlesNullBookmark() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                null
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();
        assertNull(value.getString(RecordConverter.FIELD_BOOKMARK));
        
        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        assertTrue(offset.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFinishedSnapshotWithoutBookmarkKeepsRecoveryState() {
        AMPSMessageData messageData = new AMPSMessageData("{\"id\":123}", null, SnapshotState.INCOMPLETE)
                .asSnapshotComplete();

        SourceRecord record = converter.convert(messageData);

        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        assertEquals(RecordConverter.SNAPSHOT_STATE_FINISHED_WITHOUT_BOOKMARK,
                offset.get(RecordConverter.FIELD_SNAPSHOT_STATE));
        assertFalse(offset.containsKey(RecordConverter.FIELD_BOOKMARK));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFinishedSnapshotWithBookmarkStaysResumable() {
        AMPSMessageData messageData = new AMPSMessageData("{\"id\":123}", "bookmark-abc", SnapshotState.INCOMPLETE)
                .asSnapshotComplete();

        SourceRecord record = converter.convert(messageData);

        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        assertEquals("bookmark-abc", offset.get(RecordConverter.FIELD_BOOKMARK));
        assertFalse(offset.containsKey(RecordConverter.FIELD_SNAPSHOT_STATE));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testOffsetBookmarkIsStoredWhenTheMessageHasNone() {
        AMPSMessageData messageData = new AMPSMessageData("{\"id\":123}", null, "bm-earlier", SnapshotState.NONE);

        SourceRecord record = converter.convert(messageData);

        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        assertEquals("bm-earlier", offset.get(RecordConverter.FIELD_BOOKMARK));
        assertNull(((org.apache.kafka.connect.data.Struct) record.value()).get(RecordConverter.FIELD_BOOKMARK));
    }

    @Test
    void testConvertHandlesNullData() {
        AMPSMessageData messageData = new AMPSMessageData(
                null,
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();
        assertNull(value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testConvertHandlesEmptyData() {
        AMPSMessageData messageData = new AMPSMessageData(
                "",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();
        assertNull(value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testConvertHandlesInvalidJson() {
        AMPSMessageData messageData = new AMPSMessageData(
                "not valid json",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();
        assertArrayEquals(
                "not valid json".getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testConvertNvfixEmitsOpaqueBytes() {
        RecordConverter nvfixConverter = createConverter("tcp://localhost:9007/amps/nvfix");
        String payload = "35=D|55=AAPL|54=1|38=100";
        AMPSMessageData messageData = new AMPSMessageData(
                payload,
                "bookmark-nvfix"
        );

        SourceRecord record = nvfixConverter.convert(messageData);

        Struct value = (Struct) record.value();
        assertArrayEquals(payload.getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testConvertNvfixPreservesSohInOpaqueBytes() {
        RecordConverter nvfixConverter = createConverter("tcp://localhost:9007/amps/nvfix");
        String payload = "35=D\u000155=MSFT\u000154=2\u000138=250";
        AMPSMessageData messageData = new AMPSMessageData(payload, "bookmark-nvfix");

        SourceRecord record = nvfixConverter.convert(messageData);

        Struct value = (Struct) record.value();
        assertArrayEquals(payload.getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testConvertNvfixDoesNotParseMalformedPayload() {
        RecordConverter nvfixConverter = createConverter("tcp://localhost:9007/amps/nvfix");
        String payload = "35=D|BADFIELD|38=100";
        AMPSMessageData messageData = new AMPSMessageData(
                payload,
                "bookmark-nvfix"
        );

        SourceRecord record = nvfixConverter.convert(messageData);

        Struct value = (Struct) record.value();
        assertArrayEquals(payload.getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testValueSchemaDoesNotEmitDataRaw() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        assertNull(record.valueSchema().field("data_raw"));
    }

    @Test
    void testConvertKeepsNestedJsonVerbatim() {
        String nestedJson = "{\"symbol\":\"AAPL\",\"trade\":{\"price\":150.5,\"size\":100}}";
        AMPSMessageData messageData = new AMPSMessageData(
                nestedJson,
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        Struct value = (Struct) record.value();
        assertArrayEquals(nestedJson.getBytes(StandardCharsets.UTF_8),
                value.getBytes(RecordConverter.FIELD_DATA));
    }

    @Test
    void testKeySchemaHasCorrectStructure() {
        assertNotNull(RecordConverter.KEY_SCHEMA);
        assertEquals("com.singlestore.kafka.connect.amps.AMPSKey", RecordConverter.KEY_SCHEMA.name());
        assertNotNull(RecordConverter.KEY_SCHEMA.field(RecordConverter.FIELD_TOPIC));
    }

    @Test
    void testValueSchemaHasCorrectStructure() {
        AMPSMessageData messageData = new AMPSMessageData(
                "{\"id\":123}",
                "bookmark-abc"
        );

        SourceRecord record = converter.convert(messageData);

        assertEquals("com.singlestore.kafka.connect.amps.AMPSValue", record.valueSchema().name());
        assertTrue(record.value() instanceof Struct, "Value should be a Struct");
        assertNotNull(record.valueSchema().field(RecordConverter.FIELD_BOOKMARK));
        assertNotNull(record.valueSchema().field(RecordConverter.FIELD_TIMESTAMP));
        assertNotNull(record.valueSchema().field(RecordConverter.FIELD_DATA));
        assertEquals(Schema.Type.BYTES,
                record.valueSchema().field(RecordConverter.FIELD_DATA).schema().type());
        assertNotNull(record.valueSchema().field(RecordConverter.FIELD_TOPIC));
    }
}
