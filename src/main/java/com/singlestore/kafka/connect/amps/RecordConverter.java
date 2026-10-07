package com.singlestore.kafka.connect.amps;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Converts AMPSMessageData objects to Kafka Connect SourceRecords.
 * Both the key and the value carry an explicit schema. Message payloads are emitted
 * as opaque UTF-8 bytes so the value schema stays fixed regardless of AMPS message type.
 */
public class RecordConverter {

    private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

    public static final String FIELD_TOPIC = "topic";
    public static final String FIELD_BOOKMARK = "bookmark";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_DATA = "data";

    /**
     * Offset fields used only in SOW mode. SOW query results carry no bookmark, so the
     * snapshot id plus a per-record sequence give every row a unique, increasing offset.
     * Without them a re-run snapshot is indistinguishable from the previous one.
     */
    public static final String FIELD_SNAPSHOT = "snapshot";
    public static final String FIELD_SEQUENCE = "sequence";

    /**
     * Key schema - uses AMPS topic for Kafka partitioning.
     */
    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("com.singlestore.kafka.connect.amps.AMPSKey")
            .doc("Key for AMPS messages, used for Kafka partitioning")
            .field(FIELD_TOPIC, SchemaBuilder.string()
                    .doc("The AMPS topic the message was received from")
                    .build())
            .build();

    private final AMPSSourceConnectorConfig config;
    private final Schema valueSchema;
    /**
     * Room reserved for per-record sequence numbers inside a single snapshot, so the
     * sequence stays globally monotonic across task restarts.
     */
    private static final long SNAPSHOT_SEQUENCE_SPACE = 1_000_000L;

    private final boolean sowSnapshotMode;
    private final long snapshotId;
    private final AtomicLong snapshotSequence;

    public RecordConverter(AMPSSourceConnectorConfig config) {
        this.config = config;
        this.valueSchema = buildValueSchema();
        this.sowSnapshotMode = config.ampsSubscriptionType == SubscriptionType.SOW;
        this.snapshotId = System.currentTimeMillis();
        this.snapshotSequence = new AtomicLong(snapshotId * SNAPSHOT_SEQUENCE_SPACE);
    }

    /**
     * Builds the value schema. The AMPS payload is opaque bytes, so
     * the configured AMPS message type does not alter the Connect schema.
     *
     * @return the value schema emitted for every message type
     */
    public static Schema buildValueSchema() {
        return SchemaBuilder.struct()
                .name("com.singlestore.kafka.connect.amps.AMPSValue")
                .version(2)
                .doc("AMPS message payload and delivery metadata")
                .field(FIELD_BOOKMARK, Schema.OPTIONAL_STRING_SCHEMA)
                .field(FIELD_TIMESTAMP, Schema.INT64_SCHEMA)
                .field(FIELD_DATA, SchemaBuilder.bytes()
                        .optional()
                        .doc("Opaque AMPS message payload encoded as UTF-8 bytes")
                        .build())
                .field(FIELD_TOPIC, Schema.STRING_SCHEMA)
                .build();
    }

    /**
     * @return the value schema this converter attaches to every record
     */
    public Schema valueSchema() {
        return valueSchema;
    }

    /**
     * Converts an AMPSMessageData to a Kafka Connect SourceRecord.
     *
     * @param ampsTopic   The AMPS topic the message was received from
     * @param messageData The AMPS message data (already copied from the original Message)
     * @return A SourceRecord ready for Kafka
     */
    public SourceRecord convert(String ampsTopic, AMPSMessageData messageData) {
        String kafkaTopic = config.kafkaTopicPrefix + ampsTopic;

        String bookmark = messageData.getBookmark();
        String dataString = messageData.getData();
        byte[] data = dataString == null || dataString.isEmpty()
                ? null
                : dataString.getBytes(StandardCharsets.UTF_8);

        // Build key with schema (for partitioning)
        Struct key = new Struct(KEY_SCHEMA)
                .put(FIELD_TOPIC, ampsTopic);

        Struct value = new Struct(valueSchema)
                .put(FIELD_BOOKMARK, bookmark)
                .put(FIELD_TIMESTAMP, System.currentTimeMillis())
                .put(FIELD_DATA, data)
                .put(FIELD_TOPIC, ampsTopic);

        Map<String, Object> sourcePartition = ImmutableMap.of(FIELD_TOPIC, ampsTopic);
        Map<String, Object> sourceOffset = buildSourceOffset(bookmark);

        log.trace("Converting AMPS message from topic '{}' with bookmark '{}'", ampsTopic, bookmark);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                kafkaTopic,
                null,           // partition
                KEY_SCHEMA,
                key,
                valueSchema,
                value
        );
    }

    /**
     * Builds the source offset. Subscribe modes keep the bookmark-only offset so resume
     * behaviour is unchanged. SOW mode always emits a unique offset, since a snapshot row
     * has no bookmark and an empty offset makes every snapshot look identical.
     *
     * <p>The SOW sequence is seeded from the snapshot id rather than restarting at 1, so a
     * later snapshot always reports higher offsets than an earlier one. A consumer that
     * tracks a high-water mark would otherwise discard every row of a re-run snapshot.
     *
     * @param bookmark the AMPS bookmark, or null when the message carries none
     * @return the source offset map for this record
     */
    private Map<String, Object> buildSourceOffset(String bookmark) {
        if (!sowSnapshotMode) {
            return bookmark != null
                    ? ImmutableMap.of(FIELD_BOOKMARK, bookmark)
                    : ImmutableMap.of();
        }

        long sequence = snapshotSequence.incrementAndGet();
        if (bookmark != null) {
            return ImmutableMap.of(
                    FIELD_BOOKMARK, bookmark,
                    FIELD_SNAPSHOT, snapshotId,
                    FIELD_SEQUENCE, sequence);
        }
        return ImmutableMap.of(
                FIELD_SNAPSHOT, snapshotId,
                FIELD_SEQUENCE, sequence);
    }

}
