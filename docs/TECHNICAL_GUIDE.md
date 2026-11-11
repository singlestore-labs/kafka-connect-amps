# Technical Guide: AMPS Source Connector

This is the technical reference for the Kafka Connect AMPS Source Connector (version 0.1.0, pre-GA). It explains how the connector reads AMPS topics, how it tracks position, what records it emits, what it requires, and what it does not do.

For build steps, the full configuration reference, and the SingleStore pipeline SQL example, see [README.md](../README.md). This guide links to the README rather than repeating the configuration table.

## Table of Contents

1. [Overview](#1-overview)
2. [Scope, Packaging, and Limits](#2-scope-packaging-and-limits)
3. [Architecture](#3-architecture)
4. [Connector and Task Contract](#4-connector-and-task-contract)
5. [Message Flow](#5-message-flow)
6. [Bookmark-Based Offset Management](#6-bookmark-based-offset-management)
7. [Subscription Types](#7-subscription-types)
8. [Record Conversion](#8-record-conversion)
9. [Authentication and TLS](#9-authentication-and-tls)
10. [SingleStore Pipeline Behavior](#10-singlestore-pipeline-behavior)
11. [Error Handling and Recovery](#11-error-handling-and-recovery)
12. [Performance and Resource Management](#12-performance-and-resource-management)
13. [Example Configurations](#13-example-configurations)
14. [Requirements Reference](#14-requirements-reference)
15. [Code Traceability and Support](#15-code-traceability-and-support)
16. [Summary](#16-summary)

---

## 1. Overview

The connector reads messages from one AMPS topic and emits them as Kafka Connect `SourceRecord`s. It runs in Apache Kafka Connect or as a SingleStore custom pipeline source. It is source only and does not publish to AMPS.

`subscribe` uses **subscribe-on-demand polling**. Unlike push-based connectors that keep a standing subscription and an internal queue, each `poll()`:

1. Subscribes from the current bookmark.
2. Collects up to `batch.size` messages.
3. Unsubscribes.
4. Returns the batch as records and advances the in-memory bookmark.

This keeps memory bounded, including during large EPOCH replays. `sow_and_subscribe` opens one subscription and leaves it open (see [7.3](#73-sow_and_subscribe)). `sow` runs one SOW query and drains it across successive polls (see [7.4](#74-sow)).

---

## 2. Scope, Packaging, and Limits

### Packaging

| Item | Value |
|------|-------|
| Connector class | `com.singlestore.kafka.connect.amps.AMPSSourceConnector` |
| Short name | `AMPSSourceConnector` (loaded through the ServiceLoader entry in `META-INF/services/org.apache.kafka.connect.source.SourceConnector`) |
| Artifact | `kafka-connect-amps-0.1.0.jar` (shaded, with the AMPS Java client included) |
| Java | 11 or later |
| AMPS client | 5.3.2.0, bundled |
| Kafka Connect API | `connect-api` 3.5.1, provided by the worker |

### Limits

- **One topic per connector instance.** `tasks.max` greater than 1 does not create more AMPS tasks. All records share one source partition, so two tasks must not emit the same partition.
- **Ordering.** Messages are processed in AMPS subscription order within the single task.
- **Transaction log.** Bookmark resume (`subscribe`, `sow_and_subscribe`, EPOCH replay) needs the AMPS transaction log.
- **SOW.** `sow` and `sow_and_subscribe` need SOW configured on the topic.
- **Payloads.** The payload is opaque UTF-8 bytes. Only the `json` and `nvfix` URI segments are supported.
- **Single URI.** Comma-separated HA URI lists are not supported.

### Not included

- Writing (sinking) data to AMPS
- AMPS queues and queue acknowledgements
- Multiple tasks over one topic (hash, `MOD`, or `taskFilter` splits)
- AMPS command options such as projection, grouping, conflation, and timestamp, and the `cmdTypeFilter` bitmask
- Topic-name regular expressions
- Message types other than JSON and NVFIX (XML, protobuf, binary)
- Raw byte values and Kafka headers (`eventHeaders`)
- Overriding `commit()` or `commitRecord()`. The host persists offsets after a successful batch.
- Dynamic connector reconfiguration after `start()`
- Compatibility with 60East property names (`uri`, `ampsTopic`, `clientName`, `maxBatch`, and similar). This product is not a drop-in replacement for the 60East connector.

---

## 3. Architecture

```
Kafka Connect worker or SingleStore pipeline host
  AMPSSourceConnector   validates config, returns exactly one task config
    AMPSSourceTask      poll loop, bookmark tracking
      HAClient          AMPS connection, with MemoryBookmarkStore
      RecordConverter   AMPSMessageData -> SourceRecord
      AMPSMessageData   copied payload and bookmark
```

| Class | Responsibility |
|-------|----------------|
| `AMPSSourceConnector` | Entry point. Validates config and creates the task configuration. |
| `AMPSSourceTask` | Poll loop, subscription lifecycle, and bookmark tracking. |
| `AMPSSourceConnectorConfig` | Configuration parsing and validation. |
| `AMPSClientFactoryImpl` | Creates the `HAClient` with `MemoryBookmarkStore`, and applies TLS and auth settings. |
| `AMPSMessageData` | Immutable copy of the payload and bookmark from one AMPS message. |
| `RecordConverter` | Converts AMPS messages to Kafka `SourceRecord`s and builds keys, values, and offsets. |

---

## 4. Connector and Task Contract

### Connector

- Validates and stores properties in `start(Map<String, String>)` through `AMPSSourceConnectorConfig`.
- Returns `AMPSSourceTask` from `taskClass()` and a `ConfigDef` from `config()`.
- Returns **exactly one** task configuration from `taskConfigs(int maxTasks)`, even when `tasks.max` is greater than 1.
- Performs log-only cleanup in `stop()`.
- Does not monitor AMPS or reconfigure tasks after `start()`.

### Task

- `start()` parses configuration, opens the AMPS `HAClient`, and initializes the resume position.
- `poll()` returns up to `batch.size` records, or an empty list.
- `stop()` cancels any in-flight SOW command and closes the AMPS client.
- `commit()` and `commitRecord()` are not overridden. Offset persistence is left to the host after a successful batch.

---

## 5. Message Flow

### 5.1 Poll cycle for `subscribe`

1. If the task is not running, `poll()` returns an empty list.
2. In `sow` mode, `poll()` calls `pollFullSowSnapshot()` (see [7.4](#74-sow)). In `sow_and_subscribe` mode, `poll()` calls `pollStandingSubscription()` (see [7.3](#73-sow_and_subscribe)). Otherwise it calls `pollShortLivedSubscription()`.
3. Create the batch list, an `AtomicReference` for the last bookmark (initialized to `currentBookmark`), and an `AtomicLong` for the time of the last message.
4. Create the `MessageHandler` callback.
5. Call `subscribeFromBookmark(currentBookmark, handler)`.
6. Wait in `waitForShortLivedBatch`. Return when any of these is true:
   - The batch is full (`batch.size`).
   - At least one record arrived, and AMPS has been quiet for `poll.idle.ms`.
   - `poll.timeout.ms` elapsed with no records.
7. In a `finally` block, unsubscribe (failures are logged) and purge the bookmark store.
8. Convert the batch to `SourceRecord`s.
9. If there are records, set `currentBookmark` to the last bookmark collected.
10. Return the records.

If the subscribe call fails with an `AMPSException`, the task throws `RetriableException` and leaves `currentBookmark` unchanged. The retry subscribes from the same position.

### 5.2 MessageHandler callback

AMPS calls the handler on its own thread for each delivered message. The handler copies the message and discards it from the bookmark store:

```java
public void invoke(Message message) {
    synchronized (pollWaitLock) {
        if (batch.size() < config.batchSize) {
            // Copy immediately: AMPS reuses Message objects
            AMPSMessageData data = copyMessage(message);
            batch.add(data);
            if (data.getBookmark() != null) {
                lastBookmarkRef.set(data.getBookmark());
            }
            lastMessageAtMs.set(System.currentTimeMillis());
        }
        // Messages past batch.size are not kept, but still leave the bookmark store
        discardFromBookmarkStore(message);
        pollWaitLock.notifyAll();
    }
}
```

**Critical detail:** AMPS reuses `Message` objects between callbacks. The payload and bookmark must be copied before the callback returns.

---

## 6. Bookmark-Based Offset Management

### 6.1 What is an AMPS bookmark?

An AMPS bookmark identifies a message's position in the transaction log. Example:

```
3850692670110263676|1774627282001110361|
```

Bookmarks give the connector:

- **Resumable subscriptions.** Start reading from where the last batch ended.
- **At-least-once delivery.** Replay from a known position after a failure.
- **EPOCH replay.** Read all historical messages from the beginning of the log.

### 6.2 Start position

On task start, `subscribe` chooses the starting bookmark in this order:

1. **`amps.start.bookmark`** if it is non-empty. It overrides stored offsets and `amps.position`.
2. **The stored offset** for `{ "topic": "<amps.topic>" }` from Kafka Connect offset storage.
3. **`amps.position`**: `EPOCH` (beginning of the transaction log, the default) or `LATEST` (`NOW`, new messages only).

`sow_and_subscribe` uses steps 1 and 2 only. With no start bookmark and no stored offset, the first command is a current SOW and does not send `amps.position`. `EPOCH` and `NOW` are subscribe sentinels. On `sow_and_subscribe`, AMPS reads a bookmark header as a historical SOW point.

`sow` ignores this sequence and always reads the current snapshot from the beginning.

```java
// AMPSSourceTask.initializeBookmark()
if (config.ampsSubscriptionType == SubscriptionType.SOW) {
    currentBookmark = null;                          // SOW ignores bookmarks
    return;
}
if (config.ampsStartBookmark != null && !config.ampsStartBookmark.isEmpty()) {
    currentBookmark = config.ampsStartBookmark;      // 1. explicit override
    return;
}
Map<String, Object> sourcePartition = Collections.singletonMap("topic", ampsTopic);
Map<String, Object> lastOffset = context.offsetStorageReader().offset(sourcePartition);
if (lastOffset != null && !lastOffset.isEmpty()) {
    currentBookmark = (String) lastOffset.get("bookmark");   // 2. stored offset
} else if (config.ampsSubscriptionType == SubscriptionType.SOW_AND_SUBSCRIBE) {
    currentBookmark = null;                          // current SOW, no bookmark header
} else {
    currentBookmark = config.ampsPosition == Position.EPOCH  // 3. amps.position (subscribe)
            ? Client.Bookmarks.EPOCH
            : Client.Bookmarks.NOW;
}
```

### 6.3 Offset commit flow

1. `poll()` returns records. Each record carries `sourcePartition` `{ "topic": "<amps.topic>" }` and `sourceOffset` `{ "bookmark": "<bookmark>" }`.
2. The host (Kafka Connect or SingleStore) writes the records to Kafka or the destination table in one transaction.
3. The host then commits the offsets.
4. On the next start, the stored offset is read back by `initializeBookmark()`.

The task tracks `currentBookmark` itself and passes it explicitly to each subscribe. It never recovers its position from the AMPS bookmark store.

### 6.4 Restart recovery

On task restart, the connector resumes from the last committed offset:

1. Kafka Connect calls `start()` with the task configuration.
2. `initializeBookmark()` reads the last committed offset from offset storage.
3. The first `poll()` subscribes from that bookmark.
4. Processing continues from the last committed position.

---

## 7. Subscription Types

### 7.1 Summary

| Mode | AMPS call | Uses bookmark | Behavior | Fallback on `InvalidTopicException` |
|------|-----------|---------------|----------|-------------------------------------|
| `subscribe` (default) | `bookmarkSubscribe(...)` | Yes | Transaction-log replay, then live | `subscribe(handler, topic[, filter], 0L)` |
| `sow_and_subscribe` | `sowAndSubscribe(...)` on the first start; `bookmarkSubscribe(...)` when a bookmark is already stored | No on the first SOW. Yes when resuming | One current SOW snapshot, then live updates on the same subscription. The first command omits the bookmark header. A later start resumes the log and does not request another SOW | `subscribe` when resuming |
| `sow` | `sow(...)` | No | One SOW snapshot, then end-of-file | None |

`amps.filter` applies to every mode, so AMPS evaluates the content filter on the server.

### 7.2 `subscribe`

```java
ampsClient.bookmarkSubscribe(handler, topic, filter, null, bookmark, "", 0L);
```

- Reads messages from the **transaction log** starting at the bookmark.
- Includes **historical messages** when the bookmark is in the past.
- Continues with **live messages** after catching up.
- Best for event streaming and change data capture.

### 7.3 `sow_and_subscribe`

```java
ampsClient.sowAndSubscribe(handler, topic, filter, 0, 0L);
```

The first command omits the bookmark header. AMPS treats that header as a historical SOW point, not as a current snapshot followed by live updates. With the default `amps.position` of `EPOCH`, the header value would be `Client.Bookmarks.EPOCH` (`0`), which can reject the command or return an empty snapshot plus a full transaction-log replay. `NOW` (`0|1|`) is the same kind of sentinel. An older options-string form (`bookmark=...`) never applied the value either: AMPS rejected the options and the client fell back to this call.

`0` is the SOW batch size. The client uses unlimited top-N.

The first `poll()` opens this command and later polls drain it. Messages that arrive between polls sit in a buffer capped at `batch.size * 2`. `GroupBegin` and `GroupEnd` are not emitted.

HAClient keeps that command and sends it again after a disconnect. Sending `sow_and_subscribe` again delivers another full snapshot, and those rows would be written as normal records. After `GroupEnd`, once a bookmark has been seen, the connector unsubscribes and opens `bookmarkSubscribe` from the newest bookmark. A later disconnect resumes the transaction log. A second `GroupBegin` is a replayed snapshot; those SOW rows are discarded.

- The first start, when the pipeline has no committed bookmark and `amps.start.bookmark` is empty, delivers the **current State of World (SOW)** snapshot and then **live updates** on that same subscription. `amps.position` is not sent on this command.
- A later start that has a stored bookmark, or a task started with `amps.start.bookmark`, calls `bookmarkSubscribe` from that bookmark. It does not request another SOW.
- Requires the AMPS topic to be configured with SOW for the first snapshot.
- Best for reference data and keeping current state.

If the task stops or a poll fails before `GroupEnd`, the connector logs `SOW snapshot incomplete` and tells you to truncate the target table before starting the pipeline again. The same line is logged for an unfinished `sow` query. A hard kill of the extractor process does not run this log. Rows already committed by the pipeline stay in the table.

### 7.4 `sow`

```java
ampsClient.sow(handler, topic, filter, sowQueryBatchSize, 0L);
```

`sowQueryBatchSize` is `amps.sow.query.batch.size`, or `batch.size` when that is `0`.

- Runs one SOW query. There is no standing subscription after the snapshot.
- Drains the snapshot across many `poll()` calls.
- When the snapshot is drained, closes the AMPS client so the client name is free for the next logon.
- Returns one empty `poll()` after that close. The next `poll()` reconnects and starts another full snapshot.
- A FOREGROUND extract stops on the empty poll. A background pipeline keeps polling and loads snapshot after snapshot.
- Each row gets a unique `snapshot` and `sequence` offset, because SOW rows carry no bookmark. Each new snapshot uses a new snapshot id.
- Accepted records: commands `SOW`, `Publish`, and `DeltaPublish` with a non-empty payload. Non-control payloads are also accepted when `GroupBegin` is absent.
- Best for large one-shot reference-data snapshots that must reach downstream without loading the full SOW into memory.

---

## 8. Record Conversion

### 8.1 Topic names

The Kafka topic (or pipeline source topic) is `{kafka.topic.prefix}{amps.topic}`. The default prefix is `amps-`, so `orders` becomes `amps-orders`.

### 8.2 Record shape

| Part | Content |
|------|---------|
| Source partition | `{ "topic": "<amps.topic>" }` |
| Source offset (`subscribe`, `sow_and_subscribe`) | `{ "bookmark": "<bookmark>" }`, or an empty map when the message has no bookmark |
| Source offset (`sow`) | `{ "snapshot": <id>, "sequence": <n> }`, plus `bookmark` when present |
| Kafka partition | Unset (`null`) |
| Key | Schema `com.singlestore.kafka.connect.amps.AMPSKey` |
| Value | Schema `com.singlestore.kafka.connect.amps.AMPSValue`, version 2 |

### 8.3 Key schema

```java
public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
        .name("com.singlestore.kafka.connect.amps.AMPSKey")
        .field("topic", Schema.STRING_SCHEMA)
        .build();
```

| Field | Type | Description |
|-------|------|-------------|
| `topic` | string | AMPS topic the message was received from |

### 8.4 Value schema

```java
public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
        .name("com.singlestore.kafka.connect.amps.AMPSValue")
        .version(2)
        .field("bookmark", Schema.OPTIONAL_STRING_SCHEMA)
        .field("timestamp", Schema.INT64_SCHEMA)
        .field("data", SchemaBuilder.bytes().optional().build())
        .field("topic", Schema.STRING_SCHEMA)
        .build();
```

| Field | Type | Description |
|-------|------|-------------|
| `bookmark` | optional string | AMPS bookmark, or null for SOW rows that carry none |
| `timestamp` | int64 | Time the connector built the record, in epoch milliseconds (not the AMPS server timestamp) |
| `data` | optional bytes | Opaque AMPS payload as UTF-8 bytes |
| `topic` | string | AMPS topic name |

JSON and NVFIX use the same value schema. The connector does not parse payloads.

### 8.5 Payload handling

The AMPS Java client exposes the payload as a string, so conversion is a UTF-8 copy:

```java
byte[] data = dataString == null || dataString.isEmpty()
        ? null
        : dataString.getBytes(StandardCharsets.UTF_8);
```

The message type is the segment of `amps.uri` (`.../amps/json` or `.../amps/nvfix`). There is no separate message-type setting.

Avro keeps `data` as binary. JSON output mode base64-encodes it.

### 8.6 Pipeline schema declarations

SingleStore pipelines must declare `output.key.schema` and `output.value.schema` to match these structs. With `FORMAT AVRO`, map `data` to `LONGBLOB` or `VARBINARY`, or decode UTF-8 in the pipeline when you need text.

---

## 9. Authentication and TLS

### 9.1 Authentication modes

| `amps.auth.type` | When to use |
|------------------|-------------|
| `NONE` (default) | Open AMPS instance with no credentials |
| `PASSWORD` | Username and password (AMPS LDAP, htpasswd, or another password module) |
| `KERBEROS_GSSAPI` | Kerberos GSSAPI against the AMPS service principal |

The property names, defaults, and valid values are in the [README configuration table](../README.md#configuration).

### 9.2 Password

- Requires `amps.username` and `amps.password`.
- The credentials are embedded in the AMPS URI userinfo, URL-encoded. The AMPS default authenticator forwards them to the server.
- `amps.password` has the `PASSWORD` config type, so it is not logged. It can be supplied through a Kafka Connect ConfigProvider, for example `${file:/etc/memsql/amps.secret:amps_password}`.

### 9.3 Kerberos

- Requires `amps.kerberos.spn` and either a JAAS file (`amps.kerberos.jaas.config`) or a keytab and client principal.
- With a keytab and principal, the connector writes a temporary JAAS file from a bundled template. It does not change a JAAS file already set by the worker.
- If the worker already sets `java.security.auth.login.config`, the connector leaves it alone. That JAAS file must define the entry named by `amps.kerberos.jaas.entry` (default `AMPSClient`).
- `KerberosAuthServerChooser` re-authenticates on HA reconnects.
- `amps.kerberos.force.tcp` (default `true`) wraps the host `krb5.conf` with `udp_preference_limit=1` so Java uses TCP to reach the KDC. It does not edit `/etc/krb5.conf`.
- Use a client keytab. Do not copy the AMPS server keytab to the workers.

### 9.4 TLS (`tcps://`)

- Set `amps.ssl.trust.certificate` (PEM or DER CA or server certificates) and/or `amps.ssl.truststore` (JKS or PKCS12).
- Mutual TLS uses `amps.ssl.keystore`.
- The files are loaded into an AMPS-only `SSLContext`. The JVM-wide truststore is not changed.
- SSL settings are rejected on a `tcp://` URI.
- Prefer `tcps://` whenever `PASSWORD` is used, so credentials are not sent in the clear.

Invalid auth combinations fail configuration validation at connector or task start.

---

## 10. SingleStore Pipeline Behavior

SingleStore drives the connector like a Kafka Connect source:

1. Pipeline properties and credentials are passed to the connector's `start()`.
2. The connector starts **one** task for the configured AMPS topic.
3. SingleStore calls `poll()`, writes the successful records in a **single transaction**, and then stores the offsets.
4. If a poll or the transaction fails, the next poll resumes from the last committed bookmark for that topic.
5. If AMPS has no new data, `poll()` waits up to `poll.timeout.ms` and returns an empty list. After at least one record, the poll returns when AMPS has been quiet for `poll.idle.ms`, or sooner when `batch.size` is reached.

### Recommendations

- Set `connector.class` to `AMPSSourceConnector`, or to the fully qualified class name.
- Keep `poll.timeout.ms` modest (for example 100 to 1000 ms) so empty polls do not stall the pipeline.
- Raise `batch.size` for backfills. Partial batches return after `poll.idle.ms` without waiting out the timeout.
- Leave `tasks.max` at 1. Extra tasks do not split an AMPS topic.

---

## 11. Error Handling and Recovery

### 11.1 Subscription fallback

If the bookmark subscription fails with `InvalidTopicException` (for example, the topic does not support the transaction log), `subscribe` falls back to a regular subscribe. The filter is kept:

```java
try {
    return ampsClient.bookmarkSubscribe(handler, topic, filter, null, bookmark, "", 0L);
} catch (InvalidTopicException e) {
    log.warn("Bookmark subscription failed, falling back to regular subscribe: {}", e.getMessage());
    return filter.isEmpty()
            ? ampsClient.subscribe(handler, topic, 0L)
            : ampsClient.subscribe(handler, topic, filter, 0L);
}
```

A first `sow_and_subscribe` already omits the bookmark, so there is no bookmark fallback on that command. The subscription stays open. A resume uses the `subscribe` fallback above. Other `CommandException`s, such as a bad filter, are not caught and fail the poll.

**Caveat:** a regular subscribe does not resume from the transaction log. Use it only when the topic has no transaction log.

### 11.2 Failure semantics

| Situation | Behavior |
|-----------|----------|
| Subscribe or SOW start fails | `RetriableException`. The in-memory bookmark is unchanged, so the retry subscribes from the same position. If a `sow` or `sow_and_subscribe` snapshot was already open, the connector also logs `SOW snapshot incomplete`. |
| A message cannot be converted | The poll fails before `currentBookmark` advances. The restarted task resumes from the last committed offset, so the message is not skipped. |
| AMPS logon refused (empty credentials, Kerberos failure, client name in use) | `start()` fails after `amps.connect.timeout.ms` (default 15 s) instead of retrying forever. |
| Unsubscribe or close fails | Logged. In `stop()`, shutdown is already in progress, so the failure is logged and not treated as a restart signal. |

### 11.3 Resource cleanup

The `finally` block unsubscribes even when the poll fails:

```java
CommandId subId = null;
try {
    subId = subscribeFromBookmark(currentBookmark, handler);
    waitForShortLivedBatch(batch, lastMessageAtMs);
} catch (AMPSException e) {
    throw new RetriableException("Error subscribing to AMPS topic '" + ampsTopic + "'", e);
} finally {
    if (subId != null) {
        try {
            ampsClient.unsubscribe(subId);
        } catch (Exception e) {
            log.warn("Error unsubscribing from AMPS topic '{}': {}", ampsTopic, e.getMessage());
        }
    }
    purgeBookmarkStore();
}
```

### 11.4 Operational notes

- **One topic per connector instance.** Deploy one pipeline or connector per AMPS topic.
- **SOW vs live.** Use `sow` for a one-time snapshot. Use `subscribe` or `sow_and_subscribe` for ongoing ingestion.
- **Bookmark fallback.** If the topic rejects a bookmark subscription, the connector falls back to a regular subscribe (see [11.1](#111-subscription-fallback)).

---

## 12. Performance and Resource Management

### 12.1 Tuning parameters

| Parameter | Impact | Recommendation |
|-----------|--------|----------------|
| `batch.size` | Records per `poll()`. Higher means more throughput and more memory. | Start at 100 and increase if throughput is limited. Maximum 10000. |
| `amps.sow.query.batch.size` | AMPS-side SOW response batching. Used only by `sow`. | Leave at `0` to inherit `batch.size`. |
| `poll.timeout.ms` | Cap on waiting for the first record, or for an empty poll. Partial batches do not wait this out. | Keep modest for pipelines (100 to 1000 ms). |
| `poll.idle.ms` | After the first record, return when AMPS has been quiet this long. | Default 200 ms. Raise only if AMPS bursts have gaps. |

### 12.2 Memory usage

Memory is bounded per poll:

```
Max memory per poll ~= max(batch.size, effective sow query batch size) x average message size
```

Example: `batch.size = 100`, `amps.sow.query.batch.size = 0` (inherits 100), 1 KB average message, so about 100 KB per poll. SOW mode also buffers up to `2 x batch.size` rows of the standing query until they are drained.

### 12.3 Throughput

1. Increase `batch.size` for higher throughput, up to 10000.
2. Keep `poll.idle.ms` small so partial batches return as soon as AMPS goes quiet.
3. Watch the poll cycle time against the batch fill rate.

### 12.4 Bookmark store

The connector uses `MemoryBookmarkStore` instead of `DefaultBookmarkStore`:

```java
client.setBookmarkStore(new MemoryBookmarkStore());
```

**Reason:** subscribing and unsubscribing once per poll made `DefaultBookmarkStore` corrupt its internal state:

```
java.lang.NullPointerException: Cannot load from byte/boolean array because "this.buffer" is null
    at com.crankuptheamps.client.fields.BookmarkField.clone()
```

`MemoryBookmarkStore` handles that pattern without the corruption.

### 12.5 Bookmark store lifecycle

`MemoryBookmarkStore` holds state that has to be released explicitly. Without that, the subscribe-per-poll model grows it without bound:

- The AMPS client calls `BookmarkStore.log()` for **every** message delivered on a bookmark subscription, including messages after `batch.size` is reached. The task calls `discard(message)` on each delivered message, so the store does not keep the whole replay.
- Each poll subscribes under a **new** subscription id, which adds an entry to the store's map. The task calls `purge()` after each unsubscribe, so entries do not pile up over the life of the task.

This is safe because the task tracks `currentBookmark` itself and passes it to the next `bookmarkSubscribe`. It never reads a position back from the store.

Without this, a long EPOCH replay exhausted the SingleStore extractor heap at a repeatable point (around 220,000 records with a 10,000 batch size). The `OutOfMemoryError` surfaced in message conversion, but the retained bookmarks were the cause.

---

## 13. Example Configurations

Full property descriptions are in the [README configuration table](../README.md#configuration). Ready-made files are in the `config/` directory.

### Live or replay (JSON)

```properties
connector.class=AMPSSourceConnector
amps.uri=tcp://amps.example.com:9007/amps/json
amps.topic=orders
amps.subscription.type=subscribe
amps.position=EPOCH
batch.size=100
poll.timeout.ms=1000
poll.idle.ms=200
kafka.topic.prefix=amps-
```

### SOW snapshot

```properties
connector.class=AMPSSourceConnector
amps.uri=tcp://amps.example.com:9007/amps/json
amps.topic=reference-data
amps.subscription.type=sow
amps.filter=/region = 'US'
batch.size=500
```

The snapshot streams across as many polls as it needs. After it completes, the connector closes its AMPS connection and releases the client name, then returns one empty poll. The next poll starts another full snapshot. That matches a FOREGROUND extract, which stops on the first empty batch. A background pipeline keeps going and loads snapshot after snapshot.

### NVFIX with filter

```properties
connector.class=AMPSSourceConnector
amps.uri=tcp://amps.example.com:9007/amps/nvfix
amps.topic=orders-nvfix
amps.filter=35 = 'D'
amps.subscription.type=subscribe
```

### Password authentication

```properties
amps.auth.type=PASSWORD
amps.username=pipeline_user
amps.password=${file:/etc/memsql/amps.secret:amps_password}
```

### Kerberos (keytab)

```properties
amps.auth.type=KERBEROS_GSSAPI
amps.kerberos.spn=AMPS/amps-host.example.com
amps.kerberos.keytab=/etc/memsql/connector.keytab
amps.kerberos.client.principal=kafka-connect@CORP.EXAMPLE.COM
```

---

## 14. Requirements Reference

Requirement IDs describe the connector as built, for review and traceability.

### 14.1 Connection

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-CONN-1 | Connect to AMPS using a URI | `amps.uri` (required), for example `tcp://host:9007/amps/json` |
| FR-CONN-2 | Identify the AMPS client | `amps.client.name` prefix. The client name is `{prefix}{amps.topic}`. |
| FR-CONN-3 | Use an HA client with a bookmark store | `HAClient` with `MemoryBookmarkStore`. The task owns resume position through Connect offsets. |
| FR-CONN-4 | Single URI only | The server chooser gets one URI. Comma-separated HA URI lists are not supported. |

### 14.2 Authentication

| ID | Mode | Requirement |
|----|------|-------------|
| FR-AUTH-1 | `NONE` (default) | Connect with `amps.uri` unchanged. |
| FR-AUTH-2 | `PASSWORD` | Require `amps.username` and `amps.password`. Embed credentials in the URI userinfo. The AMPS default authenticator forwards them (LDAP, htpasswd, or custom password modules on the server). |
| FR-AUTH-3 | `KERBEROS_GSSAPI` | Require `amps.kerberos.spn` and either `amps.kerberos.jaas.config`, or `amps.kerberos.keytab` with `amps.kerberos.client.principal`. |
| FR-AUTH-4 | Kerberos JAAS | If the worker has not set `java.security.auth.login.config`, set it from the JAAS file path, or from a temporary file built from the bundled keytab template. |
| FR-AUTH-5 | Kerberos reconnect | Use `KerberosAuthServerChooser` so HA reconnects re-authenticate. |
| FR-AUTH-6 | Password secrecy | `amps.password` is `ConfigDef.Type.PASSWORD`. It can come from a Connect ConfigProvider. |

Invalid auth combinations fail configuration validation at connector or task start.

### 14.3 Topic and filter

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-TOPIC-1 | Subscribe to one AMPS topic | `amps.topic` (required). Topic regular expressions are not supported. |
| FR-TOPIC-2 | Optional content filter | `amps.filter` is sent to AMPS for `subscribe`, `sow_and_subscribe`, and `sow`. |
| FR-TOPIC-3 | Kafka or pipeline topic name | `{kafka.topic.prefix}{amps.topic}`, default prefix `amps-`. |

### 14.4 Subscription modes

| ID | Value | Requirement |
|----|-------|-------------|
| FR-SUB-1 | `subscribe` | Each `poll()` bookmark-subscribes from the current bookmark. It collects until `batch.size`, or until AMPS is quiet for `poll.idle.ms` after at least one record, or until `poll.timeout.ms` passes with no records. Then it unsubscribes. |
| FR-SUB-2 | `sow_and_subscribe` | Open one `sowAndSubscribe` when there is no stored bookmark and no `amps.start.bookmark`. Omit the bookmark header so AMPS returns the current snapshot and then live updates. Do not send `EPOCH` or `NOW`; AMPS treats that header as a historical SOW point. Drain that subscription on each `poll()`. After `GroupEnd`, replace it with `bookmarkSubscribe` from the newest bookmark so an HAClient reconnect does not deliver another SOW. Discard a replayed SOW. When a bookmark is already stored, or `amps.start.bookmark` is set, open `bookmarkSubscribe` instead and do not request another SOW. |
| FR-SUB-3 | `sow` | Start one SOW query and stream rows across successive `poll()` calls. Close the AMPS client, return one empty poll, then start another full SOW on the next poll. A FOREGROUND extract stops on that empty poll. A background pipeline loads snapshot after snapshot. |
| FR-SUB-4 | SOW AMPS batching | `amps.sow.query.batch.size` (0 inherits `batch.size`, maximum 10000) sets AMPS SOW response batching independently of `batch.size`. |
| FR-SUB-5 | Bookmark subscribe fallback | If `bookmarkSubscribe` fails with `InvalidTopicException`, fall back to a regular `subscribe`. |

### 14.5 Resume and offsets

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-OFF-1 | Every record has a partition | `sourcePartition = { "topic": "<amps.topic>" }` |
| FR-OFF-2 | Every record has an offset when AMPS provides a bookmark | `sourceOffset = { "bookmark": "<bookmark>" }`. The offset is an empty map when the bookmark is null. |
| FR-OFF-3 | Restore on start | `context.offsetStorageReader().offset(partition)`, unless overridden. |
| FR-OFF-4 | Start position when no offset is stored | `subscribe` uses `amps.position`: `EPOCH` (default) or `LATEST` (`NOW`). The first `sow_and_subscribe` does not send that bookmark. |
| FR-OFF-5 | Explicit bookmark override | A non-empty `amps.start.bookmark` overrides stored offsets and `amps.position`. |
| FR-OFF-6 | SOW ignores bookmarks | SOW mode neither restores nor uses Connect bookmarks. It always reads the current snapshot from the beginning. |
| FR-OFF-7 | SOW offsets are unique | SOW rows carry no AMPS bookmark, so each row gets a `snapshot` id and a `sequence` that starts at 0 for that snapshot. Repeated snapshots stay distinct because each has its own snapshot id. |
| FR-OFF-8 | In-memory cursor | After a non-empty poll, the task advances `currentBookmark` for the next subscribe. |

Two tasks shall not emit the same `sourcePartition`. With one task, the connector does not split one AMPS topic across tasks.

### 14.6 Polling and batching

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-POLL-1 | Bound memory per poll | At most `batch.size` records (1 to 10000, default 100). |
| FR-POLL-2 | Copy AMPS messages immediately | The payload and bookmark are copied off the reused AMPS `Message`. |
| FR-POLL-3 | Empty result | Return an empty list when the task is not running, or when no messages arrive within `poll.timeout.ms`. A failed subscribe, SOW start, or SOW re-arm throws `RetriableException` and leaves the in-memory bookmark unchanged. |
| FR-POLL-4 | Conversion errors | A message that cannot be converted fails the task before the in-memory bookmark advances. The restarted task resumes from the last committed offset. |
| FR-POLL-5 | Idle return | After the first record in `subscribe` or `sow_and_subscribe`, return when AMPS has been quiet for `poll.idle.ms` (default 200), unless `batch.size` is reached first. |

### 14.7 Payload formats

| ID | URI segment | Requirement |
|----|-------------|-------------|
| FR-MSG-1 | `json` | Encode the AMPS payload as opaque UTF-8 bytes in `data`. Do not parse or validate it. |
| FR-MSG-2 | `nvfix` | Encode the AMPS payload as opaque UTF-8 bytes in `data`. Do not parse fields or delimiters. |

Other AMPS protocol message types are not implemented.

### 14.8 Record shape

- **Key:** schema `com.singlestore.kafka.connect.amps.AMPSKey`, value `{ "topic": "<amps.topic>" }`. See [8.3](#83-key-schema).
- **Value:** schema `com.singlestore.kafka.connect.amps.AMPSValue`, version 2. See [8.4](#84-value-schema).
- **Kafka partition:** unset (`null`).

The connector copies only the payload and the bookmark off the AMPS message.

### 14.9 Non-functional requirements

| ID | Requirement | Current behavior |
|----|-------------|------------------|
| NFR-1 | Predictable memory | No standing unbounded queue. The per-poll list is capped at `batch.size`. SOW may buffer AMPS-side batches until they are drained. |
| NFR-2 | Logging | SLF4J. Start and stop, SOW progress, and conversion errors are logged. |
| NFR-3 | Secrets | `amps.password` uses the Password type. The Kerberos keytab is referenced by path. The keytab file is not bundled. |

### 14.10 Assumptions and constraints

1. The AMPS transaction log is enabled when bookmark replay is required (EPOCH, a stored offset, or `amps.start.bookmark`).
2. The `amps.uri` message-type segment is `json` or `nvfix`.
3. SingleStore pipeline batches wait for every task's `poll()`. Empty polls wait `poll.timeout.ms`, so keep it modest. Partial batches return after `poll.idle.ms`.
4. SOW mode is a snapshot, not a live tail. Live updates need `subscribe` or `sow_and_subscribe`.
5. A bookmark subscribe fallback may skip transaction-log positioning if the topic does not support bookmarks.

---

## 15. Code Traceability and Support

| Area | Primary types |
|------|----------------|
| Connector SPI | `AMPSSourceConnector`, `AMPSSourceTask` |
| Configuration | `AMPSSourceConnectorConfig`, `SubscriptionType`, `Position`, `AuthType` |
| Client | `AMPSClientFactoryImpl`, `KerberosJaasMaterializer`, `KerberosKrb5Materializer`, `KerberosAuthServerChooser`, `AMPSSslConfigurator` |
| Records | `RecordConverter`, `AMPSMessageData` |
| Discovery | `src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector` |

### Support identity

When you open a support case, include:

- Connector version **0.1.0** (pre-GA)
- Connector class `com.singlestore.kafka.connect.amps.AMPSSourceConnector`
- `amps.subscription.type`, `amps.uri`, and `amps.auth.type`
- Whether the host is a SingleStore pipeline or Kafka Connect

---

## 16. Summary

The Kafka Connect AMPS Source Connector reads AMPS topics using:

1. **Pull-based polling.** Subscribe, collect, and unsubscribe on each `poll()`.
2. **Bookmark tracking.** Resume from the last committed position on restart.
3. **Defensive copying.** Payloads and bookmarks are copied before AMPS reuses the message.
4. **Named key and value schemas.** The payload is carried as opaque bytes, so the schema stays fixed.
5. **Bounded memory.** Predictable resource use regardless of message rate.

This design delivers AMPS messages to Kafka reliably and efficiently, and it handles EPOCH replay and rapid restarts.
