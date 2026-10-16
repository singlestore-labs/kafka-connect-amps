# Technical Guide: Reading AMPS Topics with Kafka Connect

This document provides an in-depth technical explanation of how the Kafka Connect AMPS Source Connector reads messages from AMPS topics and delivers them to Apache Kafka.

> See [README.md](../README.md#authentication) for authentication setup
> (`amps.auth.type` = `NONE` / `PASSWORD` / `KERBEROS_GSSAPI`), the JAAS sample,
> and the full Kerberos GSSAPI sequence diagram.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Message Flow](#message-flow)
4. [Bookmark-Based Offset Management](#bookmark-based-offset-management)
5. [Subscription Types](#subscription-types)
6. [Record Conversion](#record-conversion)
7. [Error Handling and Recovery](#error-handling-and-recovery)
8. [Performance Considerations](#performance-considerations)

---

## Overview

The connector uses **subscribe-on-demand polling** to read messages from AMPS topics. Unlike traditional push-based connectors that maintain continuous subscriptions with internal queues, this connector:

1. **Subscribes on-demand** during each `poll()` call
2. **Collects a batch** of messages (until `batch.size`, idle quiet, or empty timeout)
3. **Unsubscribes immediately** after collection
4. **Tracks position** using AMPS bookmarks committed as source offsets

This approach provides predictable memory usage and prevents queue overflow during large replays (e.g., EPOCH replay).

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Kafka Connect Framework                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      AMPSSourceConnector                                │ │
│  │  - Validates configuration                                              │ │
│  │  - Creates single AMPSSourceTask for configured topic                   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│                                    ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        AMPSSourceTask                                   │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │ HAClient     │  │ RecordConverter │ │ Bookmark    │                  │ │
│  │  │ (AMPS conn)  │  │ (AMPS→Kafka)  │  │ Tracking    │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│                                    ▼                                         │
│                          List<SourceRecord>                                  │
│                                    │                                         │
│                                    ▼                                         │
│                             Kafka Topics                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Class Responsibilities

| Class | Responsibility |
|-------|---------------|
| `AMPSSourceConnector` | Entry point. Validates config, creates task configurations |
| `AMPSSourceTask` | Core logic. Manages polling lifecycle and bookmark tracking |
| `AMPSSourceConnectorConfig` | Configuration parsing and validation |
| `AMPSClientFactoryImpl` | Creates `HAClient` with `MemoryBookmarkStore` |
| `AMPSMessageData` | Immutable POJO holding copied message data |
| `RecordConverter` | Converts AMPS messages to Kafka `SourceRecord` objects |

---

## Message Flow

### Complete Poll Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             poll() Method                                    │
│                                                                              │
│  Step 1: Check if running                                                   │
│     │                                                                        │
│     ▼                                                                        │
│  Step 2: Initialize collection structures                                   │
│     • ArrayList<AMPSMessageData> batch                                      │
│     • AtomicReference<String> lastBookmarkRef                               │
│     • AtomicLong lastMessageAtMs                                            │
│     │                                                                        │
│     ▼                                                                        │
│  Step 3: Create MessageHandler (callback)                                   │
│     │                                                                        │
│     ▼                                                                        │
│  Step 4: subscribeFromBookmark(currentBookmark, handler)                    │
│     │         │                                                              │
│     │         ├──▶ bookmarkSubscribe() ──┐                                  │
│     │         │           OR             │                                   │
│     │         └──▶ sowAndSubscribe() ────┤                                  │
│     │                                    │                                   │
│     │         ◀── Messages pushed ───────┘                                  │
│     │              from AMPS                                                 │
│     ▼                                                                        │
│  Step 5: Wait until batch.size, idle quiet, or empty timeout                 │
│     • full batch → return immediately                                       │
│     • ≥1 record then AMPS quiet for poll.idle.ms → return                   │
│     • 0 records until poll.timeout.ms → return empty                        │
│     │                                                                        │
│     ▼                                                                        │
│  Step 6: Unsubscribe (always, in finally block)                             │
│     • ampsClient.unsubscribe(subId)                                         │
│     │                                                                        │
│     ▼                                                                        │
│  Step 7: Convert AMPSMessageData → SourceRecord                             │
│     • recordConverter.convert(ampsTopic, messageData)                       │
│     │                                                                        │
│     ▼                                                                        │
│  Step 8: Update currentBookmark for next poll                               │
│     │                                                                        │
│     ▼                                                                        │
│  Step 9: Return List<SourceRecord>                                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### MessageHandler Callback Logic

When AMPS pushes messages, the `MessageHandler` callback executes:

```java
public void invoke(Message message) {
        synchronized (pollWaitLock) {
            if (batch.size() < config.batchSize) {
                // Copy payload and bookmark immediately (AMPS reuses Message objects)
                AMPSMessageData data = copyMessage(message);
                batch.add(data);
                String bookmark = data.getBookmark();
                if (bookmark != null) {
                    lastBookmarkRef.set(bookmark);
                }
                lastMessageAtMs.set(System.currentTimeMillis());
            }
            pollWaitLock.notifyAll();
        }
}
```

**Critical Detail:** AMPS reuses `Message` objects between callbacks. The connector must copy all data immediately before the next callback overwrites the buffer.

---

## Bookmark-Based Offset Management

### What is an AMPS Bookmark?

An AMPS bookmark is a unique identifier for a message's position in the transaction log. Format example:
```
3850692670110263676|1774627282001110361|
```

Bookmarks enable:
- **Resumable subscriptions** - Start reading from where you left off
- **At-least-once delivery** - Replay from a known position after failure
- **EPOCH replay** - Read all historical messages from the beginning

### Bookmark Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Task Startup                                        │
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │ Config has      │ NO │ Kafka Connect   │ NO │ Use position    │         │
│  │ start.bookmark? │───▶│ has stored      │───▶│ config          │         │
│  │                 │    │ offset?         │    │ (EPOCH/LATEST)  │         │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘         │
│           │ YES                  │ YES                  │                   │
│           ▼                      ▼                      ▼                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │ currentBookmark │    │ currentBookmark │    │ currentBookmark │         │
│  │ = config value  │    │ = stored offset │    │ = EPOCH or NOW  │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Bookmark Priority Order

1. **`amps.start.bookmark`** (highest) - Explicitly configured starting point
2. **Stored offset** - Retrieved from Kafka Connect's offset storage
3. **`amps.position`** (lowest) - `EPOCH` (beginning) or `LATEST` (now)

### Code Implementation

```java
// In AMPSSourceTask.initializeBookmark()

// Priority 1: Configured start bookmark
if (config.ampsStartBookmark != null && !config.ampsStartBookmark.isEmpty()) {
    currentBookmark = config.ampsStartBookmark;
    return;
}

// Priority 2: Stored offset from Kafka Connect
Map<String, Object> sourcePartition = Collections.singletonMap("topic", ampsTopic);
Map<String, Object> lastOffset = context.offsetStorageReader().offset(sourcePartition);

if (lastOffset != null && !lastOffset.isEmpty()) {
    currentBookmark = (String) lastOffset.get("bookmark");
} else {
    // Priority 3: Configured position
    if ("EPOCH".equalsIgnoreCase(config.ampsPosition)) {
        currentBookmark = Client.Bookmarks.EPOCH;  // Beginning of transaction log
    } else {
        currentBookmark = Client.Bookmarks.NOW;    // Current position (new messages only)
    }
}
```

### Offset Commit Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  AMPSSourceTask                 Kafka Connect                 Kafka          │
│       │                              │                          │            │
│       │  SourceRecord with           │                          │            │
│       │  sourceOffset={bookmark}     │                          │            │
│       │─────────────────────────────▶│                          │            │
│       │                              │                          │            │
│       │                              │  Write to Kafka          │            │
│       │                              │─────────────────────────▶│            │
│       │                              │                          │            │
│       │                              │  Async commit offset     │            │
│       │                              │  to __consumer_offsets   │            │
│       │                              │─────────────────────────▶│            │
│       │                              │                          │            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Subscription Types

The connector supports three AMPS read modes:

### 1. Bookmark Subscribe (`subscribe`)

```java
ampsClient.bookmarkSubscribe(handler, topic, filter, null, bookmark, "", 0L);
```

- Reads messages from the **transaction log** starting at the bookmark
- Includes **historical messages** (if bookmark is in the past)
- Continues with **live messages** after catching up
- Best for: Event streaming, change data capture

### 2. SOW and Subscribe (`sow_and_subscribe`)

```java
ampsClient.sowAndSubscribe(handler, topic, filter, 0, "bookmark=" + bookmark, 0L);
```

- First delivers **State of World (SOW)** snapshot - current state of all records
- Then delivers **live updates** as they occur
- Requires AMPS topic to be configured with SOW
- Best for: Reference data, maintaining current state

### 3. SOW Only (`sow`)

```java
ampsClient.sow(handler, topic, filter, sowQueryBatchSize, 0L);
```

- Executes a **State of World (SOW)** query only
- Does **not** keep a live subscription open after the snapshot
- Starts one AMPS SOW query and drains the snapshot across many Kafka Connect `poll()` calls
- Closes the AMPS client once the snapshot is drained, so the client name is free for the next logon
- Returns one empty `poll()` after that close, then the next `poll()` reconnects and starts another full snapshot
- A FOREGROUND extract stops on the empty poll. A background pipeline keeps polling and loads snapshot after snapshot
- Emits a unique `snapshot`/`sequence` source offset per row, since SOW results carry no bookmark. Each new snapshot uses a new snapshot id
- Uses `batch.size` for Kafka poll batching and `amps.sow.query.batch.size` for AMPS-side SOW batching (`0` inherits `batch.size`)
- Best for: Large one-shot reference-data snapshots that must be delivered downstream without loading the full SOW into memory

### Subscription Type Selection

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  subscribeFromBookmark(bookmark, handler)                                   │
│       │                                                                      │
│       ▼                                                                      │
│  ┌────────────────────────────────────────────┐                              │
│  │ subscriptionType?                         │                              │
│  │ subscribe / sow_and_subscribe / sow       │                              │
│  └───────────────┬───────────────┬────────────┘                              │
│                  │               │                                           │
│         subscribe│    sow_and_   │sow                                        │
│                  │    subscribe  │                                           │
│                  ▼               ▼                   ▼                       │
│  ┌───────────────┐   ┌────────────────┐   ┌────────────────────────────┐     │
│  │bookmarkSubscrib│   │sowAndSubscribe │   │sow                         │     │
│  │e (txlog replay)│   │(SOW + live)    │   │(SOW query only)            │     │
│  └───────┬───────┘   └───────┬────────┘   └──────────────┬─────────────┘     │
│          │                   │                                         │      │
│          │ fallback          │ fallback                                │      │
│          ▼                   ▼                                         ▼      │
│  ┌───────────────┐   ┌────────────────┐                      Kafka records     │
│  │subscribe       │   │sowAndSubscribe │                                       │
│  │(no bookmark)   │   │(no bookmark opt│                                       │
│  │                │   │ions, filter kept)                                      │
│  └───────────────┘   └────────────────┘                                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Filters

`amps.filter` is passed through to all three modes. That means AMPS evaluates the
content filter server-side for bookmark subscriptions, SOW-and-subscribe, and pure
SOW queries.

---

## Record Conversion

### AMPS Message to Kafka Record

The `RecordConverter` transforms AMPS messages into Kafka Connect `SourceRecord` objects:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AMPSMessageData                                     │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │ data: "{\"symbol\":\"AAPL\",\"price\":182.50}"              │            │
│  │ bookmark: "3850692670110263676|1774627282001110361|"        │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                              │                                               │
│                              ▼                                               │
│                      RecordConverter.convert()                               │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │                     SourceRecord                             │            │
│  │                                                              │            │
│  │  sourcePartition: {"topic": "market-ticker"}                │            │
│  │  sourceOffset:    {"bookmark": "3850...|1774...|"}          │            │
│  │  kafkaTopic:      "amps-market-ticker"                      │            │
│  │                                                              │            │
│  │  key (schema):                                               │            │
│  │    {                                                         │            │
│  │      "topic": "market-ticker"                               │            │
│  │    }                                                         │            │
│  │                                                              │            │
│  │  value (schema):                                             │            │
│  │    {                                                         │            │
│  │      "bookmark": "3850692670110263676|1774627282001110361|",│            │
│  │      "timestamp": 1646234567890,                            │            │
│  │      "data": "{\"symbol\":\"AAPL\",\"price\":182.50}",      │            │
│  │      "topic": "market-ticker"                               │            │
│  │    }                                                         │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Schema

The key uses a defined schema for consistent Kafka partitioning:

```java
public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
    .name("com.singlestore.kafka.connect.amps.AMPSKey")
    .field("topic", Schema.STRING_SCHEMA)
    .build();
```

### Value Schema

The value is a `Struct` so SingleStore pipelines can declare a matching `output.value.schema`.
JSON and NVFIX use the same opaque-bytes schema:

```java
SchemaBuilder.struct()
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
| `bookmark` | optional string | AMPS bookmark for offset tracking |
| `timestamp` | int64 | Processing timestamp (milliseconds) |
| `data` | optional bytes | Opaque AMPS payload encoded as UTF-8 bytes |
| `topic` | string | Source AMPS topic name |

### Payload Handling

The AMPS payload is treated as opaque bytes. The connector performs no JSON or NVFIX
parsing. The AMPS Java client exposes the body as a string, so conversion is a UTF-8 byte copy:

```java
byte[] data = dataString == null || dataString.isEmpty()
        ? null
        : dataString.getBytes(StandardCharsets.UTF_8);
```

Avro preserves `data` as binary. JSON output mode base64-encodes it.

---

## Error Handling and Recovery

### Subscription Fallback

If bookmark subscription fails (e.g., topic doesn't support transaction log), the connector falls back to regular subscription:

```java
try {
    return ampsClient.bookmarkSubscribe(handler, topic, filter, null, bookmark, "", 0L);
} catch (InvalidTopicException e) {
    log.warn("Bookmark subscription failed, falling back to regular subscribe");
    return ampsClient.subscribe(handler, topic, 0L);
}
```

### Resource Cleanup

The `finally` block ensures unsubscription even on error:

```java
try {
    subId = subscribeFromBookmark(currentBookmark, handler);
    // ... wait for messages
} catch (AMPSException e) {
    throw new RetriableException("Error subscribing", e);
} finally {
    // ALWAYS unsubscribe to prevent resource leaks
    if (subId != null) {
        try {
            ampsClient.unsubscribe(subId);
        } catch (Exception e) {
            log.warn("Error unsubscribing: {}", e.getMessage());
        }
    }
}
```

A message that cannot be converted fails the task before `currentBookmark` moves, so the restarted task resumes from the last committed offset. `stop()` still only logs a failed `close()`; shutdown is already in progress and is not a restart signal.

### Restart Recovery

On task restart, the connector resumes from the last committed offset:

1. Kafka Connect calls `start()` with task configuration
2. `initializeBookmark()` reads last offset from Kafka's offset storage
3. First `poll()` subscribes from the stored bookmark
4. Processing continues without message loss

---

## Performance Considerations

### Tuning Parameters

| Parameter | Impact | Recommendation |
|-----------|--------|----------------|
| `batch.size` | Messages per poll. Higher = better throughput, more memory | Start with 100, increase if throughput limited |
| `amps.sow.query.batch.size` | AMPS-side SOW response batching. Only applies to `sow` | Leave unset/`0` to inherit `batch.size`; override only for SOW tuning |
| `poll.timeout.ms` | Cap waiting for the first record / empty poll. Partial batches do not wait this out | Keep modest for SingleStore pipelines (e.g. 100–1000ms) |
| `poll.idle.ms` | After the first record, return when AMPS has been quiet this long | Default 200ms; raise only if AMPS bursts have gaps |

### Memory Usage

Unlike push-based connectors, memory usage is bounded:

```
Max Memory ≈ max(batch.size, amps.sow.query.batch.size_effective) × average_message_size
```

For example:
- `batch.size = 100`
- `amps.sow.query.batch.size = 0` (inherits `100`)
- Average message = 1KB
- Max memory per poll ≈ 100KB

### Throughput Optimization

1. **Increase batch.size** for higher throughput (up to 10,000)
2. **Keep poll.idle.ms** small so partial batches return as soon as AMPS goes quiet
3. **Monitor** poll cycle time vs batch fill rate

### Why MemoryBookmarkStore?

The connector uses `MemoryBookmarkStore` instead of `DefaultBookmarkStore`:

```java
client.setBookmarkStore(new MemoryBookmarkStore());
```

**Reason:** Rapid subscribe/unsubscribe cycles (one per poll) caused `DefaultBookmarkStore` to corrupt its internal state, resulting in:
```
java.lang.NullPointerException: Cannot load from byte/boolean array because "this.buffer" is null
    at com.crankuptheamps.client.fields.BookmarkField.clone()
```

`MemoryBookmarkStore` has simpler state management that tolerates this pattern.

### Bookmark store lifecycle

`MemoryBookmarkStore` keeps state that must be released explicitly, and the connector's
subscribe-per-poll model makes both kinds of growth unbounded if it is not:

- The AMPS client calls `BookmarkStore.log()` for **every** message delivered on a bookmark
  subscription, including messages that arrive after `batch.size` is reached and get dropped.
  The task calls `discard(message)` on each delivered message so the store does not retain
  the whole replay.
- Each poll subscribes under a **new** subscription id, which creates a new entry in the
  store's internal subscription map. The task calls `purge()` after unsubscribing so entries
  do not accumulate one per poll for the life of the task.

This is safe because the task tracks `currentBookmark` itself and passes it explicitly to the
next `bookmarkSubscribe`; it never recovers a position from the store.

Without this, a long EPOCH replay exhausted the SingleStore extractor heap at a reproducible
point (around 220,000 records with a 10,000 batch size), surfacing as
`java.lang.OutOfMemoryError: Java heap space` at whatever allocated next — the stack trace
pointed at message conversion rather than at the retained bookmarks.

---

## Summary

The Kafka Connect AMPS Source Connector reads AMPS topics using:

1. **Pull-based polling** - Subscribe, collect, unsubscribe per `poll()` call
2. **Bookmark tracking** - Resume from last position on restart
3. **Defensive copying** - Protect against AMPS buffer reuse
4. **Named key and value schemas** - Payload carried as opaque bytes so the schema stays fixed
5. **Bounded memory** - Predictable resource usage regardless of message rate

This design ensures reliable, efficient message delivery from AMPS to Kafka while handling edge cases like EPOCH replay and rapid restarts.
