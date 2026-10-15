# AMPS Source Connector — Requirements (As Built)

**Product:** Kafka Connect AMPS Source Connector (`kafka-connect-amps`)  
**Version documented:** 0.1.0 (pre-GA)  
**Status:** As-built, pre-GA. This document describes capabilities that exist in the current codebase. It is not a GA release and not a roadmap.

**Implementing classes**

| Role                       | Fully qualified name |
|----------------------------|----------------------|
| Connector                  | `com.singlestore.kafka.connect.amps.AMPSSourceConnector` |
| Task                       | `com.singlestore.kafka.connect.amps.AMPSSourceTask` |
| Short name (ServiceLoader) | `AMPSSourceConnector` |

---

## 1. Purpose

The connector shall read messages from a single AMPS topic and emit Kafka Connect `SourceRecord` objects that can be consumed by:

- Apache Kafka Connect workers, or
- SingleStore `CREATE PIPELINE` custom connectors that host Kafka Connect source plugins.

It shall not publish to AMPS (no sink).

---

## 2. Runtime and packaging

| Requirement         | Current capability                                                                                                                                                     |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Language / bytecode | Java 11                                                                                                                                                                |
| Kafka Connect API   | `connect-api` 3.5.1 (provided by the worker)                                                                                                                           |
| AMPS Java client    | 5.3.2.0, bundled into the shaded jar                                                                                                                                   |
| Artifact            | Shaded jar `kafka-connect-amps-0.1.0.jar`                                                                                                                              |
| Discovery           | `META-INF/services/org.apache.kafka.connect.source.SourceConnector` contains the fully qualified connector class so hosts can load `AMPSSourceConnector` by short name |

---

## 3. Kafka Connect / SingleStore contract

The connector shall implement the Kafka Connect source SPI as follows.

### 3.1 SourceConnector

The connector shall:

- Validate and store properties in `start(Map<String, String>)` via `AMPSSourceConnectorConfig`.
- Return `AMPSSourceTask` from `taskClass()`.
- Return a `ConfigDef` from `config()`.
- Return **exactly one** task configuration from `taskConfigs(int maxTasks)`, even if `tasks.max` is greater than 1.
- Perform log-only cleanup in `stop()`.

The connector shall not monitor AMPS or reconfigure tasks dynamically after `start()`.

### 3.2 SourceTask

The task shall:

- Parse configuration, open an AMPS `HAClient`, and initialize resume position in `start()`.
- Fetch up to `batch.size` messages (or an empty list) in `poll()`.
- Close the AMPS client and cancel any in-flight SOW command in `stop()`.

Optional `commit()` and `commitRecord()` shall not be overridden. Offset persistence is left to the host (Kafka Connect or SingleStore) after a successful batch.

### 3.3 SingleStore pipeline behavior

When used under SingleStore pipelines, the following shall hold:

| Requirement | Current behavior |
|-------------|------------------|
| Configuration source | Properties from `CREATE PIPELINE` are passed to `start()` |
| Idle poll | Empty `subscribe` / `sow_and_subscribe` polls wait up to `poll.timeout.ms`. After at least one record, the poll returns when AMPS is quiet for `poll.idle.ms` (or sooner if `batch.size` is hit). |
| Failed batch | Host restarts `poll()` from the last committed `sourceOffset` per `sourcePartition` |
| Task count | One stream task; all records share one `sourcePartition` |

`poll.timeout.ms` is the maximum wait for the first AMPS record after subscribe, or while draining SOW rows already in flight. After the first record, `subscribe` / `sow_and_subscribe` polls return when AMPS has been quiet for `poll.idle.ms`.

---

## 4. Functional requirements

### 4.1 Connection

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-CONN-1 | Connect to AMPS using a URI | `amps.uri` (required), e.g. `tcp://host:9007/amps/json` |
| FR-CONN-2 | Identify the AMPS client | `amps.client.name` prefix; actual name is `{prefix}{amps.topic}` |
| FR-CONN-3 | Use HA client with bookmark store | `HAClient` + `MemoryBookmarkStore` (bookmarks for resume are owned by the task / Connect offsets, not AMPS discard) |
| FR-CONN-4 | Single URI only | Server chooser is given one URI; comma-separated HA URI lists are not supported |

TLS (`tcps://`) is configured with `amps.ssl.trust.certificate` (PEM) and/or `amps.ssl.truststore`. Mutual TLS uses `amps.ssl.keystore`. These files are loaded into an AMPS-only SSLContext; the connector does not require JVM-wide trustStore flags.

### 4.2 Authentication

| ID | Mode | Requirement |
|----|------|-------------|
| FR-AUTH-1 | `NONE` (default) | Connect with `amps.uri` unchanged |
| FR-AUTH-2 | `PASSWORD` | Require `amps.username` and `amps.password`; embed credentials in the URI userinfo; AMPS default authenticator forwards them (LDAP / htpasswd / custom password modules on the server) |
| FR-AUTH-3 | `KERBEROS_GSSAPI` | Require `amps.kerberos.spn` and either `amps.kerberos.jaas.config` **or** (`amps.kerberos.keytab` + `amps.kerberos.client.principal`) |
| FR-AUTH-4 | Kerberos JAAS | If the worker has not set `java.security.auth.login.config`, set it from the JAAS file path or from a temp file built from the bundled keytab template |
| FR-AUTH-5 | Kerberos reconnect | Use `KerberosAuthServerChooser` so HA reconnects re-authenticate |
| FR-AUTH-6 | Password secrecy | `amps.password` is `ConfigDef.Type.PASSWORD` and may be supplied via a Connect ConfigProvider |

Invalid auth combinations shall fail configuration validation at connector/task start.

### 4.3 Topic and filter

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-TOPIC-1 | Subscribe to one AMPS topic | `amps.topic` (required); topic regex is not supported |
| FR-TOPIC-2 | Optional content filter | `amps.filter` sent to AMPS for subscribe, sow_and_subscribe, and sow |
| FR-TOPIC-3 | Kafka / pipeline topic name | `{kafka.topic.prefix}{amps.topic}` (default prefix `amps-`) |

### 4.4 Subscription modes

Configured by `amps.subscription.type` (default `subscribe`).

| ID | Value | Requirement |
|----|-------|-------------|
| FR-SUB-1 | `subscribe` | Each `poll()` bookmark-subscribes from the current bookmark, collects until `batch.size`, AMPS is quiet for `poll.idle.ms` after at least one record, or `poll.timeout.ms` with no records, then unsubscribes |
| FR-SUB-2 | `sow_and_subscribe` | Same poll cycle using `sowAndSubscribe`, with bookmark options when AMPS accepts them; fallback without bookmark options on `InvalidTopicException` |
| FR-SUB-3 | `sow` | Start one SOW query, stream snapshot rows across successive `poll()` calls, close the AMPS client, return one empty poll, then on the following poll start another full SOW. A FOREGROUND extract stops on that empty poll, so the next FOREGROUND loads a fresh snapshot. A background pipeline keeps polling and loads snapshot after snapshot |
| FR-SUB-4 | SOW AMPS batching | `amps.sow.query.batch.size` (0 = inherit `batch.size`, max 10000) controls AMPS SOW response batching independently of Connect `batch.size` |
| FR-SUB-5 | Bookmark subscribe fallback | If `bookmarkSubscribe` fails with `InvalidTopicException`, fall back to regular `subscribe` |

SOW snapshot records accepted: commands SOW, Publish, and DeltaPublish with non-empty payload (plus non-control payloads if GroupBegin is omitted).

### 4.5 Resume and offsets

Bookmark resume requires an AMPS transaction log for `subscribe` / `sow_and_subscribe`.

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-OFF-1 | Every record has a partition | `sourcePartition = { "topic": "<amps.topic>" }` |
| FR-OFF-2 | Every record has an offset when AMPS provides a bookmark | `sourceOffset = { "bookmark": "<bookmark>" }`; empty map if bookmark is null |
| FR-OFF-3 | Restore on start | `context.offsetStorageReader().offset(partition)` unless overridden |
| FR-OFF-4 | Start position if no stored offset | `amps.position`: `EPOCH` (default) or `LATEST` (`NOW`) |
| FR-OFF-5 | Explicit bookmark override | Non-empty `amps.start.bookmark` overrides stored offsets and `amps.position` |
| FR-OFF-6 | SOW ignores bookmarks | SOW mode does not restore or drive AMPS from Connect bookmarks; it always reads the current snapshot from the beginning |
| FR-OFF-7 | SOW offsets are unique | SOW rows carry no AMPS bookmark, so each row is assigned a `snapshot` id plus a monotonic `sequence` to keep repeated snapshots distinguishable downstream |
| FR-OFF-7 | In-memory cursor | After a non-empty poll, the task advances `currentBookmark` for the next subscribe |

Two tasks shall not emit the same `sourcePartition`. Combined with FR one-task, this connector shall not split one AMPS topic across tasks.

### 4.6 Polling and batching

| ID | Requirement | Implementation |
|----|-------------|----------------|
| FR-POLL-1 | Bound memory per poll | At most `batch.size` records (1–10000, default 100) |
| FR-POLL-2 | Copy AMPS messages immediately | Payload and bookmark copied off the reused AMPS `Message` |
| FR-POLL-3 | Empty result | Return empty list when not running, on subscribe/SOW start failure, or when no messages were collected after `poll.timeout.ms` |
| FR-POLL-4 | Conversion errors | Failed message conversion is logged and that message is skipped; remaining batch is still returned |
| FR-POLL-5 | Idle return | After the first record in `subscribe` / `sow_and_subscribe`, return when AMPS has been quiet for `poll.idle.ms` (default 200) unless `batch.size` is already hit |

### 4.7 Payload formats

The message type is the segment of `amps.uri` (`json` or `nvfix`). There is no separate message-type setting. The connector does not parse the payload.

| ID | URI segment | Requirement |
|----|-------------|-------------|
| FR-MSG-1 | `json` | Encode the original AMPS payload as opaque UTF-8 bytes in `data`; do not parse or validate it |
| FR-MSG-2 | `nvfix` | Encode the original AMPS payload as opaque UTF-8 bytes in `data`; do not parse fields or delimiters |

Other AMPS protocol message types are not implemented.

### 4.8 Record shape

Each `SourceRecord` shall have:

**Key** (schema `com.singlestore.kafka.connect.amps.AMPSKey`)

```json
{ "topic": "<amps.topic>" }
```

**Value** (schema `com.singlestore.kafka.connect.amps.AMPSValue`, version 2)

| Field | Connect type | Description |
|-------|--------------|-------------|
| `bookmark` | optional string | AMPS bookmark or null |
| `timestamp` | int64 | Connector receive time (epoch ms) |
| `data` | optional bytes | Opaque AMPS payload encoded as UTF-8 bytes |
| `topic` | string | AMPS topic |

The value schema is named so SingleStore pipelines can declare a matching `output.value.schema`.
JSON and NVFIX use the same schema. Payloads are not parsed or validated by the connector; Avro
preserves the bytes while JSON output mode base64-encodes them.

Kafka partition on the record is unset (`null`). The connector copies only the payload and bookmark off the AMPS message.

---

## 5. Configuration catalog

### 5.1 Required

| Property | Type |
|----------|------|
| `amps.uri` | string |
| `amps.topic` | string |

### 5.2 Optional

| Property | Default | Notes |
|----------|---------|--------|
| `amps.client.name` | `kafka-connect-amps-` | Concatenated with `amps.topic` |
| `kafka.topic.prefix` | `amps-` | |
| `amps.position` | `EPOCH` | `EPOCH` or `LATEST` |
| `amps.start.bookmark` | empty | Overrides offsets and position |
| `amps.subscription.type` | `subscribe` | `subscribe`, `sow_and_subscribe`, `sow` |
| `amps.filter` | empty | AMPS content filter |
| `batch.size` | `100` | 1–10000 |
| `poll.timeout.ms` | `1000` | 100–60000; empty subscribe polls and SOW drains |
| `amps.connect.timeout.ms` | `15000` | 1000–120000; HAClient logon retry budget |
| `poll.idle.ms` | `200` | 10–5000; subscribe / sow_and_subscribe only, after the first record |
| `amps.sow.query.batch.size` | `0` | 0–10000; SOW only |
| `amps.auth.type` | `NONE` | `NONE`, `PASSWORD`, `KERBEROS_GSSAPI` |
| `amps.username` | empty | Required for PASSWORD |
| `amps.password` | null | Required for PASSWORD |
| `amps.kerberos.spn` | empty | Required for KERBEROS_GSSAPI |
| `amps.kerberos.jaas.entry` | `AMPSClient` | |
| `amps.kerberos.jaas.config` | empty | Path to JAAS file |
| `amps.kerberos.keytab` | empty | With client principal if no JAAS path |
| `amps.kerberos.client.principal` | empty | |
| `amps.kerberos.lifetime.seconds` | `28800` | 60–2592000 |
| `amps.kerberos.debug` | `false` | Sets `sun.security.krb5.debug` for extractor stderr traces |
| `amps.kerberos.force.tcp` | `true` | Wraps host krb5.conf so Java GSS prefers TCP to the KDC |
| `amps.kerberos.krb5.conf` | empty | Host krb5.conf path; default `/etc/krb5.conf` |
| `amps.ssl.trust.certificate` | empty | PEM/DER CA or server cert path(s) |
| `amps.ssl.truststore` | empty | JKS/PKCS12 truststore |
| `amps.ssl.truststore.password` | null | |
| `amps.ssl.keystore` | empty | Client keystore for mTLS |
| `amps.ssl.keystore.password` | null | Password for the keystore and its key entry, when the file has one |

### 5.3 Host properties (not defined by this connector)

`connector.class`, `tasks.max`, converters, and SingleStore pipeline DDL are supplied by the host. `tasks.max` greater than 1 does not create additional AMPS tasks.

---

## 6. Non-functional requirements

| ID | Requirement | Current behavior |
|----|-------------|------------------|
| NFR-1 | Predictable memory | No standing unbounded queue; per-poll list capped at `batch.size` (SOW may buffer AMPS-side batches until drained) |
| NFR-2 | Logging | SLF4J; start/stop, SOW progress, conversion errors |
| NFR-3 | Secrets | Password type for `amps.password`; Kerberos keytab path only (keytab file not bundled) |

---

## 7. Explicitly out of scope (not supported)

The following are **not** implemented. They are listed so this document is not read as matching the 60East AMPS Kafka connector.

- Sink / publish to AMPS
- AMPS queues (`isQueue`, queue ack)
- Multiple tasks / `taskFilter` partitioning
- AMPS command `options` (projection, grouping, conflation, timestamp, etc.)
- Topic regex subscriptions
- Logged bookmark store / `commitRecord` bookmark discard
- Kafka Connect offset maps left empty (this connector always sets partition/offset as in §4.5)
- Raw byte values and Kafka headers (`eventHeaders`)
- `cmdTypeFilter` bitmask
- Comma-separated HA URI lists
- AMPS XML / protobuf / binary message types
- Dynamic connector reconfiguration
- Compatibility with 60East property names (`uri`, `ampsTopic`, `clientName`, `maxBatch`, …)

---

## 8. Assumptions and constraints

1. AMPS transaction log is enabled when bookmark replay (`EPOCH` / stored offset / `amps.start.bookmark`) is required.
2. The `amps.uri` message-type segment is `json` or `nvfix`. The connector does not parse the payload.
3. SingleStore pipeline batches wait for every task’s `poll()`; empty polls wait `poll.timeout.ms`, so keep that modest. Partial batches return after `poll.idle.ms`.
4. SOW mode is a snapshot, not a live tail; live updates require `subscribe` or `sow_and_subscribe`.
5. Bookmark subscribe fallback may skip transaction-log positioning if the AMPS topic does not support bookmarks.

---

## 9. Traceability (code)

| Area | Primary types |
|------|----------------|
| Connector SPI | `AMPSSourceConnector`, `AMPSSourceTask` |
| Config | `AMPSSourceConnectorConfig`, `SubscriptionType`, `Position`, `AuthType` |
| Client | `AMPSClientFactoryImpl`, `KerberosJaasMaterializer`, `KerberosAuthServerChooser` |
| Records | `RecordConverter`, `AMPSMessageData` |
| Discovery | `src/main/resources/META-INF/services/org.apache.kafka.connect.source.SourceConnector` |
