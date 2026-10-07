# AMPS Source Connector for SingleStore Pipelines

**Product specification**  
**Version:** 0.1.0 (pre-GA)  
**Audience:** Customers deploying AMPS ingestion into SingleStore (or Apache Kafka Connect)

This specification describes the current pre-GA product. It is not a GA release, not a roadmap, and not a substitute for the 60East AMPS Kafka connector.

---

## 1. Overview

The AMPS Source Connector reads messages from **one AMPS topic** and delivers them as Kafka Connect `SourceRecord`s. You can run the same shaded JAR with:

- SingleStore **custom pipelines** (`CREATE PIPELINE` with a Kafka Connect source plugin)
- Apache Kafka Connect (standalone or distributed)

The connector is **source only**. It does not publish data back to AMPS.

| Item | Value |
|------|--------|
| Connector class | `com.singlestore.kafka.connect.amps.AMPSSourceConnector` |
| Short name | `AMPSSourceConnector` |
| Artifact | `kafka-connect-amps-0.1.0.jar` (shaded; AMPS Java client included) |
| Java | 11 or later |
| AMPS client | 5.3.0.0 |

SingleStore can load the plugin by short class name because the JAR includes a ServiceLoader manifest for `org.apache.kafka.connect.source.SourceConnector`.

---

## 2. What you can do

### Ingest from AMPS

- Connect to an AMPS server using a standard AMPS URI (`tcp://host:port/amps/json` or `.../amps/nvfix`).
- Read a single named AMPS topic.
- Optionally apply an AMPS **content filter** so filtering happens on the server.

### Choose how to read the topic

| Mode | Config value | Typical use |
|------|----------------|-------------|
| Bookmark subscribe | `subscribe` (default) | Replay from the transaction log and continue with live messages |
| Snapshot then live | `sow_and_subscribe` | Current State of the World, then live updates |
| Snapshot only | `sow` | One full SOW snapshot per FOREGROUND extract; the poll after the empty end-of-file starts another snapshot |

Bookmark replay (`subscribe` / `sow_and_subscribe`) requires the AMPS **transaction log**.

### Resume after restart

The connector records each message’s AMPS **bookmark** as the Kafka Connect source offset. After a successful pipeline (or Connect) commit, a restart continues from the last stored bookmark for that topic—unless you override start position (see §6).

`sow` mode always reads the **current** snapshot. It does not resume from a bookmark.

### Payload handling

| `amps.message.type` | Result |
|---------------------|--------|
| `json` (default) | Original AMPS payload encoded as opaque UTF-8 bytes in `data` |
| `nvfix` | Original AMPS payload encoded as opaque UTF-8 bytes in `data` |

Set `amps.message.type` to match the message-type segment of `amps.uri`.

### Authenticate

| `amps.auth.type` | When to use |
|------------------|-------------|
| `NONE` (default) | Open AMPS instance; no credentials |
| `PASSWORD` | Username/password (AMPS LDAP, file/htpasswd, or other password module) |
| `KERBEROS_GSSAPI` | Kerberos GSSAPI against the AMPS SPN |

Password values support Kafka Connect ConfigProviders (for example `${file:...}`) so secrets need not sit in plaintext pipeline config.

For Kerberos, supply either a JAAS file path or a keytab plus client principal (the connector can write a temporary JAAS file from a bundled template).

### Bound memory

For `subscribe` and `sow_and_subscribe`, each poll creates a short-lived AMPS subscription, copies at most `batch.size` messages, then unsubscribes. That avoids an unbounded in-memory queue during large EPOCH replays.

---

## 3. Behavior in SingleStore pipelines

SingleStore drives the connector like a Kafka Connect source:

1. Pipeline properties and credentials are passed into connector `start()`.
2. The connector starts **one** task for the configured AMPS topic.
3. SingleStore calls `poll()` on that task, writes successful records in a **single transaction**, then stores offsets.
4. If a poll or the transaction fails, the next poll resumes from the last committed bookmark for that topic.
5. If AMPS has no new data, `poll()` waits up to `poll.timeout.ms` then returns an empty list. After at least one record, it returns when AMPS has been quiet for `poll.idle.ms` (or sooner if `batch.size` is reached).

**Recommendations for pipelines**

- Set `connector.class` to `AMPSSourceConnector` (or the fully qualified name).
- Keep `poll.timeout.ms` modest (for example 100–1000) so empty polls do not stall the pipeline. Raise `batch.size` for backfills; incomplete batches return after `poll.idle.ms` without waiting out the timeout.
- `tasks.max` greater than 1 does **not** split one AMPS topic across tasks. All records share one source partition (`topic` = AMPS topic). Two tasks must not emit the same partition.

`empty.backoff.ms` is accepted for compatibility and is **ignored**.

---

## 4. Record format

Each record’s Kafka / pipeline topic name is:

```text
{kafka.topic.prefix}{amps.topic}
```

Default prefix is `amps-`. Example: AMPS topic `orders` → `amps-orders`.

### Key

```json
{ "topic": "orders" }
```

### Value payload

```json
{
  "bookmark": "3850692670110263676|1774627282001110361|",
  "timestamp": 1715448123456,
  "data": "<opaque bytes>",
  "topic": "orders"
}
```

`data` holds the AMPS payload as opaque UTF-8 bytes. The connector does not parse or validate JSON or NVFIX payloads. `timestamp` is when the connector received the message (milliseconds since Unix epoch), not the AMPS server timestamp. Avro preserves the bytes; JSON output mode base64-encodes them.

### Schemas

Key and value both carry named Connect schemas: `com.singlestore.kafka.connect.amps.AMPSKey` and `com.singlestore.kafka.connect.amps.AMPSValue` (version 2). SingleStore pipelines must declare `output.key.schema` and `output.value.schema` to match. Field types are: `bookmark` optional string, `timestamp` int64, `data` optional bytes, and `topic` string. JSON and NVFIX use the same value schema.

Offset metadata used for resume: source partition `{ "topic": "<amps.topic>" }`, source offset `{ "bookmark": "<amps bookmark>" }` when AMPS provides a bookmark.

---

## 5. Configuration

### Required

| Property | Description |
|----------|-------------|
| `amps.uri` | AMPS URI, for example `tcp://amps.example.com:9007/amps/json` |
| `amps.topic` | AMPS topic name |

### Connection and topic

| Property | Default | Description |
|----------|---------|-------------|
| `amps.client.name` | `kafka-connect-amps` | Client name prefix. The live client name is `{prefix}-{topic}`. |
| `kafka.topic.prefix` | `amps-` | Prefix for the output topic name |
| `amps.filter` | _(empty)_ | AMPS content filter, for example `/price > 100` |

### Read position and mode

| Property | Default | Description |
|----------|---------|-------------|
| `amps.subscription.type` | `subscribe` | `subscribe`, `sow_and_subscribe`, or `sow` |
| `amps.position` | `EPOCH` | Used when there is no stored offset: `EPOCH` (beginning of the transaction log) or `LATEST` (new messages only) |
| `amps.start.bookmark` | _(empty)_ | If set, starts from this AMPS bookmark and **overrides** stored offsets and `amps.position` |

### Batching

| Property | Default | Description |
|----------|---------|-------------|
| `batch.size` | `100` | Maximum records returned from one `poll()` (1–10000) |
| `poll.timeout.ms` | `1000` | Max wait for the first record after subscribe / while draining SOW (100–60000) |
| `amps.connect.timeout.ms` | `15000` | Max time to connect and log on at task start (1000–120000). Refused logon fails the task instead of retrying forever. |
| `poll.idle.ms` | `200` | After the first record, return when AMPS has been quiet this long (10–5000) |
| `amps.sow.query.batch.size` | `0` | AMPS-side SOW batch size when mode is `sow`. `0` means use `batch.size`. Maximum 10000. |
| `empty.backoff.ms` | `100` | Deprecated. Ignored. |

### Payload

| Property | Default | Description |
|----------|---------|-------------|
| `amps.message.type` | `json` | `json` or `nvfix` |

### Authentication

| Property | Default | Description |
|----------|---------|-------------|
| `amps.auth.type` | `NONE` | `NONE`, `PASSWORD`, or `KERBEROS_GSSAPI` |
| `amps.username` | _(empty)_ | Required for `PASSWORD` |
| `amps.password` | _(unset)_ | Required for `PASSWORD` |
| `amps.kerberos.spn` | _(empty)_ | Required for `KERBEROS_GSSAPI`, for example `AMPS/amps-host.example.com` |
| `amps.kerberos.jaas.config` | _(empty)_ | Path to a JAAS file **or** omit and use keytab + principal |
| `amps.kerberos.keytab` | _(empty)_ | Keytab path (with client principal) if no JAAS file |
| `amps.kerberos.client.principal` | _(empty)_ | Principal in the keytab |
| `amps.kerberos.jaas.entry` | `AMPSClient` | JAAS login context name |
| `amps.kerberos.lifetime.seconds` | `28800` | Credential lifetime (8 hours) |
| `amps.kerberos.debug` | `false` | When `true`, sets `sun.security.krb5.debug` so JDK Kerberos traces go to extractor stderr |
| `amps.kerberos.force.tcp` | `true` | Wrap host `krb5.conf` with `udp_preference_limit=1` so Java uses TCP/88 (no host file edit) |
| `amps.kerberos.krb5.conf` | _(empty → `/etc/krb5.conf`)_ | Source file for the TCP wrap |
| `amps.ssl.trust.certificate` | _(empty)_ | PEM/DER CA or server certificate path(s) for `tcps://` |
| `amps.ssl.truststore` | _(empty)_ | Java truststore as an alternative to the PEM |
| `amps.ssl.truststore.password` | _(unset)_ | |
| `amps.ssl.keystore` | _(empty)_ | Client keystore (mutual TLS only) |
| `amps.ssl.keystore.password` | _(unset)_ | Required with keystore |

Prefer `tcps://` when using `PASSWORD` so credentials are not sent in the clear. For `tcps://`, set `amps.ssl.trust.certificate` or `amps.ssl.truststore` in CONFIG so leaves do not need a JVM-wide truststore.

---

## 6. Start position

On task start, bookmark-based modes (`subscribe`, `sow_and_subscribe`) choose a starting bookmark in this order:

1. **`amps.start.bookmark`** if it is non-empty  
2. Else the **last committed offset** for `{ "topic": "<amps.topic>" }`  
3. Else **`amps.position`**: `EPOCH` or `LATEST`

`sow` ignores this sequence and always queries the current SOW from the beginning of that snapshot.

---

## 7. Example configurations

### Live / replay (JSON)

```properties
connector.class=AMPSSourceConnector
amps.uri=tcp://amps.example.com:9007/amps/json
amps.topic=orders
amps.message.type=json
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

The snapshot is streamed across as many polls as needed. After it completes, the connector closes its AMPS connection and releases the client name, then returns one empty poll. The following poll starts another full snapshot. That matches a FOREGROUND extract, which stops on the first empty batch. A background pipeline does not stop and will load snapshot after snapshot.

### NVFIX with filter

```properties
connector.class=AMPSSourceConnector
amps.uri=tcp://amps.example.com:9007/amps/nvfix
amps.topic=orders-nvfix
amps.message.type=nvfix
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

## 8. Operational notes

- **One topic per connector instance.** To ingest several AMPS topics, deploy one pipeline (or Connect connector) per topic.
- **Ordering.** Messages are processed in AMPS subscription order for that single task. There is no multi-task parallel split.
- **Failed conversion.** A message that cannot be converted is logged and skipped; other messages in the batch are still returned.
- **Bookmark fallback.** If AMPS rejects a bookmark subscription for the topic, the connector falls back to a regular subscribe, which may not resume from the transaction log.
- **SOW vs live.** Use `sow` for a one-time snapshot. Use `subscribe` or `sow_and_subscribe` for ongoing ingestion.
- **Logon failures.** A refused AMPS logon (empty credentials, Kerberos failure, name in use) fails `start()` after `amps.connect.timeout.ms` (default 15s) instead of retrying forever.
- **Worker JVM (Kerberos).** If `java.security.auth.login.config` is already set on the worker, the connector does not override it. That JAAS file must define the entry named by `amps.kerberos.jaas.entry`.

---

## 9. Requirements and limits

**You need**

- Java 11+
- AMPS server reachable from the SingleStore / Connect host
- Transaction log on AMPS for bookmark replay and restart resume (`subscribe` / `sow_and_subscribe`)

**This product does not include**

- Writing (sinking) data to AMPS
- AMPS queue subscriptions and queue acknowledgements
- Multiple tasks over one topic (hash/`MOD` style splits)
- AMPS command options such as projection, grouping, or conflation
- Topic-name regular expressions
- Message types other than JSON and NVFIX
- Drop-in compatibility with 60East connector property names (`uri`, `ampsTopic`, `maxBatch`, and similar)

Those capabilities belong to other products (including the official 60East AMPS Kafka connector) and are outside this specification.

---

## 10. Support identity

When opening a support case, include:

- Connector version **0.1.0** (pre-GA)
- Connector class `com.singlestore.kafka.connect.amps.AMPSSourceConnector`
- `amps.subscription.type`, `amps.message.type`, and `amps.auth.type`
- Whether the host is SingleStore pipeline or Kafka Connect
