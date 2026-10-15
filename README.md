# Introduction

This connector reads messages from [AMPS](https://www.crankuptheamps.com/) and writes them as Kafka Connect `SourceRecord`s. The same jar works with Apache Kafka Connect and with SingleStore `CREATE PIPELINE` custom connectors.

**Connector class:** `com.singlestore.kafka.connect.amps.AMPSSourceConnector`  
**Short name:** `AMPSSourceConnector`

`subscribe` and `sow_and_subscribe` subscribe from the current AMPS bookmark, collect up to `batch.size` messages, then unsubscribe. `sow` streams one State of the World snapshot. After that snapshot is delivered, the next `poll()` is empty so a FOREGROUND extract can stop. The `poll()` after that starts another full snapshot. Use `sow` with `START PIPELINE ... FOREGROUND` or `TEST PIPELINE`. A background `START PIPELINE` keeps polling and loads snapshot after snapshot.

# Build

```bash
mvn clean package -DskipTests
```

The jar is `target/kafka-connect-amps-0.1.0.jar`. Copy it to the Kafka Connect plugin path or to the SingleStore extractor library directory, then restart the worker.

# Configuration

## AMPSSourceConnector

| Name | Description | Type | Default | Valid Values | Importance |
|------|-------------|------|---------|--------------|------------|
| `amps.uri` | AMPS server URI. | string | | `tcp://host:9007/amps/json` or `tcps://host:9008/amps/nvfix` | high |
| `amps.topic` | AMPS topic to read. | string | | | high |
| `amps.client.name` | Client name prefix. Final name is the prefix plus `amps.topic`. | string | `kafka-connect-amps-` | | medium |
| `kafka.topic.prefix` | Prefix for the Kafka topic name. Final topic is prefix plus `amps.topic`. | string | `amps-` | | medium |
| `amps.subscription.type` | How each poll reads AMPS. | string | `subscribe` | `subscribe`, `sow_and_subscribe`, `sow` | medium |
| `amps.position` | Start position when no stored bookmark exists. Ignored by `sow`. | string | `EPOCH` | `EPOCH`, `LATEST` | medium |
| `amps.start.bookmark` | Bookmark that overrides stored offsets and `amps.position`. Ignored by `sow`. | string | | | low |
| `amps.filter` | AMPS content filter. Applied for every subscription type. | string | | | low |
| `batch.size` | Maximum records returned from one `poll()`. | int | `100` | [1,...,10000] | medium |
| `amps.sow.query.batch.size` | AMPS SOW response batch size. `0` inherits `batch.size`. Used when `amps.subscription.type=sow`. | int | `0` | [0,...,10000] | low |
| `poll.timeout.ms` | Max wait for the first record, or while draining SOW. | int | `1000` | [100,...,60000] | low |
| `poll.idle.ms` | After the first record in `subscribe` or `sow_and_subscribe`, return when AMPS has been quiet this long. | int | `200` | [10,...,5000] | low |
| `amps.connect.timeout.ms` | Max time for connect and logon. A refused logon fails the task. | int | `15000` | [1000,...,120000] | low |
| `amps.auth.type` | Authentication mode. | string | `NONE` | `NONE`, `PASSWORD`, `KERBEROS_GSSAPI` | medium |
| `amps.username` | User for `PASSWORD`, or the logon user id for Kerberos when the JAAS file does not supply a principal. | string | | | medium |
| `amps.password` | Password for `PASSWORD`. Not logged. | password | | | medium |
| `amps.kerberos.spn` | AMPS service principal, for example `AMPS/amps-host.example.com`. Required for `KERBEROS_GSSAPI`. | string | | | medium |
| `amps.kerberos.jaas.entry` | JAAS entry name. | string | `AMPSClient` | | low |
| `amps.kerberos.jaas.config` | Path to a JAAS file. Omit it when `amps.kerberos.keytab` and `amps.kerberos.client.principal` are set. | string | | | low |
| `amps.kerberos.keytab` | Client keytab path. Do not copy the AMPS server keytab to the leaves. | string | | | low |
| `amps.kerberos.client.principal` | Client principal, for example `kafka-connect@CORP.EXAMPLE.COM`. | string | | | low |
| `amps.kerberos.lifetime.seconds` | Kerberos credential lifetime. | int | `28800` | [60,...,2592000] | low |
| `amps.kerberos.debug` | When true, JDK Kerberos traces go to extractor stderr. | boolean | `false` | | low |
| `amps.kerberos.force.tcp` | When true, copy the host `krb5.conf` and force TCP/88. Does not edit `/etc/krb5.conf`. | boolean | `true` | | low |
| `amps.kerberos.krb5.conf` | Host `krb5.conf` to copy when `amps.kerberos.force.tcp=true`. Empty means `/etc/krb5.conf`. | string | | | low |
| `amps.ssl.trust.certificate` | Comma-separated PEM or DER CA or server certificates. For `tcps://`. | string | | | medium |
| `amps.ssl.truststore` | JKS or PKCS12 truststore. Alternative or addition to the PEM. | string | | | medium |
| `amps.ssl.truststore.password` | Truststore password. | password | | | medium |
| `amps.ssl.truststore.type` | Truststore type. Empty lets Java detect it. | string | | | low |
| `amps.ssl.keystore` | Client keystore for mutual TLS. | string | | | low |
| `amps.ssl.keystore.password` | Password for `amps.ssl.keystore` and its key entry, when the file has one. | password | | | low |
| `amps.ssl.keystore.type` | Keystore type. Empty lets Java detect it. | string | | | low |

`PASSWORD` requires `amps.username` and `amps.password`. Prefer `tcps://`. `KERBEROS_GSSAPI` requires `amps.kerberos.spn` and either a JAAS file or a keytab plus client principal.

SSL settings are rejected on a `tcp://` URI. The connector installs an AMPS-only `SSLContext`. It does not change the JVM truststore.

The connector creates one task per AMPS topic. Every record uses the same source partition, `{ "topic": "<amps.topic>" }`.

## Example

```properties
name=amps-source-connector
connector.class=com.singlestore.kafka.connect.amps.AMPSSourceConnector
tasks.max=1
amps.uri=tcp://localhost:9007/amps/json
amps.topic=market-ticker
amps.subscription.type=subscribe
amps.position=EPOCH
batch.size=100
kafka.topic.prefix=amps-
```

SingleStore pipeline:

```sql
CREATE PIPELINE market_json AS
LOAD DATA KAFKA 'amps-market-ticker'
CONFIG '{
  "connector.class": "AMPSSourceConnector",
  "amps.uri": "tcps://amps-host:9008/amps/json",
  "amps.topic": "market-ticker",
  "amps.subscription.type": "subscribe",
  "amps.auth.type": "PASSWORD",
  "amps.username": "ampsusr",
  "amps.password": "secret",
  "amps.ssl.trust.certificate": "/etc/memsql/amps-ca.pem"
}'
INTO TABLE market_json
FORMAT JSON;
```

`ALTER PIPELINE ... SET CONFIG` replaces the whole CONFIG object. Keep every property when you change one.

# Data

Offsets for `subscribe` and `sow_and_subscribe` are `{ "bookmark": "<amps bookmark>" }`. `sow` rows have no AMPS bookmark. Each row is `{ "snapshot": <id>, "sequence": <n> }`. A later snapshot uses a new id so its sequences stay above the previous commit.

## com.singlestore.kafka.connect.amps.AMPSKey

| Name | Optional | Schema | Documentation |
|------|----------|--------|---------------|
| topic | false | String | AMPS topic the message was received from. Used as the source partition. |

## com.singlestore.kafka.connect.amps.AMPSValue

Version 2. JSON and NVFIX URIs use this schema. `data` is opaque UTF-8 bytes. The connector does not parse the payload; the message type is the segment in `amps.uri`.

| Name | Optional | Schema | Documentation |
|------|----------|--------|---------------|
| bookmark | true | String | AMPS bookmark when the message has one. Null for a SOW query row. |
| timestamp | false | INT64 | Time the connector copied the message, in epoch milliseconds. |
| data | true | Bytes | Opaque AMPS payload. JSON pipeline output base64-encodes this field. |
| topic | false | String | AMPS topic name. |

`CREATE PIPELINE` schemas must match these structs. With `FORMAT AVRO`, `data` stays binary. Map it to `LONGBLOB` or `VARBINARY`, or decode UTF-8 in the pipeline when you need text.

# Requirements

- Java 11 or higher
- Apache Kafka 3.x with Kafka Connect, or SingleStore Pipelines
- AMPS client 5.3.2.0, bundled in the connector jar
- AMPS transaction log for bookmark resume (`subscribe` and `sow_and_subscribe`)
- AMPS SOW configured on the topic for `sow` and `sow_and_subscribe`
