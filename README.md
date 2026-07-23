# Parsley

Causal delivery order for Kafka Streams. Producers stamp each record with its causal dependencies.
Kafka Streams processors hold back any record whose dependencies have not yet been observed, and
release the record once the frontier catches up.

When the preconditions hold — co-partitioning by key, closed processor effects, and stamping by
every processor between causally related topics — if A causally precedes B, every Kafka Streams
processor that subscribes to both topics processes A before B.

Joining a running topology needs no coordination: a new application simply starts consuming, its
replay self-gates into causal delivery order, and its truthful stamps make its outputs correctly
gated everywhere from its first emission.

## Install

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).

```xml
<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0</version>
</dependency>
```

0.1.0 is an early release. Pre-1.0 versions have no upgrade path between versions: upgrading is a
fresh start (new state, new offsets), because wire formats and the public API change without
compatibility aliases until 1.0.

Requires Java 21 or later, and Kafka brokers version 3.7.0 or later — the minimum supported
broker; the integration suite runs against both 3.7.0 and the current stable broker line.
Building from source needs JDK 25; build with `./mvnw install`.

## Docs

**[tobyjamesclements.github.io/parsley](https://tobyjamesclements.github.io/parsley)**

Covers concepts, getting started, Streams integration, configuration, and internals.
