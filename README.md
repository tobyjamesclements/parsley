# Parsley

Causal delivery order for Kafka Streams. Producers stamp each record with its causal dependencies.
Kafka Streams processors hold back any record whose dependencies have not yet been observed, and
release the record once the frontier catches up.

When co-partitioning and closed processor effects hold, if A causally precedes B, every Kafka
Streams processor that subscribes to both topics processes A before B.

## Install

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).

```xml
<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0</version>
</dependency>
```

0.1.0 is an early release. The causal guarantee is implemented and tested, but the fault-injection
suite that exercises it under crashes, rebalances, and partitions lands in 1.0.

Requires Java 25. Build from source with `./mvnw install`.

## Docs

**[tobyjamesclements.github.io/parsley](https://tobyjamesclements.github.io/parsley)**

Covers concepts, getting started, Streams integration, configuration, and internals.
