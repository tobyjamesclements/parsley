# Parsley

Causal delivery order for Kafka Streams. Producers stamp each record with its causal dependencies.
Kafka Streams processors hold back any record whose dependencies have not yet been observed, and
release the record once the frontier catches up.

When co-partitioning and closed processor effects hold, if A causally precedes B, every Kafka
Streams processor that subscribes to both topics processes A before B.

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

0.1.0 is an early release. The causal guarantee is implemented and tested, but the fault-injection
suite that exercises it under crashes, rebalances, and partitions lands in 1.0. Pre-1.0 versions
have no upgrade path between versions: upgrading is a fresh start (new state, new offsets), because
wire formats and the public API change without compatibility aliases until 1.0.

Requires Java 25. Build from source with `./mvnw install`.

## Docs

**[tobyjamesclements.github.io/parsley](https://tobyjamesclements.github.io/parsley)**

Covers concepts, getting started, Streams integration, configuration, and internals.
