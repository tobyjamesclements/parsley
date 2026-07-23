# Parsley

Causal delivery order for Kafka Streams. Producers stamp each record with its causal dependencies,
and Kafka Streams processors hold back any record whose dependencies have not yet been observed,
releasing it once the frontier catches up. The guarantee is the causal-consistency model of the
distributed-systems literature (Lamport's happened-before relation, realised with vector clocks),
instantiated on Kafka coordinates: when the preconditions hold, if A causally precedes B, every
processor that subscribes to both of their topics processes A before B.

Internally Parsley is a stack of three protocols, presented in the module style of Cachin,
Guerraoui, and Rodrigues: a **channels** layer that adapts Kafka topic-partitions into reliable
FIFO channels, a **causal broadcast** layer (Birman–Schiper–Stephenson) that delivers in causal
order, and a **gossip** layer (Chandy–Misra–Bryant null messages) that keeps causal progress
observable through non-emitting processors. The preconditions are co-partitioning by key, closed
processor effects, and stamping by every processor between causally related topics.

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

Leads with the causal-consistency foundations and the three protocols, then covers getting
started, Streams integration, configuration, and reference.
