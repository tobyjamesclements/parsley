# Parsley

Causal consistency for Kafka — producers that encode a vector clock onto every message,
consumers that deliver multiple topics in causal order, and Kafka Streams topologies built from
causally consistent processors.

A producer stamps each message with a **vector clock** (a snapshot of its causal state). A
consumer keeps a **frontier** (its own vector clock) and holds back any message whose causal
dependencies it has not yet observed, releasing it once the frontier catches up. The whole library
is a single jar with three entry points — producer, consumer, and a Streams processor — over a
shared vocabulary of value types.

## The problem

A consumer reads a price from `prices-0` at offset 27 and, on that basis, produces an order to
`orders`. A downstream consumer of `orders` processes that order while its own `prices` consumer is
only at offset 24 — acting on an order whose causal premise it has not yet seen. This is a causal
violation, and it happens under normal operation (consumer lag on one topic racing ahead of a write
to another). Kafka's per-partition ordering does not prevent it; Parsley does.

The guarantee is **causal consistency**: if A causally precedes B, every consumer observes A before
B — stronger than eventual consistency, weaker than linearisability, with a predictable rather than
load-dependent latency cost.

## How it works

Every message carries the producer's vector clock as a header. When a message arrives, the consumer
checks whether its frontier already satisfies that clock; if so it forwards immediately, otherwise
it holds the message in a **causal buffer** until the frontier catches up. If the frontier never
catches up, a configurable **buffering policy** forwards out-of-order, drops, or dead-letters the
record.

The consumer and the processor supplier run on Kafka Streams internally, inheriting its
changelog-backed state stores, rebalance-safe restoration, and transactional forwarding. The
frontier is persisted before each record is forwarded, so it survives restarts.

## Getting started

Parsley is published to [GitHub Packages](https://github.com/tobyjamesclements?tab=packages&repo_name=parsley),
which requires a GitHub account and a [PAT](https://github.com/settings/tokens) with `read:packages`
even for public repos. Add credentials to `~/.m2/settings.xml` (a `<server>` with id `github-parsley`),
then declare the repository and dependency:

```xml
<repository>
  <id>github-parsley</id>
  <url>https://maven.pkg.github.com/tobyjamesclements/parsley</url>
</repository>

<dependency>
  <groupId>io.parsley</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`parsley` pulls in `kafka-streams` and `kafka-clients` transitively. Java 25 is required
(`--release 25`). Build from source with `./mvnw install`; the suite is verified with
[PIT](https://pitest.org) via `./mvnw -Pmutation test` (report-only).

## Usage

**Consume in causal order** — a drop-in replacement for a plain Kafka consumer:

```java
try (CausalConsumer<String, String> consumer = CausalConsumers.<String, String>builder(
        List.of("prices", "orders"),
        CausalBufferingPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "my-group"),
        Map.of(StreamsConfig.APPLICATION_ID_CONFIG,    "my-app",
               StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")).build()) {

    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    records.forEach(this::process);
}
```

**Produce with causal context** — pass the consumer's frontier; it is attached as a header:

```java
producer.send(new ProducerRecord<>("orders", key, value), consumer.frontier());
```

**Inside a Streams topology** — write an ordinary Kafka Streams `Processor` and wrap its supplier
with `CausalProcessors.builder(...).build()`. Inside your `process()`, every state-store read/write and every `forward`
is causally ordered and the outgoing record is stamped with the current vector clock — transparently,
with no `CausalProducer` on egress (Streams sinks carry the stamped header out to the topic). You
never see a vector clock, a frontier, or a buffer; the required state stores are registered for you:

```java
ProcessorSupplier<String, Order, String, Enriched> user = new ProcessorSupplier<>() {
    public Processor<String, Order, String, Enriched> get() { return new EnrichOrder(); }
    public Set<StoreBuilder<?>> stores() { return Set.of(pricesStateBuilder); }   // your own stores
};

builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
       .process(CausalProcessors.builder(user, CausalBufferingPolicy.deadLetter(limit, "parsley-dlq"))
                               .serdes(Serdes.String(), orderSerde).onViolation(onViolation)
                               .deadLetterSink(deadLetterSink).build())
       .to("output-topic");
```

Held records are persisted to a changelog-backed buffer store (serialised with the serdes you pass,
resolved per source topic) so they survive a restart. The guarantee holds for every admitted record
under any policy, and for every record under a **strict** policy (`deadLetter`/`drop`); the lenient
`forwardUnsafe` policy preserves delivery by forwarding un-satisfied records under sustained lag,
suspending the guarantee for exactly those records (each flagged via the `CausalViolationHandler`, with the
causal gap). Three preconditions apply — closed effects, co-partitioning, and acceptance of the
policy — documented on `CausalProcessorSupplier` and `CausalProcessors`.

**Propagate across services** — a vector clock is also a causal token you can hand another service
(e.g. over HTTP) so it reads consistently elsewhere. Extract it from a consumed record, serialise,
and apply *your own* encryption/transport (Parsley ships none); the receiver rebuilds it and gates
its read:

```java
CausalDependencies context = CausalDependencies.fromRecord(reply).orElseGet(consumer::frontier);
send(myCipher.encrypt(context.toBytes()));                 // → HTTP, your transport
// receiver: CausalDependencies.fromBytes(...).satisfiedBy(localFrontier)
```

## Operating notes

A causal processor only helps when records arrive from multiple partitions concurrently — multiple
topics, or multiple partitions on one instance. With a single partition from a single topic, Kafka
already gives total order and Parsley only adds overhead.

For dependencies to be evaluated correctly, an instance must be assigned **all** partitions in a
causally related set. The recommended approach is to co-partition related topics so related messages
share a partition number across topics, letting each instance own partition N everywhere. **Parsley
does not detect or enforce co-partitioning** — a misconfigured topology evaluates against an
incomplete partition set silently.

## Design notes

- **Kafka Streams internally.** Using `ProcessorSupplier` as the integration point inherits
  changelog-backed state, rebalance-safe restoration, and transactional forwarding for free.
- **Frontier on every produce.** The producer holds no frontier of its own; passing the clock at
  each `send` makes the causal claim explicit at the call site.
- **One concrete `CausalDependencies`.** Parsley targets Kafka only, so the clock is a single concrete
  value type that owns its wire format — no broker-neutral, self-typed indirection the library would
  never use.
