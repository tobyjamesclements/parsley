# Parsley

Causal consistency for Kafka — producers that encode a vector clock onto every message,
consumers that deliver multiple topics in causal order, and Kafka Streams topologies built from
causally consistent processors.

---

## What is Parsley?

Parsley enforces causal message ordering across Kafka topics. A producer stamps each message
with a **vector clock** — a snapshot of its causal state. A consumer maintains a **frontier**
(its own vector clock) and holds back any message whose causal dependencies it has not yet
observed, releasing it once the frontier catches up. The whole library is a single jar with
three entry points — a producer, a consumer, and a Kafka Streams processor — over a shared
vocabulary of value types.

---

## The Problem

Consider two topics: `prices` and `orders`. A consumer reads a price update from `prices-0` at
offset 27 and, on the basis of that observation, produces an order to `orders`. A downstream
consumer of `orders` processes that order — but its consumer of `prices` is only at offset 24.
It is now acting on an order whose causal premise it has not yet observed.

This is a causal violation. It occurs under normal operating conditions — consumer lag on one
topic racing ahead of a write to another. Kafka's per-partition ordering guarantee does not
prevent it. Parsley detects and prevents this class of violation transparently.

---

## Consistency Model

Parsley provides **causal consistency** — if event A causally precedes event B, every consumer
observes A before B. This is stronger than eventual consistency but weaker than linearisability,
and carries a predictable latency overhead rather than a load-dependent one.

---

## How It Works

Every message carries a **vector clock** header — a compact snapshot of the producer's causal
state at the time of writing (a map from `TopicPartition` to the highest observed offset).

A consumer maintains a **frontier**: its own current vector clock reflecting everything it has
processed. When a message arrives, the consumer checks whether its frontier
satisfies the message's clock — i.e. whether it has already processed everything the message
causally depends on. If yes, the message is forwarded immediately. If not, it is held in a
**causal buffer** until the frontier catches up.

If the frontier never advances far enough, a configurable **buffering policy** decides what to
do: forward out-of-order, drop, or route to a dead-letter destination.

Internally the consumer and the processor supplier run on Kafka Streams, inheriting its
changelog-backed state stores, rebalance-safe restoration, and transactional forwarding. The
frontier is persisted to a state store before each record is forwarded, so it survives restarts.

---

## Getting Started

### Installation

Parsley is published to [GitHub Packages](https://github.com/tobyjamesclements?tab=packages&repo_name=parsley).
GitHub Packages requires authentication even for public repositories — you need a GitHub account
and a [personal access token](https://github.com/settings/tokens) with the `read:packages` scope.

**1. Add credentials to `~/.m2/settings.xml`**

```xml
<settings>
  <servers>
    <server>
      <id>github-parsley</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>YOUR_PAT</password>
    </server>
  </servers>
</settings>
```

**2. Add the repository and dependency to your project**

```xml
<repositories>
  <repository>
    <id>github-parsley</id>
    <url>https://maven.pkg.github.com/tobyjamesclements/parsley</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.parsley</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`parsley` brings in `kafka-streams` and `kafka-clients` transitively. Java 25 is required (the
project compiles with `--release 25`).

### Local build

```bash
git clone https://github.com/tobyjamesclements/parsley.git
cd parsley
mvn install -DskipTests
```

### Mutation testing

The test suite is verified with [PIT](https://pitest.org):

```bash
mvn -Pmutation test
```

Reports land in `target/pit-reports/index.html` (report-only, no threshold gating). The
Testcontainers broker integration tests are excluded from mutant-by-mutant execution.

---

## Usage

### Consuming with causal ordering

`CausalConsumer` is a drop-in replacement for a plain Kafka consumer that enforces causal
consistency automatically:

```java
try (CausalConsumer<String, String> consumer = CausalConsumer.create(
        List.of("prices", "orders"),
        BufferingPolicy.deadLetter(
            BufferLimit.ofDuration(Duration.ofSeconds(30)), "parsley-dead-letter"),
        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group"),
        Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG,    "my-causal-app",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

    while (running) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            process(record);
        }
    }
}
```

### Producing with causal context

Obtain the current frontier from the consumer and pass it to every produce call. The clock is
attached as a message header for downstream processors to evaluate:

```java
CausalProducer<String, String> producer = CausalProducer.create(
    Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
           ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
           ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

producer.send(new ProducerRecord<>("orders", key, value), consumer.frontier());
```

### Integrating into an existing Kafka Streams topology

Use `CausalProcessorSupplier` directly when building a custom topology with `StreamsBuilder`. The
frontier state store is registered automatically:

```java
CausalProcessorSupplier<String, String> supplier = CausalProcessorSupplier.create(
    BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
    (record, reason) -> log.warn("Causal violation on {}: {}", record.topic(), reason));

StreamsBuilder builder = new StreamsBuilder();
builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), Serdes.String()))
       .process(supplier)
       .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
streams.start();
```

`CausalStreams.process(stream, supplier)` is a convenience wrapper if you prefer chaining.

### Propagating causal context across services

A vector clock is also the unit of **causal context** you can hand to another service over HTTP —
for example, to tell a client "the reply has been processed; don't read until you've caught up to
here." This is what fence tokens used to do. Parsley deliberately ships no encryption or transport
for it (that's your concern), but the building blocks are public.

**Producing side** — after consuming a reply, take the causal context and serialise it. Use the
reply's own embedded clock for "the context of this reply", or `consumer.frontier()` for
"everything I've consumed":

```java
ConsumerRecord<String, Reply> reply = /* one record from consumer.poll(...) */;

VectorClock context = VectorClock.fromRecord(reply)   // the upstream producer's clock
        .orElseGet(consumer::frontier);               // fall back to our own frontier

String token = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(myCipher.encrypt(context.toBytes()));   // your encryption

httpResponse.setHeader("X-Causal-Token", token);
```

**Receiving side** — decode, decrypt, and gate the read until the target store has caught up:

```java
byte[] raw = myCipher.decrypt(Base64.getUrlDecoder().decode(token));
VectorClock required = VectorClock.fromBytes(raw);

if (required.satisfiedBy(myConsumer.frontier())) {
    // the store has observed everything `required` depends on — safe to read
}
```

`VectorClock.fromRecord(record)` / `fromHeaders(headers)` read the clock a Parsley-stamped message
carries without depending on the internal header name; `toBytes()` / `fromBytes()` are the wire
format; `satisfiedBy()` is the readiness check.

---

## Public API

Everything users program against lives in four packages; the implementation classes are
package-private.

| Package | Public types |
|---|---|
| `io.parsley` | `VectorClock`, `BufferingPolicy`, `BufferLimit`, `CausalViolationHandler`, `CausalViolationReason`, `CausalViolationException`, `Metrics` |
| `io.parsley.producer` | `CausalProducer` |
| `io.parsley.consumer` | `CausalConsumer` |
| `io.parsley.stream` | `CausalProcessorSupplier`, `CausalStreams` |

The component types (`CausalProducer`, `CausalConsumer`, `CausalProcessorSupplier`) are
**interfaces** with static `create(...)` **factory** methods; their implementations are
package-private `Kafka*` **decorators** (e.g. `CausalProducer` wraps a plain Kafka `Producer`).

### VectorClock

```java
public record VectorClock(Map<TopicPartition, Long> positions) {
    static VectorClock empty();
    VectorClock advance(TopicPartition tp, long offset);
    boolean satisfiedBy(VectorClock frontier);
    VectorClock merge(VectorClock other);
    byte[] toBytes();
    static VectorClock fromBytes(byte[] bytes);
    static Optional<VectorClock> fromRecord(ConsumerRecord<?, ?> record);
    static Optional<VectorClock> fromHeaders(Headers headers);
}
```

A snapshot of causal progress. `satisfiedBy(frontier)` returns `true` when `frontier` has
observed everything this clock depends on; `merge` is the causal union. `toBytes`/`fromBytes`
carry the compact wire format used for the message header and the persisted frontier.
`fromRecord`/`fromHeaders` extract the clock a Parsley-stamped message carries (see
[Propagating causal context across services](#propagating-causal-context-across-services)).

### BufferLimit

Controls when a buffered record is evicted: `ofDuration(Duration)`, `ofSize(int)`, or
`first(BufferLimit...)` (whichever constituent fires first).

### BufferingPolicy

Defines what happens to a record when its `BufferLimit` fires:

| Policy | Behaviour on eviction |
|---|---|
| `ignore(limit)` | Forward out-of-causal-order, report `LIMIT_REACHED` |
| `drop(limit)` | Discard silently, report `LIMIT_REACHED` |
| `deadLetter(limit, destination)` | Route to the named destination, report `LIMIT_REACHED` |

Every policy makes a genuine attempt at causal consistency before falling back. The dead-letter
policy is recommended for production — timed-out records are recoverable and retain their vector
clock headers for diagnostics.

### CausalViolationHandler

```java
@FunctionalInterface
public interface CausalViolationHandler {
    void onViolation(ConsumerRecord<?, ?> record, CausalViolationReason reason);
    static CausalViolationHandler throwing();
    static CausalViolationHandler noop();
}
```

Invoked when a record cannot be delivered in causal order. The offending record is handed back
as the original Kafka `ConsumerRecord`, so the handler can inspect its topic, partition, offset,
key, value, and headers. `CausalViolationReason` is one of `MISSING_HEADER` (non-Parsley
producer), `UNRESOLVABLE_CLOCK` (header present but undecodable), or `LIMIT_REACHED` (a
`BufferLimit` fired).

### Metrics

```java
public interface Metrics {
    void onMessageBuffered();
    void onMessageReleased(Duration bufferDuration);
    void onViolation(CausalViolationReason reason);
    void onFrontierAdvanced(VectorClock frontier);
    static Metrics noop();
}
```

Parsley emits no metrics or logs directly. Bind this interface to Micrometer, OpenTelemetry, or
any observability framework. `Metrics.noop()` is a do-nothing default.

---

## Prerequisites

A causal processor only provides a benefit when records arrive from multiple partitions
concurrently — i.e. the processor subscribes to **multiple topics**, or one instance is assigned
**multiple partitions**. If a processor is assigned exactly one partition from one topic, Kafka
already guarantees total order within that partition and Parsley adds overhead for no benefit.

### Co-partitioning

For a causal processor to evaluate dependencies correctly, it must be assigned all partitions in
its causally related partition set. The recommended approach is to co-partition causally related
topics so that related messages share the same partition key and partition number across topics;
each instance then handles partition N across all topics independently, preserving horizontal
scalability.

**Parsley does not detect or enforce co-partitioning.** A misconfigured topology will not fail at
startup — it will silently evaluate causal dependencies against an incomplete partition set.

---

## Design Decisions

**Why Kafka Streams internally?** Streams provides changelog-backed state stores, rebalance-safe
restoration, and transactional forwarding as primitives. Using `ProcessorSupplier` as the
integration point means Parsley inherits these guarantees without reimplementing them.

**Why is the frontier required on every produce?** The producer has no consumption frontier of
its own — it is a transport for the application's causal claim. Passing the clock explicitly on
every send makes the causal dependency visible at the call site and prevents accidental omission.

**Why a single concrete `VectorClock`?** Parsley targets Kafka only. A broker-neutral, self-typed
clock abstraction would buy generality the library does not use, at the cost of indirection
through every type. The clock is one concrete value type that owns its own wire format.

**What about cross-service fence tokens?** Propagating causal state across service boundaries
(e.g. as an HTTP header) is intentionally not a built-in feature — encryption and transport are
deployment concerns. The building blocks are public, though: extract a clock with
`VectorClock.fromRecord(...)` or `consumer.frontier()`, serialise with `toBytes()`/`fromBytes()`,
and gate downstream reads with `satisfiedBy(...)`. See
[Propagating causal context across services](#propagating-causal-context-across-services).
