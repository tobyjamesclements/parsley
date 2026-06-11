# Parsley

A library for causal consistency in distributed messaging systems, with an implementation for Kafka Streams.

---

## What is Parsley?

Parsley enforces causal message ordering across distributed services. Its core abstractions — vector clocks, fence tokens, and buffering policies — are broker-agnostic. The library ships with a complete implementation on top of Kafka Streams (`parsley-kafka`) and a JDK-only fence token encryption module (`parsley-crypto-jdk-aes`). Both can be replaced with alternative implementations of the same SPIs.

The adoption model mirrors Kafka transactions: any topology that opts in gets the guarantee; any that doesn't is outside the contract.

---

## The Problem

Consider two topics: `price` and `order`. A consumer reads a price update from `price-0` at offset 27, and on the basis of that observation produces an order to `order`. A downstream consumer of `order` processes that order — but its consumer of `price` is only at offset 24. It is now acting on an order whose causal premise it has not yet observed.

This is a causal violation. It occurs under normal operating conditions — it is simply consumer lag on one topic racing ahead of a write to another. Kafka's per-partition ordering guarantee does not prevent it.

Parsley detects and prevents this class of violation transparently, without changes to application logic.

---

## Consistency Model

Parsley provides **causal consistency** — if event A causally precedes event B, every consumer observes A before B. This is stronger than eventual consistency but weaker than linearisability, and carries a predictable latency overhead rather than a load-dependent one.

---

## How It Works

### Core protocol

Every message carries a **vector clock** header — a compact snapshot of the producer's causal state at the time of writing. The vector clock is derived from a **fence token** supplied explicitly by the application.

A consumer maintains a **frontier**: its own current vector clock reflecting everything it has processed so far. When a message arrives, the consumer checks whether its frontier satisfies the message's clock — i.e. whether the consumer has already processed everything the message causally depends on. If yes, the message is forwarded immediately. If not, it is held in a **causal buffer** until the frontier catches up.

If the frontier never advances far enough, a configurable **buffering policy** decides what to do: forward out-of-order, drop, or route to a dead-letter destination.

### Kafka Streams implementation

In `parsley-kafka`, the vector clock is a `KafkaVectorClock` — a map from `TopicPartition` to the highest observed offset on that partition. The frontier and buffer are maintained by `CausalProcessor`, a Kafka Streams `Processor` that runs inside the normal Streams lifecycle.

`CausalConsumer` is a high-level facade that wires this processor up internally and exposes a `poll()`-based API. `CausalProcessorSupplier` exposes the same processor for applications building their own topologies. `CausalProducer` attaches the clock as a Kafka message header before producing.

---

## Getting Started

### Installation

Parsley is not yet published to Maven Central. Clone the repository and install to your local Maven cache:

```bash
git clone https://github.com/tobyjamesclements/parsley.git
cd parsley
mvn install -DskipTests
```

### Dependencies

`parsley-kafka` is the only required module. It bundles `KafkaVectorClockSerialiser` (the `VectorClockSerialiser` SPI implementation, auto-registered via `ServiceLoader`) alongside all Kafka Streams integration.

`parsley-crypto-jdk-aes` is an optional module providing the default `FenceTokenEncryption` SPI implementation (AES-256-GCM using only JDK APIs, auto-registered via `ServiceLoader`). Omit it and supply your own `FenceTokenEncryption` SPI if you need persistent, cross-process, or KMS-backed encryption (see [JDK Encryption Implementation](#jdk-encryption-implementation)).

**Maven:**
```xml
<!-- Required: Kafka Streams integration + bundled VectorClockSerialiser -->
<dependency>
    <groupId>io.parsley</groupId>
    <artifactId>parsley-kafka</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>

<!-- Optional default: AES-256-GCM FenceToken encryption (ephemeral per-JVM key) -->
<dependency>
    <groupId>io.parsley</groupId>
    <artifactId>parsley-crypto-jdk-aes</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

**Gradle:**
```kotlin
implementation("io.parsley:parsley-kafka:0.1.0-SNAPSHOT")
implementation("io.parsley:parsley-crypto-jdk-aes:0.1.0-SNAPSHOT")  // optional
```

`parsley-kafka` brings in `kafka-streams` and `kafka-clients` transitively. Java 25 is required (the project compiles with `--release 25`).

### Usage

#### Consuming with causal ordering

`CausalConsumer` is a drop-in replacement for a plain Kafka consumer that enforces causal consistency automatically:

```java
CausalConsumer<String, String> consumer = CausalConsumer.create(
    List.of("prices", "orders"),
    BufferingPolicy.deadLetter(
        BufferLimit.ofDuration(Duration.ofSeconds(30)), "parsley-dead-letter"),
    Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG,          "my-consumer-group"),
    Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "my-causal-app"));

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        process(record);
    }
}
consumer.close();
```

#### Producing with causal context

Obtain a `FenceToken` from the consumer after polling and pass it to every produce call. The token encodes the consumer's causal position and is attached as a message header for downstream processors to evaluate:

```java
CausalProducer<String, String> producer = CausalProducer.create(
    Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

FenceToken token = consumer.fenceToken();
producer.send(new ProducerRecord<>("output-topic", key, value), token);
```

#### Integrating into an existing Kafka Streams topology

Use `CausalProcessorSupplier` directly when building a custom topology with `StreamsBuilder`:

```java
CausalProcessorSupplier<String, String> supplier = new CausalProcessorSupplier<>(
    BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
    (record, reason) -> log.warn("Causal violation on {}: {}", record.topic(), reason),
    new KafkaVectorClockSerialiser());

StreamsBuilder builder = new StreamsBuilder();
builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), Serdes.String()))
       .process(supplier)
       .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
streams.start();
```

State store registration, frontier persistence, and buffer lifecycle are all handled by `CausalProcessorSupplier` — no additional configuration is required.

---

## Core API

These types live in `parsley-core` (`io.parsley`). They have no Kafka or external dependency — they are the broker-agnostic protocol definitions.

### VectorClock

```java
public interface VectorClock {
    boolean satisfiedBy(VectorClock frontier);
    VectorClock merge(VectorClock other);
}
```

An opaque snapshot of causal progress. `satisfiedBy(frontier)` returns `true` when `frontier` has observed everything this clock depends on — i.e. the receiver is ready to process a message carrying this clock. `merge(other)` returns the causal union: the earliest clock that dominates both this and `other`.

The interface intentionally exposes no partition-level detail. Concrete implementations (`KafkaVectorClock`, or a future Kinesis/RabbitMQ equivalent) carry broker-specific state without coupling the core module to any broker.

### FenceToken

```java
public interface FenceToken {
    String encode();
    VectorClock vectorClock();
    static FenceToken of(VectorClock clock) { ... }
    static FenceToken decode(String encoded) { ... }
}
```

An opaque, encrypted, URL-safe token encoding a `VectorClock`. Intended for cross-service causal propagation — e.g. embedding in HTTP response headers so a downstream service can assert "don't act on this until you have caught up to my causal state."

`FenceToken.of(clock)` wraps a clock in a new token. `encode()` produces a URL-safe Base64 string (encrypted via the `FenceTokenEncryption` SPI). `decode(encoded)` reconstructs the token and recovers the clock via `vectorClock()`.

### BufferLimit

Controls when a buffered record is evicted.

```java
public sealed interface BufferLimit {
    static BufferLimit ofDuration(Duration duration);
    static BufferLimit ofSize(int messages);
    static BufferLimit ofBytes(long bytes);
    static BufferLimit ofFrontierAdvancement(long offsets);
    static BufferLimit first(BufferLimit... limits);
}
```

| Limit | Eviction trigger |
|---|---|
| `ofDuration(Duration)` | Wall clock time since the record was buffered |
| `ofSize(int)` | Buffer holds more than this many records |
| `ofBytes(long)` | Buffer exceeds this many bytes |
| `ofFrontierAdvancement(long)` | Frontier has advanced by this many offsets |
| `first(BufferLimit...)` | Whichever constituent limit fires first |

### BufferingPolicy

Defines what happens to a record when its `BufferLimit` fires.

```java
public sealed interface BufferingPolicy {
    static BufferingPolicy ignore(BufferLimit limit);
    static BufferingPolicy drop(BufferLimit limit);
    static BufferingPolicy deadLetter(BufferLimit limit, String topic);
}
```

| Policy | Behaviour on eviction |
|---|---|
| `ignore(limit)` | Forward out-of-causal-order, report `LIMIT_REACHED` violation |
| `drop(limit)` | Discard silently, report `LIMIT_REACHED` violation |
| `deadLetter(limit, topic)` | Route to named topic, report `LIMIT_REACHED` violation |

Every policy makes a genuine attempt at causal consistency before falling back. The dead-letter policy is recommended for production — timed-out records are recoverable and retain their vector clock headers for diagnostics.

### VectorClockSerialiser SPI

```java
public interface VectorClockSerialiser {
    byte[] serialise(VectorClock clock);
    VectorClock deserialise(byte[] bytes);
}
```

Controls the binary wire format of vector clock headers. Discovered via `ServiceLoader`. The default implementation is `KafkaVectorClockSerialiser`, bundled in `parsley-kafka`. Replace it by registering an alternative in `META-INF/services/io.parsley.VectorClockSerialiser`.

### FenceTokenEncryption SPI

```java
public interface FenceTokenEncryption {
    String encrypt(byte[] data);
    byte[] decrypt(String encoded);
}
```

Controls how fence tokens are encrypted and encoded. Discovered via `ServiceLoader`. The default implementation is `JdkFenceTokenEncryption`, in `parsley-crypto-jdk-aes`. See [JDK Encryption Implementation](#jdk-encryption-implementation) for key lifecycle details.

### CausalBuffer SPI

```java
// io.parsley.kafka.buffer
public interface CausalBuffer<K, V> {
    void add(ConsumerRecord<K, V> record);
    List<ConsumerRecord<K, V>> drain(VectorClock frontier);
    List<ConsumerRecord<K, V>> evict(BufferLimit limit, CausalViolationHandler handler);
}
```

The buffer abstraction underlying `BufferingPolicy`. `drain(frontier)` returns and removes all records whose clock is satisfied by `frontier`. `evict(limit, handler)` forcibly removes records when a limit fires, invoking `handler` for each, and returns records to forward downstream (non-empty only for the `Ignore` policy — `Drop` and `DeadLetter` handle forwarding themselves).

The three default implementations (returned by `CausalBuffers.create()`) cover all three `BufferingPolicy` variants. Implement this interface directly for custom buffering — bounded queues, persistent stores, priority ordering.

### ParsleyMetrics

```java
public interface ParsleyMetrics {
    void onMessageBuffered();
    void onMessageReleased(Duration bufferDuration);
    void onViolation(CausalViolationReason reason);
    void onFrontierAdvanced(VectorClock frontier);
    static ParsleyMetrics noop();
}
```

Parsley emits no metrics or logs directly. Bind this interface to Micrometer, OpenTelemetry, or any observability framework already in use. `ParsleyMetrics.noop()` is a do-nothing default.

---

## Kafka Streams Implementation

These types live in `parsley-kafka`. They are the Kafka-specific implementation of the core abstractions — not the abstractions themselves.

### KafkaVectorClock

```java
// io.parsley.kafka
public final class KafkaVectorClock implements VectorClock {
    public static KafkaVectorClock empty();
    public KafkaVectorClock(Map<TopicPartition, Long> positions);
    public Map<TopicPartition, Long> positions();
    public KafkaVectorClock advance(TopicPartition tp, long offset);

    @Override public boolean satisfiedBy(VectorClock frontier);
    @Override public VectorClock merge(VectorClock other);
    @Override public boolean equals(Object obj);
    @Override public int hashCode();
}
```

The Kafka-specific `VectorClock` — a map from `TopicPartition` to highest observed offset. `positions()` is a concrete method not present on the `VectorClock` interface; cast to `KafkaVectorClock` when partition-level inspection is needed. `advance(tp, offset)` returns a new clock with that partition advanced to `max(current, offset)`.

### KafkaVectorClockSerialiser

```java
// io.parsley.kafka.internal  (implements VectorClockSerialiser, auto-registered via ServiceLoader)
public final class KafkaVectorClockSerialiser implements VectorClockSerialiser { ... }
```

Compact binary serialiser for `KafkaVectorClock`. Wire format: `[int count]` followed by `[short topicLen][byte[] topic UTF-8][int partition][long offset]` per entry. Registered in `META-INF/services/io.parsley.VectorClockSerialiser` — available automatically when `parsley-kafka` is on the classpath.

Pass an instance explicitly when constructing `CausalProcessorSupplier`. Use the same instance on both producer and consumer sides to ensure wire-format compatibility.

### CausalConsumer

```java
// io.parsley.kafka
public interface CausalConsumer<K, V> extends Closeable {
    static <K, V> CausalConsumer<K, V> create(
        Collection<String> topics,
        BufferingPolicy policy,
        Map<String, Object> consumerConfig,
        Map<String, Object> streamsConfig);

    ConsumerRecords<K, V> poll(Duration timeout);
    VectorClock frontier();
    FenceToken fenceToken();
    void close();
}
```

High-level Kafka consumer that delivers records in causal order. Backed internally by a Kafka Streams topology built around `CausalProcessorSupplier`. `frontier()` returns the current `KafkaVectorClock` (cast if partition-level access is needed). `fenceToken()` wraps the frontier in an encrypted token for propagation.

### CausalProducer

```java
// io.parsley.kafka
public interface CausalProducer<K, V> {
    static <K, V> CausalProducer<K, V> create(Map<String, Object> config);
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token);
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token, Callback callback);
    void close();
}
```

Kafka producer that attaches the vector clock from `token` as a `parsley-vector-clock` message header before producing. The producer has no internal frontier — it is a transport for the application's causal claim. The token must come from a Parsley-aware source (typically `CausalConsumer.fenceToken()`).

### CausalProcessorSupplier

```java
// io.parsley.kafka
public final class CausalProcessorSupplier<K, V> implements ProcessorSupplier<K, V, K, V> {

    public CausalProcessorSupplier(
        BufferingPolicy policy,
        CausalViolationHandler violationHandler,
        VectorClockSerialiser serialiser);

    // For DeadLetter policy — routes evicted records to the dead-letter sink
    public CausalProcessorSupplier(
        BufferingPolicy.DeadLetter policy,
        CausalViolationHandler violationHandler,
        VectorClockSerialiser serialiser,
        Consumer<ConsumerRecord<K, V>> deadLetterSink);

    @Override public Processor<K, V, K, V> get();
    @Override public Set<StoreBuilder<?>> stores(); // frontier + buffer stores, registered automatically
}
```

The integration point between Parsley and Kafka Streams. Pass to `KStream.process()` to insert causal evaluation into any topology. State stores (frontier persistence, buffer) are registered automatically via `stores()`.

### CausalStreams

```java
// io.parsley.kafka
public final class CausalStreams {
    public static <K, V> KStream<K, V> process(KStream<K, V> stream, CausalProcessorSupplier<K, V> supplier);
    public static <K, V> KStream<K, V> process(KStream<K, V> stream, CausalProcessorSupplier<K, V> supplier, Named named);
}
```

Convenience wrapper applying a `CausalProcessorSupplier` to a `KStream` and returning the result for chaining:

```java
KStream<String, Order> causal = CausalStreams.process(
    builder.stream(List.of("price", "order")),
    new CausalProcessorSupplier<>(policy, violationHandler, serialiser),
    Named.as("causal-processor"));

causal.filter(...).mapValues(...).to("output-topic");
```

### CausalViolationHandler

```java
// io.parsley.kafka.buffer
@FunctionalInterface
public interface CausalViolationHandler {
    void onViolation(ConsumerRecord<?, ?> record, CausalViolationReason reason);
    static CausalViolationHandler throwing();
    static CausalViolationHandler noop();
}
```

Callback invoked when a causal violation is detected. `CausalViolationReason` has three values:

| Reason | Cause |
|---|---|
| `MISSING_HEADER` | Record carried no `parsley-vector-clock` header (non-Parsley producer) |
| `UNRESOLVABLE_CLOCK` | Header present but could not be deserialised |
| `LIMIT_REACHED` | Record evicted because a `BufferLimit` fired |

`throwing()` throws `CausalViolationException` on every violation. `noop()` silently ignores them. Lambda syntax is idiomatic for custom handlers.

---

## JDK Encryption Implementation

`parsley-crypto-jdk-aes` provides `JdkFenceTokenEncryption` — an AES-256-GCM implementation of the `FenceTokenEncryption` SPI using only `javax.crypto`. It is auto-registered via `ServiceLoader` and requires no configuration.

### Key lifecycle

The no-arg constructor (used by `ServiceLoader`) generates a **fresh, random 256-bit AES key per JVM process**. Tokens produced by `FenceToken.of()` are therefore only decodable within the same JVM. This is appropriate for single-process causal propagation (e.g. one service instance decorating outgoing Kafka records with its frontier).

**Cross-process and cross-service use** — if fence tokens need to survive process restarts or cross service boundaries (e.g. embedded in HTTP headers between two separate deployments), you have two options:

1. Inject a persistent key: `new JdkFenceTokenEncryption(yourSecretKey)` and register it manually instead of relying on `ServiceLoader`.
2. Provide a custom `FenceTokenEncryption` SPI implementation (e.g. backed by AWS KMS, Vault, or a shared keystore) and register it in `META-INF/services/io.parsley.FenceTokenEncryption`.

---

## Kafka Streams Integration

Parsley is a Kafka Streams-native library. The integration point is `CausalProcessorSupplier`, which implements `ProcessorSupplier`. When passed to `KStream.process()`, Kafka Streams manages the full processor lifecycle:

```
Kafka Streams                        Parsley
──────────────────────────────────────────────────────
KStream.process()            →       CausalProcessorSupplier.get()
StoreBuilder registration    →       CausalProcessorSupplier.stores()
Processor.init()             →       CausalProcessor.init(ProcessorContext)
Processor.process()          →       CausalProcessor.process(Record)
Processor.close()            →       CausalProcessor.close()
```

Parsley inherits all Streams guarantees — changelog-backed state store restoration on restart, rebalance handling, transactional forwarding — without reimplementing any of it.

---

## Prerequisites

`CausalProcessor` only provides a benefit when records arrive from multiple partitions concurrently. This arises when:

- The processor subscribes to **multiple topics** (each with its own partition stream)
- One consumer instance is assigned **multiple partitions** (fewer instances than total partitions)

If a processor is assigned exactly one partition from one topic, Kafka already guarantees total order within that partition. Parsley adds overhead for no benefit in that configuration.

### Co-partitioning

For `CausalProcessor` to evaluate causal dependencies correctly, it must be assigned all partitions in its causally related partition set. A processor that sees only a subset cannot determine whether a message's dependencies have been satisfied by messages it never received.

The recommended approach: co-partition causally related topics so that causally related messages share the same partition key and are assigned to the same partition number across all topics. Each `CausalProcessor` instance handles partition N across all topics independently, preserving horizontal scalability.

If co-partitioning is impossible, a single instance must be assigned all partitions, limiting throughput to what one processor can handle.

**Parsley does not detect or enforce co-partitioning.** A misconfigured topology will not fail at startup — it will silently evaluate causal dependencies against an incomplete partition set and produce incorrect results.

---

## Scope

### The guarantee is per-protocol, not per-topology

Parsley enforces causal consistency across any set of topologies that honour the vector clock protocol. Participants opt in by using `CausalProducer` and `CausalConsumer`; those that don't are outside the contract.

### What Parsley does not do

- Linearisable reads
- Cross-cluster causal consistency without a coordination layer
- Causal consistency for producers that bypass `CausalProducer`
- Conflict resolution for concurrent writes — causal ordering only
- Co-partitioning detection or enforcement

---

## Internal Architecture

```
CausalProducer.send(record, fenceToken)
    → extracts VectorClock from fenceToken
    → serialises clock to bytes (KafkaVectorClockSerialiser)
    → attaches as parsley-vector-clock header
    → produces directly to target Kafka topic

target topic
    → CausalProcessor (Kafka Streams, via CausalProcessorSupplier)
        → advances KafkaVectorClock frontier (persisted in state store)
        → deserialises parsley-vector-clock header
        → if recordClock.satisfiedBy(frontier): forward immediately
        → else: add to CausalBuffer
        → on punctuator: drain buffer, evict expired records per BufferingPolicy
        → forwards ready records downstream

CausalConsumer.poll()
    → reads from internal ready topic (output of CausalProcessor)
    → surfaces to application as ConsumerRecords
```

---

## Module Structure

| Java module | Maven artifact | Contents |
|---|---|---|
| `io.parsley` | `parsley-core` | `VectorClock`, `FenceToken`, `BufferingPolicy`, `BufferLimit`, `CausalViolationReason`, `ParsleyMetrics`, `VectorClockSerialiser` SPI, `FenceTokenEncryption` SPI |
| `io.parsley.kafka` | `parsley-kafka` | `KafkaVectorClock`, `CausalConsumer`, `CausalProducer`, `CausalProcessorSupplier`, `CausalStreams` |
| `io.parsley.kafka.buffer` | `parsley-kafka` | `CausalBuffer`, `CausalViolationHandler`, `CausalViolationException`, default buffer implementations |
| `io.parsley.kafka.internal` | `parsley-kafka` | `KafkaVectorClockSerialiser` (ServiceLoader-registered), `CausalProcessor` (internal) |
| `io.parsley.crypto.jdk.aes` | `parsley-crypto-jdk-aes` | `JdkFenceTokenEncryption` (ServiceLoader-registered, AES-256-GCM) |

`parsley-core` has no external dependencies. `parsley-kafka` requires `kafka-streams` (pulls in `kafka-clients` transitively). `parsley-crypto-jdk-aes` requires only the JDK.

---

## Design Decisions

**Why Kafka Streams internally?**
Streams provides changelog-backed state stores, rebalance-safe state restoration, and transactional forwarding as primitives. Using `ProcessorSupplier` as the integration point means Parsley inherits all of these guarantees without reimplementing them.

**Why is `VectorClock` opaque?**
The interface exposes only `satisfiedBy()` and `merge()` — the two operations the buffer needs. Exposing `Map<TopicPartition, Long> positions()` on the interface would couple `parsley-core` to Kafka. `KafkaVectorClock` exposes `positions()` as a concrete method; a future Kinesis or RabbitMQ implementation can define its own equivalent without the interface dictating the type.

**Why per-protocol rather than per-topology scope?**
Causal consistency is a property of message provenance, not topology membership. A message carries its causal history in its header regardless of which topology produced it. Any processor consuming that message can evaluate the header regardless of which topology it belongs to.

**Why SPI for encryption and serialisation?**
Key management and wire format are application concerns. The SPI pattern keeps `parsley-core` free of opinions and free of dependencies, while default implementations cover the common case without forcing a choice.

**Why is the FenceToken required on every send?**
The producer has no consumption frontier of its own — it is a transport for the application's causal claim. Requiring the token explicitly on every send makes the causal dependency visible at the call site and prevents accidental omission.

**Why does `JdkFenceTokenEncryption` use an ephemeral key by default?**
The `ServiceLoader`-managed singleton generates a new AES-256 key on first load. This is the right default for the common case — a single service instance tagging its own outgoing records with its own frontier, then decoding tokens it produced itself. It also avoids key distribution complexity. Cross-service or cross-process use requires a persistent key; that is an application-level decision and is intentionally not baked into the default.
