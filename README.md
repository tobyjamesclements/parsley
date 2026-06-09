# Parsley

A Kafka Streams library for causal consistency across distributed topologies.

## What is Parsley?

Parsley is a lightweight library that enforces causal message ordering across Kafka Streams topologies. It follows the same adoption model as Kafka transactions — any topology that opts in gets the guarantee, any that doesn't is outside the contract — but unlike transactions, Parsley maintains stateful vector clocks to track causal dependencies across topic-partition frontiers.

---

## The Problem

Consider two topics: `price` and `order`. A consumer reads a price update from `price-0` at offset 27, and on the basis of that observation produces an order to `order`. A downstream consumer of `order` processes that order — but its consumer of `price` is only at offset 24. It is now acting on an order whose causal premise it has not yet observed.

This is a causal violation. It occurs under normal conditions — it is simply consumer lag on one topic racing ahead of a write to another. Kafka's per-partition ordering guarantee does not prevent it.

Parsley detects and prevents this class of violation transparently, without changes to application logic.

---

## Consistency Model

Parsley provides **causal consistency** — a guarantee that if event A causally precedes event B, every consumer observes A before B. This is stronger than eventual consistency but weaker than linearisability, and carries a fixed, predictable latency overhead rather than a load-dependent one.

---

## How It Works

### Vector clocks on message headers

Every message produced via a `CausalProducer` carries a vector clock header derived from a `FenceToken` supplied explicitly by the application. The token encodes the application's causal position at the time of writing — obtained from a `CausalConsumer.fenceToken()` call or propagated from another Parsley-enabled service via an HTTP response header. The producer does not maintain its own consumption frontier; it is a transport for the application's causal claim. Tokens cannot be constructed arbitrarily or adapted from non-Parsley sources.

### Header attachment on produce

The `CausalProducer` attaches the vector clock derived from the application-supplied `FenceToken` as a message header before producing to Kafka. There is no internal relay or staging topic on the producer side — the header is attached directly and the message is produced in a single operation.

### Causal evaluation and buffering

Causal evaluation is performed by `CausalProcessor` — a Kafka Streams `Processor` implementation. As messages arrive from subscribed topics, the processor advances its frontier store, evaluates each message's vector clock against the current frontier, and either forwards the message or buffers it until its dependencies are satisfied. The `CausalConsumer` is a convenience facade that wires up this processor internally — applications building their own topologies can use `CausalProcessorSupplier` directly.

### Causal violations

A genuine violation occurs when a message arrives with a missing or structurally invalid vector clock header — indicating a producer that bypassed the decorator. These are surfaced via a configurable `CausalViolationHandler`.

---

## Kafka Streams Integration

Parsley is a Kafka Streams-native library. The integration point between the two libraries is `CausalProcessorSupplier`, which implements Kafka Streams' `ProcessorSupplier` interface. When passed to `KStream.process()`, Kafka Streams calls `get()` to obtain a `CausalProcessor` instance and `stores()` to register the frontier and buffer state stores automatically. Parsley inherits all of Streams' processor lifecycle guarantees — state store restoration on restart, context injection, punctuator scheduling — without reimplementing any of it.

```
Kafka Streams                      Parsley
──────────────────────────────────────────────────────
KStream.process()          →       CausalProcessorSupplier.get()
StoreBuilder registration  →       CausalProcessorSupplier.stores()
Processor.init()           →       CausalProcessor.init(ProcessorContext)
Processor.process()        →       CausalProcessor.process(Record)
Processor.close()          →       CausalProcessor.close()
```

Parsley is a Streams-native extension, not a parallel framework. Everything Streams provides — rebalance handling, changelog backing, transactional forwarding — applies to Parsley's internal processor without any additional configuration.

---

## Prerequisites

Parsley's `CausalProcessor` only provides a benefit when processing messages from multiple partitions concurrently. This arises in two situations:

- **Multiple topics** — the processor is subscribed to more than one topic, each with its own partition stream
- **Multiple partitions per consumer** — the consumer group has fewer instances than the total number of partitions, so one instance is assigned more than one partition

In either case, Kafka provides no ordering guarantee across those partition streams. Parsley detects and corrects causal violations that arise from their interleaving.

If a processor is assigned exactly one partition from one topic, Kafka already guarantees total order within that partition. Parsley adds overhead for no benefit in that configuration and should not be used.

### Parallelism and co-partitioning

For `CausalProcessor` to evaluate causal dependencies correctly, it must be assigned all partitions in its causally related partition set. A processor that only sees a subset of partitions cannot determine whether a message's dependencies have been satisfied by messages it never received.

The recommended approach is to design for causal order per topic and co-partition topics that have causally related messages — that is, ensure that causally related messages across topics share the same partition key, so they are assigned to the same partition number across all topics. A `CausalProcessor` can then be assigned partition N from each topic and evaluate causal dependencies correctly across all of them, while other instances handle other partition numbers independently.

This preserves horizontal scalability — each instance handles a causally self-contained partition set — while giving Parsley the visibility it needs to enforce ordering within that set. The number of instances can scale with the number of partitions, as long as co-partitioning is maintained.

If topics cannot be co-partitioned, a single instance must be assigned all partitions across all topics, which limits parallelism to a single instance. In that case throughput is bounded by what a single processor can handle, and a plain Kafka consumer should be considered if causal consistency can be relaxed.

**Parsley does not detect or enforce co-partitioning.** Kafka Streams is a mature library that detects co-partitioning violations at topology build time and inserts repartition topics automatically where needed. Parsley provides no equivalent — it is the application operator's responsibility to ensure that causally related topics are co-partitioned correctly. A misconfigured topology will not fail at startup; it will silently evaluate causal dependencies against an incomplete partition set and produce incorrect results. This is a known limitation.

---

## Scope

### The guarantee is per-protocol, not per-topology

Parsley enforces causal consistency across any set of topologies that honour the vector clock protocol. This is the same model as Kafka transactions: participants opt in by using the decorated types; those that don't are outside the contract.

### What Parsley does not do

- Linearisable reads
- Cross-cluster causal consistency without a coordination layer
- Causal consistency for producers that bypass `CausalProducer`
- Conflict resolution for concurrent writes — causal ordering only
- Co-partitioning detection or enforcement

---

## Internal Architecture

The `CausalProducer` is a lightweight decorator — it attaches a vector clock header derived from the application-supplied `FenceToken` and produces directly to the target topic. No staging, no relay.

The `CausalConsumer` is a convenience facade over an internal Kafka Streams topology built using `CausalProcessorSupplier`. Messages flow through `CausalProcessor`, which handles frontier tracking and buffering, before being forwarded to an internal ready topic that `CausalConsumer.poll()` reads from.

```
CausalProducer.send(record, fenceToken)
    → attaches vector clock header derived from fenceToken
    → produces directly to target topic

target topic
    → CausalProcessor (via CausalProcessorSupplier, Kafka Streams)
        → advances frontier store
        → evaluates vector clock against frontier
        → buffers if unsatisfied
        → forwards to internal ready topic when satisfied
    → CausalConsumer.poll()
        → reads from internal ready topic
        → surfaces to application
```

---

## API

### CausalProducer

```java
public interface CausalProducer<K, V> {
    static <K, V> CausalProducer<K, V> create(Map<String, Object> config);
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token);
    Future<RecordMetadata> send(ProducerRecord<K, V> record, FenceToken token, Callback callback);
    void close();
}
```

The `FenceToken` encodes the application's causal position at the time of the write and is attached as a message header for evaluation by downstream `CausalProcessor` instances. The token must be obtained from a Parsley-aware source — the application is responsible for obtaining and passing it explicitly.

The most common source is `CausalConsumer.fenceToken()`, called after polling. For cross-service causal consistency, a token may also be propagated via an HTTP response header from another Parsley-enabled service and presented on the next produce. Tokens cannot be constructed arbitrarily or adapted from non-Parsley sources — the fence token is the protocol boundary.

### CausalConsumer

```java
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

Convenience facade over an internal Streams topology built using `CausalProcessorSupplier`. Applications that need to compose causal processing into their own topology should use `CausalProcessorSupplier` and `CausalStreams` directly.

### CausalProcessorSupplier

```java
public final class CausalProcessorSupplier<K, V> implements ProcessorSupplier<K, V, K, V> {

    public CausalProcessorSupplier(
        BufferingPolicy policy,
        CausalViolationHandler violationHandler,
        VectorClockSerialiser serialiser);

    @Override
    public Processor<K, V, K, V> get();

    @Override
    public Set<StoreBuilder<?>> stores(); // registers frontier and buffer stores automatically
}
```

The integration point between Parsley and Kafka Streams. Pass to `KStream.process()` to insert causal evaluation into any topology. State stores are registered automatically via `stores()` — no manual store registration required.

### CausalStreams

```java
public final class CausalStreams {
    public static <K, V> KStream<K, V> process(
        KStream<K, V> stream,
        CausalProcessorSupplier<K, V> supplier);

    public static <K, V> KStream<K, V> process(
        KStream<K, V> stream,
        CausalProcessorSupplier<K, V> supplier,
        Named named);
}
```

Convenience wrapper that applies a `CausalProcessorSupplier` to a `KStream` and returns the resulting `KStream` for further chaining. Applications building their own topologies use this rather than `CausalConsumer`.

```java
// Advanced usage — full topology control
KStream<String, Order> causal = CausalStreams.process(
    builder.stream(List.of("price", "order")),
    new CausalProcessorSupplier<>(policy, violationHandler, serialiser),
    Named.as("causal-processor"));

causal.filter(...)
      .mapValues(...)
      .to("output-topic");
```

### VectorClock

```java
public interface VectorClock {
    Map<TopicPartition, Long> positions();
    boolean dominates(VectorClock other);
    boolean dominatedBy(VectorClock other);
}
```

### FenceToken

```java
public interface FenceToken {
    String encode();
    static FenceToken decode(String encoded);
}
```

Opaque encrypted token encoding a vector clock position from a Parsley-aware source. Intended for HTTP response headers, allowing downstream services to propagate causal context across service boundaries. Clients cannot inspect or forge the token.

### BufferLimit

Controls when a buffered message is evicted. Used as a parameter to buffering policies.

```java
public interface BufferLimit {
    static BufferLimit ofDuration(Duration duration);
    static BufferLimit ofSize(int messages);
    static BufferLimit ofBytes(long bytes);
    static BufferLimit ofFrontierAdvancement(long offsets);
    static BufferLimit first(BufferLimit... limits); // whichever fires first
}
```

| Limit | Eviction trigger |
|---|---|
| `ofDuration(Duration)` | Wall clock time since the message was buffered |
| `ofSize(int)` | Buffer exceeds a maximum number of messages |
| `ofBytes(long)` | Buffer exceeds a maximum memory footprint |
| `ofFrontierAdvancement(long)` | Frontier has advanced by N offsets since the message was buffered |
| `first(BufferLimit...)` | Whichever limit fires first — useful for combining time and size bounds |

### BufferingPolicy

```java
public interface BufferingPolicy {
    static BufferingPolicy ignore(BufferLimit limit);
    static BufferingPolicy drop(BufferLimit limit);
    static BufferingPolicy deadLetter(BufferLimit limit, String topic);
}
```

| Policy | Behaviour on unsatisfied message |
|---|---|
| `ignore(BufferLimit)` | Hold until causal dependencies satisfied, process anyway when limit reached |
| `drop(BufferLimit)` | Hold until causal dependencies satisfied, discard and notify `CausalViolationHandler` when limit reached |
| `deadLetter(BufferLimit, String)` | Hold until causal dependencies satisfied, forward to dead letter topic when limit reached — recoverable, retains original headers including vector clock |

Every policy makes a genuine attempt at causal consistency before falling back at the limit. The dead letter policy is recommended for production use — timed-out messages are recoverable and retain their vector clock headers for diagnostics.

Applications requiring fully custom behaviour implement the `CausalBuffer` SPI directly.

### Violation handling

```java
public interface CausalViolationHandler {
    void onViolation(ConsumerRecord<?, ?> record, CausalViolationReason reason);
}

public enum CausalViolationReason {
    MISSING_HEADER,    // Producer did not use CausalProducer
    UNRESOLVABLE_CLOCK, // Header present but structurally invalid
    LIMIT_REACHED      // Causal dependencies not satisfied before BufferLimit was reached
}
```

The default handler throws. Applications may substitute a dead letter handler, metric emitter, or logger.

---

## Extension Points

Parsley has few opinions. Most behaviours are abstracted via the Java Service Provider Interface, allowing applications to substitute their own implementations without forking the library.

### FenceTokenEncryption

```java
public interface FenceTokenEncryption {
    String encrypt(byte[] data);
    byte[] decrypt(String encoded);
}
```

Controls how fence tokens are encrypted and encoded. Applications provide their own implementation — JDK crypto, Bouncy Castle, a KMS-backed scheme — via the SPI. A default JDK-only implementation is available in `io.parsley.crypto.jdk`.

### VectorClockSerialiser

```java
public interface VectorClockSerialiser {
    byte[] serialise(VectorClock clock);
    VectorClock deserialise(byte[] bytes);
}
```

Controls the wire format of vector clock headers. A default implementation is provided in `io.parsley.serialisation`. Applications with opinions about wire format or schema evolution can substitute their own.

### CausalBuffer

```java
public interface CausalBuffer<K, V> {
    void add(ConsumerRecord<K, V> record);
    List<ConsumerRecord<K, V>> drain(VectorClock frontier);
    void evict(BufferLimit limit, CausalViolationHandler handler);
}
```

The SPI underlying `BufferingPolicy`. The three default policies — `ignore`, `drop`, `deadLetter` — are implementations of this interface provided in `io.parsley.buffer`. Applications requiring fully custom behaviour, such as a bounded, persistent, or priority-ordered buffer, may implement this interface directly and supply it in place of a `BufferingPolicy`.

### ParsleyMetrics

```java
public interface ParsleyMetrics {
    void onMessageBuffered(TopicPartition partition);
    void onMessageReleased(TopicPartition partition, Duration bufferDuration);
    void onViolation(CausalViolationReason reason);
    void onFrontierAdvanced(VectorClock frontier);
}
```

Parsley emits no metrics or logs directly. Applications bind this interface to Micrometer, OpenTelemetry, or any other observability framework already in use. A no-op default is provided.

---

## Module Structure

```
io.parsley                  — core interfaces, BufferingPolicy, BufferLimit, and SPI definitions
io.parsley.streams          — CausalProcessor, CausalProcessorSupplier, CausalStreams, CausalConsumer, CausalProducer
io.parsley.crypto.jdk       — default FenceToken encryption (JDK javax.crypto)
io.parsley.serialisation    — default VectorClock serialisation
io.parsley.buffer           — default CausalBuffer implementations (ignore, drop, deadLetter)
```

The core module has no Kafka dependency — it contains only interfaces and SPI definitions. The Kafka Streams dependency is scoped to `io.parsley.streams`. All default implementation modules are optional — applications that provide their own SPI implementations need not include them.

---

## Dependencies

`io.parsley` has no external dependencies. `io.parsley.streams` requires `kafka-streams` on the classpath; `kafka-clients` is pulled in transitively. Default implementation modules use only JDK APIs.

---

## Design Decisions

**Why Kafka Streams internally?**
Streams provides changelog-backed state stores, rebalance-safe state restoration, and transactional forwarding as primitives. Using `ProcessorSupplier` as the integration point means Parsley inherits all of these guarantees without reimplementing them. The framework does its job; Parsley fills in the causal logic.

**Why per-protocol rather than per-topology scope?**
Causal consistency is a property of message provenance, not topology membership. A message carries its causal history in its header regardless of which topology produced it. Any processor consuming that message can evaluate the header regardless of which topology it belongs to.

**Why SPI for encryption and serialisation?**
Key management and wire format are application concerns. Parsley has no business making decisions about either. The SPI pattern keeps the core module free of opinions and free of dependencies, while default implementations cover the common case without forcing a choice.

**Why is the FenceToken required on every send?**
The producer has no consumption frontier of its own — it is a transport for the application's causal claim. Requiring the token explicitly on every send makes the causal dependency visible at the call site and prevents accidental omission.