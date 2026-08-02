# Getting started

## Requirements

- Java 21 or later.
- Kafka brokers new enough to serve topic IDs (3.7+).
- `processing.guarantee=exactly_once_v2` — enforced by `Parsley.streams`, which also pins
  the consumer to `read_committed`. Parsley's state is defined against EOS; there is no
  at-least-once mode.

## Adding the dependency

```xml
<repositories>
    <repository>
        <id>central-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases><enabled>false</enabled></releases>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.github.tobyjamesclements</groupId>
        <artifactId>parsley</artifactId>
        <version>0.2.0-SNAPSHOT</version>
    </dependency>

    <!-- Only for a test suite that uses ContractProbes. -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams-test-utils</artifactId>
        <version>4.3.1</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

The same in Gradle:

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://central.sonatype.com/repository/maven-snapshots/") }
}

dependencies {
    implementation("io.github.tobyjamesclements:parsley:0.2.0-SNAPSHOT")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:4.3.1")   // ContractProbes
}
```

`kafka-clients` and `kafka-streams` are inherited and required; the coordinates alone run
everything on this page. An application with no stage, using only `CausalClock` and
`CausalHeaders` ([plain clients](clients.md)), is the one exception and may exclude
`kafka-streams` with `<exclusion>` /
`exclude(group = "org.apache.kafka", module = "kafka-streams")`, which takes 74 MiB off the
classpath — see [excluding kafka-streams](#excluding-kafka-streams) for the rest of what
goes with it.

## What the published POM declares

`ContractProbes` ships in the library jar and compiles against `TopologyTestDriver`, so the
published POM declares `kafka-streams-test-utils` at compile scope, marked optional. Optional
is what holds the boundary: an application declaring only `parsley` does not resolve it, and
declares it at test scope itself to use the probes — the coordinates are under [adding the
dependency](#adding-the-dependency).

The declaration is deliberate, and it is visible. A scanner reading the POM sees a
`*-test-utils` artifact at compile scope on a production library, which is the shape
supply-chain policies are written to flag, so expect to justify or suppress it once. What it
admits is bounded: that artifact's own dependencies are `kafka-streams`, `kafka-clients`, and
`slf4j-api`, every one of them already inherited below, and it carries no test framework, so
it adds one node to your graph, not a subtree. Whole-program tooling is the other tell:
`jdeps`, jlink image builds, and shading with class-retention analysis all report
`ContractProbes` referencing classes that are not on a production classpath. Nothing fails at
runtime; the class is never loaded unless a test suite calls it.

## What you inherit

The two Kafka artifacts bring the rest of the classpath with them. Resolved from the
coordinates above — `kafka-streams-test-utils` is declared optional, so it is not resolved
from them and is not part of this:

| artifact | scope | size | licence | bundled natives |
|---|---|---|---|---|
| `io.github.tobyjamesclements:parsley:0.2.0-SNAPSHOT` | — | 0.1 MiB | MIT | — |
| `org.apache.kafka:kafka-clients:4.3.1` | compile | 9.7 MiB | Apache-2.0 | — |
| `com.github.luben:zstd-jni:1.5.6-10` | runtime | 7.0 MiB | BSD-2-Clause | 18 |
| `at.yawk.lz4:lz4-java:1.10.2` | runtime | 1.0 MiB | Apache-2.0 | 8 |
| `org.xerial.snappy:snappy-java:1.1.10.7` | runtime | 2.2 MiB | Apache-2.0 | 24 |
| `org.apache.kafka:kafka-streams:4.3.1` | compile | 2.2 MiB | Apache-2.0 | — |
| `org.rocksdb:rocksdbjni:10.1.3` | compile | 69.4 MiB | Apache-2.0 **or** GPL-2.0 | 12 |
| `com.fasterxml.jackson.core:jackson-databind:2.21.2` | runtime | 1.6 MiB | Apache-2.0 | — |
| `com.fasterxml.jackson.core:jackson-core:2.21.2` | runtime | 0.6 MiB | Apache-2.0 | — |
| `com.fasterxml.jackson.core:jackson-annotations:2.21` | runtime | 0.1 MiB | Apache-2.0 | — |
| `org.slf4j:slf4j-api:1.7.36` | compile | 40 KiB | MIT | — |

93.8 MiB and 62 native binaries in total; native counts are the `.so`, `.dll`, and `.dylib`
entries in each jar. `slf4j-api` is the version `kafka-clients` itself declares, so Parsley
adds no node to that part of your graph. No logging binding is inherited; choosing one, and
choosing one that binds the 1.7 API rather than 2.x, stays yours.

Three of those rows carry something worth deciding once rather than rediscovering per
project.

### RocksDB is dual-licensed; elect Apache-2.0

`rocksdbjni-10.1.3.pom` declares two licences — Apache-2.0 and "GNU General Public License,
version 2" — and a consumer elects one. Elect Apache-2.0 and there is no copyleft obligation.
It is the only non-permissive string anywhere in the tree, and a dual Apache/GPL entry is
exactly what an automated licence scanner escalates for manual review, so it is worth
recording the election in your own compliance notes rather than reaching it again each time
the scanner runs.

### The natives are inherited, and extracted at runtime

The compression codecs and RocksDB ship a binary per supported platform inside their jars, and
extract the one they need to a directory on disk before loading it:

| library | extracts to | override |
|---|---|---|
| `zstd-jni` | `java.io.tmpdir` | `-DZstdTempFolder=…`, or `-DZstdNativePath=…` to load a pre-installed library instead |
| `snappy-java` | `java.io.tmpdir` | `-Dorg.xerial.snappy.tempdir=…` |
| `lz4-java` | `java.io.tmpdir` | — |
| `rocksdbjni` | `java.io.tmpdir` | `ROCKSDB_SHAREDLIB_DIR` (environment variable) |

Each loads on first use. RocksDB's is not conditional in a Parsley application: every stage
keeps its protocol state in a persistent store, so the store opens and the binary loads as the
application starts. The codecs load when their codec is used, which a consumer does not
choose — reading a partition someone else wrote with zstd loads zstd, whatever this
application produces.

Three consequences. A hardened container that mounts the extraction directory `noexec` fails
the load, so point the overrides above at a path that is both writable and executable. An
audited or FIPS environment needs an owner for every native library on the host, and these
arrive without being named in any build file. And the jars are multi-platform by
construction, so an image built for one architecture still carries every other platform's
binaries — the bulk of the 93.8 MiB, and not removable without repackaging the jars.

### `at.yawk.lz4:lz4-java` is a fork coordinate

Kafka 4.x moved off `org.lz4:lz4-java`, which is dormant, to a fork published from
[github.com/yawkat/lz4-java](https://github.com/yawkat/lz4-java) under a personal-domain
groupId. It is Apache-2.0 and it comes straight from `kafka-clients`, but a site running an
artifact allowlist, a groupId policy, or a provenance check has to admit it, and it arrives
through this library with nothing else explaining where it came from.

### Excluding kafka-streams

The plain-client exclusion above removes `kafka-streams`, and `rocksdbjni` and the Jackson
tree with it: 73.8 MiB, the GPL-2.0 option, and 12 of the 62 natives. What remains is 20.0 MiB
and the 50 codec natives, all of it under permissive licences. The exclusion is only available
to an application with no stage; anything that runs the causal gate needs the state store, and
so needs RocksDB.

## The shape: functional core, imperative edges

Your logic is a pure function. It receives a causally delivered `Message` and returns
`Emission` values; with per-key state, it is a fold that also returns the next state. It
holds no reference to any runtime, imports nothing from Kafka, and is unit-testable with
plain equality. Everything imperative — gating, decoding, state persistence, stamping,
partitioning, the transaction — lives in the stage runtime that hosts it.

## Topics and codecs

Topics are typed values declared once — codecs live with the declaration, and a pipeline hop
is the same `Topic` appearing as one stage's sink and another's source, which makes codec
agreement across the hop hold by construction. A `Codec` is two pure functions between a
value and its bytes; Kafka serdes (Avro, JSON Schema, and friends) bridge in with
`Codec.fromSerde(serde, topicName)`. The codec contract, hand-rolled codecs, and the
schema-registry formats are covered in [codecs and Avro](codecs.md).

```java
Topic<String, Order>   orders      = Topic.of("orders", Codec.utf8(), orderCodec);
Topic<String, Payment> payments    = Topic.of("payments", Codec.utf8(), paymentCodec);
Topic<String, Settled> settlements = Topic.of("settlements", Codec.utf8(), settledCodec);
```

## A stateless stage

A `Handler` maps one delivered message to its emissions. Emissions are created by the sink
topic itself — `topic.send(key, value)` — so the payload type-checks at the construction
site, and an emission to an undeclared sink fails loudly at the stamping site.

```java
Stage settlement = Stage.named("settlement")
        .on(orders,   m -> List.of(settlements.send(m.key(), settle(m.value()))))
        .on(payments, m -> List.of(settlements.send(m.key(), apply(m.value()))))
        .into(settlements)
        .build();

Properties props = new Properties();
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");

try (CausalStreams app = Parsley.named("settlements-app", settlement).streams(props)) {
    app.start();
    // run until shutdown
}
```

The application id goes to `Parsley.named` rather than into the properties, because it names
things: `streams` sets `application.id` from it, and it prefixes every topic and store the
application owns — the same convention Kafka Streams uses, so a cluster's topic listing keeps
an application's topics together. See [expectations](expectations.md#what-parsley-expects-of-you).

Every cause the stage consumes has already been delivered when a handler runs; that is the
guarantee. A message carries its own coordinate — source topic, partition, offset,
timestamp — and for a message released from the hold queue, the coordinate is its own, not
the release trigger's. By default an emission inherits the handled message's timestamp;
`send(key, value, timestamp)` overrides it.

## A stateful stage

Declaring `state` gives the stage per-key state, and sources fold over it: the runtime
resolves the state for the message's key (the declared initial value for an unseen key),
applies the pure `Fold`, persists the returned state, and applies the returned emissions —
all in the same transaction as the delivery. Causal order plus a pure fold makes the state a
deterministic function of the delivered history.

```java
Stage balances = Stage.named("balances")
        .state(balanceCodec, Balance::zero)
        .on(orders,   (bal, m) -> Step.of(bal.minus(m.value().total()),
                                          settlements.send(m.key(), settled(bal, m))))
        .on(payments, (bal, m) -> Step.of(bal.plus(m.value().amount())))
        .into(settlements)
        .build();
```

State is keyed by the message key's encoded bytes — the same agreement co-partitioning
already requires — and scoped to the stage. Returning `Step.of(null, ...)` deletes the key's
state. The stage name keys its stores, so keep it stable across deployments.

## Testing your logic

Handlers and folds are pure functions over values with equality, so the primary tests need
no runtime at all:

```java
assertEquals(List.of(settlements.send("k", expected)),
        handler.handle(Message.of("orders", "k", order)));

assertEquals(Step.of(expectedBalance, settlements.send("k", expected)),
        fold.apply(balance, Message.of("payments", "k", payment)));
```

For the wiring, `Parsley.named(applicationId, stage).testTopology()` returns a fresh
broker-less topology for `TopologyTestDriver`: pipe records and assert on outputs. Stamp
presence is observable via the `CausalHeaders` name constants; gating behaviour itself is
Parsley's contract, verified by Parsley's own suite. The test topology is for the driver
only — under a real cluster it neither captures consumer positions nor resolves real topic
identity, and the adapter fails closed at the first delivered record.

## What the runtime wires for you

`Parsley.streams` installs a client supplier that records consumer positions after each poll
(the liveness signal — see [liveness](../foundations/liveness.md#position-advance-bridging)),
and resolves topic identity and sink end offsets through an admin client built from the same
properties. A declared sink must exist before the application starts: init fails loudly
rather than run with own-output claims silently off. The returned `CausalStreams` is a
curated allowlist over the Kafka Streams runtime — lifecycle, state, listeners, metrics,
lag — with no accessor to the underlying instance. The withheld members are absent for
stated reasons, on the class Javadoc member by member: interactive queries can observe
uncommitted transactional state, pausing an instance freezes the release of held records
fleet-wide, and the rest duplicates what scaling by instances already expresses. `metrics()` includes the stage's own gauges and
counters in the `vc-metrics` group, listed in the
[metrics reference](../reference/metrics.md).

## Where next

- [Topology shapes](topologies.md) — the common wiring shapes (linear, fan-out, fan-in,
  diamond, request and reply, event flows, cycles) and exactly what ordering each one gets.
- [Codecs and Avro](codecs.md) — the codec contract, writing your own, bridging serdes, and
  schema-registry formats.
- [Ticks](ticks.md) — time-driven policy as delivered records: the runtime emits a stamped
  tick per interval, and pure logic folds over it.
- [The contract](expectations.md) — everything Parsley expects of you and everything it
  promises back, including the operational notes.
- [Verifying your application](verifying.md) — the contract's checkable clauses as probes
  in your test suite and deploy pipeline.
- [Diagnosing held records](diagnosing-holds.md) — `explainHolds()`, the hold warnings, and
  what each diagnosis means when records are waiting at the gate.
- [Plain clients](clients.md) — stamping from plain producers, observing from plain
  consumers.
