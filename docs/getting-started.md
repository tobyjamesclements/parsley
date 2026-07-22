# Getting started

## Prerequisites

- Java 21 or later (the artifact is compiled with `--release 21`; building Parsley from source
  needs JDK 25)
- Maven 3.9 or later, or the included `./mvnw` wrapper
- Kafka brokers version 3.7.0 or later. 3.7.0 is the minimum supported broker version; the
  integration suite runs against both 3.7.0 and the current stable broker line.

## Installation

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).
The latest tagged release is `0.1.0`, available from Central's default repository with no extra
configuration. This documentation describes the current development version, `0.1.1-SNAPSHOT`,
which lives in Central's snapshot repository. No credentials are needed, because snapshots are
publicly readable. To use the snapshot, declare the repository and the dependency in your
`pom.xml`.

```xml
<repositories>
  <repository>
    <id>central-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots/</url>
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>

<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
```

`parsley` pulls in `kafka-clients` transitively. `kafka-streams` is an optional dependency and does
not arrive transitively: an application using the `CausalStreams` runtime declares `kafka-streams`
itself (a Kafka Streams application already does), while edge stamping with `CausalClock` needs only
`kafka-clients`.

## Ordering records causally

Parsley delivers records in causal order inside a Kafka Streams topology. Declare your topology with
`CausalStreamsBuilder`, binding your own processor to a causal-decorated stage with `.process(...)`,
and it holds any record whose dependencies have not yet been observed, releasing the record once the
frontier catches up. Create every topic the topology touches — sink topics included — before the
first start: a declared sink that does not exist fails startup, because causal stamping depends on
its resolved identity. See [Streams integration](streams.md) for the full setup.

## Stamping causal context onto produced records

At the edges of a topology, where plain Kafka producers feed records in, a node has no Parsley
processor maintaining a frontier for it, so it maintains one itself. A `CausalClock` value is that
frontier: the running set of positions the node has observed. Bind one with `using`, giving it the
Kafka client configuration to resolve topic UUIDs through, fold in each record you consume with
`observe`, and attach the result to each record you produce with `stamp`. Topic UUID resolution is
entirely internal: each distinct topic name is resolved (and cached) the first time it is needed,
through a Kafka admin client Parsley opens and closes on its own — nothing to construct or close
yourself.

<!-- Mirrored verbatim by DocsSamplesTest#relaySample; keep the sample and the test in sync. -->
```java
// m1's own dependencies plus its own position
CausalClock deps = CausalClock.using(props).observe(m1);
producer.send(deps.stamp(new ProducerRecord<>("c3", key, value)));
```

`observe` folds in the dependencies the consumed record arrived with, together with the consumed
record's own position. A downstream causal processor therefore waits until it has observed `m1`
before it delivers anything stamped here. The resolver bound by `using` carries through each
`observe`, so a fan-in — where an output is caused by several inputs — chains an `observe` per input.

<!-- Mirrored verbatim by DocsSamplesTest#fanInSample; keep the sample and the test in sync. -->
```java
CausalClock deps = CausalClock.using(props)
        .observe(m1)
        .observe(m2);
producer.send(deps.stamp(m3));
```

A stateful node whose output reflects everything it has consumed keeps a single instance and
`observe`s into it across records, so each record it produces carries the node's full frontier.

To declare a dependency on a specific upstream position that you did not consume, build one
explicitly.

<!-- Mirrored verbatim by DocsSamplesTest#builderSample; keep the sample and the test in sync. -->
```java
CausalClock deps = CausalClock.builder(props)
        .require("c1", /* partition */ 0, /* offset */ 42)
        .build();
```

Tests without a live broker can bind a resolver over a fixed topic-name-to-UUID map instead, with the
`using(Map<String, Uuid>)` / `builder(Map<String, Uuid>)` overloads.

The serialised dependencies header grows with the number of topic-partitions it names. See the
[header size note](configuration.md#header-size) in Configuration.

## Propagating causal context across services

A `CausalClock` value is a portable causal token. To gate a read in a downstream service on
what an upstream record depended on, serialise the dependencies and send them over your transport.

<!-- Mirrored verbatim by DocsSamplesTest#portableTokenSample; keep the sample and the test in sync. -->
```java
// Sender. Extract the relevant dependencies and serialise them.
CausalClock context = CausalClock.fromRecord(m1)
        .orElse(CausalClock.empty());
byte[] token = context.toBytes();
// Send the token over HTTP, gRPC, or another transport, applying your own encryption.

// Receiver. Rebuild the dependencies.
CausalClock required = CausalClock.fromBytes(receivedToken);
```

Parsley ships no encryption or transport layer. Securing and routing the token is the application's
responsibility.
