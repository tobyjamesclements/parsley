# Getting started

## Prerequisites

- Java 25 (`--release 25`)
- Maven 3.9 or later, or the included `./mvnw` wrapper

## Installation

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).
The current version is a snapshot, so it lives in Central's snapshot repository rather than the main
one. No credentials are needed, because snapshots are publicly readable. Declare the repository and
the dependency in your `pom.xml`.

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
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Once a tagged `0.1.0` release ships, this repository block is no longer needed, because Central's
default repository, which is already in every Maven setup, covers real releases.

`parsley` pulls in `kafka-streams` and `kafka-clients` transitively.

## Ordering records causally

Parsley delivers records in causal order inside a Kafka Streams topology. Wrap your own processor
with `CausalProcessors`, and it holds any record whose dependencies have not yet been observed,
releasing the record once the frontier catches up. See [Streams integration](streams.md) for the full
setup.

## Stamping causal context onto produced records

At the edges of a topology, where plain Kafka producers feed records in, a node has no Parsley engine
maintaining a frontier for it, so it maintains one itself. A `CausalDependencies` value is that
frontier: the running set of positions the node has observed. Bind a `CausalTopics` resolver once with
`using`, fold in each record you consume with `observe`, and attach the result to each record you
produce with `stamp`. The `CausalTopics` is backed by an `Admin` you own; Parsley never closes it.

```java
CausalTopics topics = CausalTopics.of(admin);

// the trigger's own dependencies plus its own position
CausalDependencies deps = CausalDependencies.using(topics).observe(trigger);
producer.send(deps.stamp(new ProducerRecord<>("orders", key, value)));
```

`observe` folds in the dependencies the consumed record arrived with, together with the consumed
record's own position. A downstream causal processor therefore waits until it has observed `trigger`
before it delivers anything stamped here. The resolver bound by `using` carries through each
`observe`, so a fan-in — where an output is caused by several inputs — chains an `observe` per input.

```java
CausalDependencies deps = CausalDependencies.using(topics)
        .observe(priceUpdate)
        .observe(inventoryChange);
producer.send(deps.stamp(record));
```

A stateful node whose output reflects everything it has consumed keeps a single instance and
`observe`s into it across records, so each record it produces carries the node's full frontier.

To declare a dependency on a specific upstream position that you did not consume, build one
explicitly.

```java
CausalDependencies deps = CausalDependencies.builder(topics)
        .require("prices", /* partition */ 0, /* offset */ 42)
        .build();
```

The serialised dependencies header grows with the number of topic-partitions it names. See the
[header size note](configuration.md#header-size) in Configuration.

## Propagating causal context across services

A `CausalDependencies` value is a portable causal token. To gate a read in a downstream service on
what an upstream record depended on, serialise the dependencies and send them over your transport.

```java
// Sender. Extract the relevant dependencies and serialise them.
CausalDependencies context = CausalDependencies.fromRecord(consumedRecord)
        .orElse(CausalDependencies.empty());
byte[] token = context.toBytes();
// Send the token over HTTP, gRPC, or another transport, applying your own encryption.

// Receiver. Rebuild the dependencies.
CausalDependencies required = CausalDependencies.fromBytes(receivedToken);
```

Parsley ships no encryption or transport layer. Securing and routing the token is the application's
responsibility.
