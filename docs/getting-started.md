# Getting started

## Prerequisites

- Java 25 (`--release 25`)
- Maven 3.9+ (or use the included `./mvnw` wrapper)

## Installation

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).
The current version is a snapshot, so it lives in Central's snapshot repository rather than the main
one — no credentials needed, snapshots are publicly readable. Declare the repository and dependency in
your `pom.xml`:

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

Once a tagged `0.1.0` release ships, this repository block won't be needed — Central's default repo
(already in every Maven setup) covers real releases.

`parsley` pulls in `kafka-streams` and `kafka-clients` transitively.

## Ordering records causally

Parsley delivers records in causal order **inside a Kafka Streams topology**. Wrap your own processor
with `CausalProcessors` and it holds any record whose dependencies have not yet been observed,
releasing it once the frontier catches up. See [Streams integration](streams.md) for the full setup.

## Stamping causal context onto produced records

At the edges of a topology — where plain Kafka producers feed records in — attach the causal
dependencies as a header with `CausalDependencies.stamp`. Resolve topic UUIDs once through a
`CausalTopics` backed by an `Admin` you own (Parsley never closes it):

```java
CausalTopics topics = CausalTopics.of(admin);

// trigger's own dependencies plus its own position
CausalDependencies deps = CausalDependencies.from(topics, trigger);
producer.send(deps.stamp(new ProducerRecord<>("orders", key, value)));
```

`from` carries the dependencies the triggering record arrived with **and** its own position, so a
downstream causal processor waits until it has observed `trigger` before delivering anything stamped
here. Combine several with `merge` for a fan-in:

```java
CausalDependencies deps = CausalDependencies.from(topics, priceUpdate)
        .merge(CausalDependencies.from(topics, inventoryChange));
producer.send(deps.stamp(record));
```

To declare a dependency on a specific upstream position you did not consume, build one explicitly:

```java
CausalDependencies deps = CausalDependencies.builder(topics)
        .require("prices", /* partition */ 0, /* offset */ 42)
        .build();
```

The serialised dependencies header grows with the number of topic-partitions it names — see the
[header size note](configuration.md#header-size) in Configuration.

## Propagating causal context across services

A `CausalDependencies` value is a portable causal token. To gate a read in a downstream service on
what an upstream record depended on, serialise the dependencies and send them over your transport:

```java
// Sender — extract the relevant dependencies and serialise them
CausalDependencies context = CausalDependencies.fromRecord(consumedRecord)
        .orElse(CausalDependencies.empty());
byte[] token = context.toBytes();
// ... send token over HTTP, gRPC, etc. (apply your own encryption/transport)

// Receiver — rebuild the dependencies
CausalDependencies required = CausalDependencies.fromBytes(receivedToken);
```

Parsley ships no encryption or transport layer; securing and routing the token is the
application's responsibility.
