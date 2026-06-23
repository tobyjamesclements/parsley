# Getting started

## Prerequisites

- Java 25 (`--release 25`)
- Maven 3.9+ (or use the included `./mvnw` wrapper)
- A GitHub account with a [personal access token](https://github.com/settings/tokens) that has the
  `read:packages` scope (required for GitHub Packages, even for public repositories)

## Installation

Parsley is published to [GitHub Packages](https://github.com/tobyjamesclements/parsley/packages).
Add your credentials to `~/.m2/settings.xml`:

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

Then declare the repository and dependency in your `pom.xml`:

```xml
<repositories>
  <repository>
    <id>github-parsley</id>
    <url>https://maven.pkg.github.com/tobyjamesclements/parsley</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

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
