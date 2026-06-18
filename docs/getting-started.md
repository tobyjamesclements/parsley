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
  <groupId>io.parsley</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`parsley` pulls in `kafka-streams` and `kafka-clients` transitively.

## Consuming in causal order

`CausalConsumer` is a drop-in replacement for a plain Kafka consumer. It delivers records from all
subscribed topics in causal order, holding any record whose dependencies have not yet been observed:

```java
try (CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
        List.of("prices", "orders"),
        CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "my-group"),
        Map.of(StreamsConfig.APPLICATION_ID_CONFIG,    "my-app",
               StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")).build()) {

    while (running) {
        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(this::process);
    }
}
```

## Producing with causal context

`CausalProducer` wraps a standard Kafka producer and attaches the current causal dependencies as a
header on every record it sends:

```java
CausalProducer<String, Event> producer = CausalProducers.<String, Event>builder(producerConfig).build();
```

When sending, pass the dependencies that represent the causal premise of the record. The right
choice for most cases is the dependencies carried by the message that triggered this send — bounded
by that hop's fan-in and transitively carries its own dependencies:

```java
CausalDependencies context = CausalDependencies.fromRecord(trigger)
        .orElseGet(consumer::frontier);
producer.send(new ProducerRecord<>("orders", key, value), context);
```

Use `consumer.frontier()` only when the record genuinely depends on *everything* the consumer has
processed (for example, an aggregator whose output is affected by every record it has ever consumed).
`frontier()` carries every partition ever seen, which can make the dependencies header large — see the
[header size note](configuration.md#header-size) in Configuration.

## Propagating causal context across services

A `CausalDependencies` value is a portable causal token. To gate a read in a downstream service on
what the current service has observed, serialise the dependencies and send them over your transport:

```java
// Sender — extract the relevant dependencies and serialise them
CausalDependencies context = CausalDependencies.fromRecord(consumedRecord)
        .orElseGet(consumer::frontier);
byte[] token = context.toBytes();
// ... send token over HTTP, gRPC, etc. (apply your own encryption/transport)

// Receiver — rebuild and check against local frontier
CausalDependencies required = CausalDependencies.fromBytes(receivedToken);
boolean ready = required.isSatisfiedBy(localFrontier);
```

Parsley ships no encryption or transport layer; securing and routing the token is the
application's responsibility.
