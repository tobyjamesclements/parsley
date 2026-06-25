# Parsley

Causal consistency for Kafka. Producers stamp each record with its causal dependencies. Consumers and
Kafka Streams processors hold back any record whose dependencies have not yet been observed, and
release the record once the frontier catches up.

The guarantee is that if A causally precedes B, every consumer observes A before B.

## Install

Parsley is published to [Maven Central](https://central.sonatype.com/artifact/io.github.tobyjamesclements/parsley).
The current version is a snapshot, so it lives in Central's snapshot repository rather than the main
one. No credentials are needed, because snapshots are publicly readable.

```xml
<repository>
  <id>central-snapshots</id>
  <url>https://central.sonatype.com/repository/maven-snapshots/</url>
  <releases><enabled>false</enabled></releases>
  <snapshots><enabled>true</enabled></snapshots>
</repository>

<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Once a tagged `0.1.0` release ships, this repository block is no longer needed, because Central's
default repository, which is already in every Maven setup, covers real releases.

Requires Java 25. Build from source with `./mvnw install`.

## Docs

**[tobyjamesclements.github.io/parsley](https://tobyjamesclements.github.io/parsley)**

Covers concepts, getting started, Streams integration, configuration, and internals.
