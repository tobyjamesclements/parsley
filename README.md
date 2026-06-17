# Parsley

Causal consistency for Kafka. Producers stamp each record with its causal dependencies;
consumers and Kafka Streams processors hold back any record whose dependencies have not yet
been observed, releasing it once the frontier catches up.

The guarantee: if A causally precedes B, every consumer observes A before B.

## Install

Parsley is published to [GitHub Packages](https://github.com/tobyjamesclements/parsley/packages).
GitHub requires a PAT with `read:packages` even for public packages — add it to `~/.m2/settings.xml`
as a server with id `github-parsley`.

```xml
<repository>
  <id>github-parsley</id>
  <url>https://maven.pkg.github.com/tobyjamesclements/parsley</url>
</repository>

<dependency>
  <groupId>io.parsley</groupId>
  <artifactId>parsley</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Requires Java 25. Build from source: `./mvnw install`.

## Docs

**[tobyjamesclements.github.io/parsley](https://tobyjamesclements.github.io/parsley)**

Covers concepts, getting started, Streams integration, configuration, and internals.
