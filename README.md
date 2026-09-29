# Parsley

Causal delivery order for Kafka Streams processors: if message A is a cause of message B,
every process that delivers both delivers A first, across restarts and for the whole lifetime
of a process. Where the guarantee cannot be upheld a process stops rather than weaken it.

## Getting it

```xml
<dependency>
  <groupId>io.github.tobyjamesclements</groupId>
  <artifactId>parsley</artifactId>
  <version>0.3.0</version>
</dependency>
```

Java 21 or newer. The runtime dependencies are `kafka-streams`, `kafka-clients` and
`slf4j-api`. The 0.1.0 release is a different implementation whose API and wire format both
differ from this one.

## Documentation

`SPEC.md` is the authority on correctness. `AGENTS.md` is the guide for anyone, or any
agent, working on the code. The `docs/` directory has the rest:

| Page | Subject |
|---|---|
| [Getting started](docs/getting-started.md) | From an empty project to a running process, and a handler test with no broker |
| [Model](docs/model.md) | How the specification's terms map onto Kafka, and what the metadata expresses |
| [Delivery](docs/delivery.md) | The settled frontier and the delivery decision |
| [State](docs/state.md) | Ordering state, persistence and recovery |
| [Failing closed](docs/failing-closed.md) | What stops a process, and why the blast radius is the process |
| [Runtime](docs/runtime.md) | Wiring into Kafka Streams |
| [Operations](docs/operations.md) | Names, prerequisites, scaling, resets and sizing |
| [Runbooks](docs/runbooks.md) | What an operator does when a process stops or holds, reason by reason |
| [Wire format](docs/wire-format.md) | The frozen on-wire definition of causal metadata |
| [Verification](docs/verification.md) | How the guarantee is tested |

## Requirements

JDK 21 or newer. Maven arrives through the wrapper in this repository, and downloads its
dependencies on the first build. The test suite needs no broker and no container runtime.

## Build

```
./mvnw clean verify
```

Use `mvnw.cmd` on Windows. This compiles, runs the full test suite and packages the jar to
`target/`.

To package without running the tests:

```
./mvnw clean package -DskipTests
```

## Tests

```
./mvnw test
```

The whole suite, roughly eleven minutes; the surefire summary prints the count. Integration
tests start an embedded KRaft broker in the same JVM, so nothing external needs to be
running.

Run one class, or one method:

```
./mvnw test -Dtest=DeliverabilityTest
./mvnw test -Dtest=DeliverabilityTest#noCausesIsDeliverable
```

Simulation runs are seeded and deterministic. A failure names its seed, and re-running that
seed reproduces the run exactly.

## Javadoc

```
./mvnw javadoc:javadoc
```

Output lands in `target/reports/apidocs`. The Javadoc of `main` is published at
https://tobyjamesclements.github.io/parsley/ on every push.

## Releasing

Push a tag `v<version>`. The Release workflow sets the POM version from the tag, builds the
jar with its sources and Javadoc jars, signs them with the project's GPG key, and uploads the
bundle to the Central Portal, where it waits for a manual publish. Locally, the same
artifacts build without signing:

```
./mvnw -Prelease package -DskipTests -Dgpg.skip
```

