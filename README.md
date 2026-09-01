# Parsley

Causal delivery order for Kafka Streams processors: if message A is a cause of message B,
every process that delivers both delivers A first, across restarts and for the whole lifetime
of a process. Where the guarantee cannot be upheld a process stops rather than weaken it. The
documentation site is at <https://tobyjamesclements.github.io/parsley/>, built from `docs/`
in this repository.

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

The whole suite, roughly five minutes; the surefire summary prints the count. Integration
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

Output lands in `target/reports/apidocs`.

## Documentation site

```
./scripts/docs-build.sh
```

Builds the site to `site/`. The script creates its own `.venv` on first run and installs the
MkDocs toolchain there. Pass `--serve` to serve it locally with live reload, or `--skip-api`
to skip the Javadoc step.
