# Parsley

A Java library for Kafka Streams applications. Documentation is in `docs/`.

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

668 tests, taking roughly four minutes. Integration tests start an embedded KRaft broker in
the same JVM, so nothing external needs to be running.

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
