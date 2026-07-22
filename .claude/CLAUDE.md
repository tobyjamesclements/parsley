# Parsley project instructions

Parsley provides causal delivery order for Kafka Streams processors. Single Maven module,
single package `io.github.tobyjamesclements.parsley`, Java 21. The design documentation in
`docs/internals/` is authoritative for the current architecture; start with
`docs/internals/overview.md`.

## Build and verification

- The gate for every commit: `mvn clean verify` green. That runs the unit suite, the
  Testcontainers broker ITs, PIT mutation testing (71 % threshold, enforced in the pom), and
  NullAway.
- NullAway: main sources are ERROR-clean and must stay so. Tests currently run at WARN
  (cleanup before promoting them to ERROR is open backlog). The package is `@NullMarked`
  (JSpecify); mind the JSpecify placement rules on arrays and record components.
- Broker ITs pin their Kafka image through the `ParsleyBrokerImage` seam; the minimum
  supported broker is 3.7.0 and CI runs both a 3.7.0 and a current-broker leg.
- Update `CHANGELOG.md` `[Unreleased]` in the same commit as the change it records.

## Naming

- `Causal*` = public API, `Parsley*` = package-private internals; seam implementations are
  named for their backing (`KafkaTopics*`, `StoreBacked*`). Codified in `package-info.java`.
- Run every new or renamed identifier through the academic naming test in
  `docs/internals/naming.md`: exact literature match → adopt and cite; near miss → do not
  borrow; Kafka-specific → mark as coinage. Record decisions in that page's register.
- Tests and docs samples name channels/topics `c1..cN`, messages `m1..mN`, and processor
  nodes `p1..pN`. Scenario-named broker ITs are exempt.

## Code style

- Package-private visibility over an "internal" package; no "don't look here" packages.
- Interface-first public API; classic Factory and Decorator patterns.
- A single generic type parameter is named `T`, not a mnemonic letter.
- Use Kafka's public `Header`/`Headers` API, never `*.internals.*` implementations.
- Never log payload bytes. Log metadata plus a pointer to the durable source.

## Testing

- No mock frameworks. Hand-rolled test doubles only; route wide third-party interfaces
  through narrow Parsley seams so the doubles stay small.
- camelCase test method names, Javadoc on every `@Test`, assertion messages, descriptive
  helper methods.

## Design and process rules

- Causal safety is inviolable: never deliver a record before a real cause, no timeout
  guessing; block or fault rather than reorder. The invariant catalogue is
  `docs/internals/invariants.md` (I1–I9, cited throughout Javadoc and tests).
- No silent feature drops: a refactor carries existing capabilities over or explicitly lists
  what it drops and why.
- Pre-1.0: prefer clean breaking changes over deprecation aliases.
- Prefer existing configuration surfaces over new mode-specific semantics.
- Model a feature's data flow end to end before building it; an existing primitive may
  already express it.

## Documentation

- Plain full sentences, technical tone, no selling. No em-dash or semicolon sentence
  fragments. Do not duplicate examples across pages.
- A "docs audit" covers `docs/**/*.md`, `README.md`, and the root `overview.html`, not just
  Javadoc.
- Code samples in `docs/getting-started.md` and `docs/streams.md` are compile-pinned by
  `DocsSamplesTest` via verbatim mirrors; the doc fences carry HTML comments naming their
  mirrors. Keep them in sync when editing either side.
