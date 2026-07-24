# Parsley project instructions

Parsley provides causal delivery order for Kafka Streams processors. Single Maven module,
single package `io.github.tobyjamesclements.parsley`, Java 21. The design documentation under
`docs/` is authoritative for the current architecture; start with `docs/foundations/` (the causal
model) and `docs/protocols/` (the three protocols, with the overview at `docs/protocols/index.md`).

## Build and verification

- The gate for every commit: `mvn clean verify` green. That runs the unit suite, the
  Testcontainers broker ITs, PIT mutation testing (71 % threshold, enforced in the pom), and
  NullAway.
- NullAway: main and test sources are both ERROR-clean and must stay so. The generated
  `io.github.tobyjamesclements.parsley.avro` subpackage is excluded (`UnannotatedSubPackages`).
  The package is `@NullMarked` (JSpecify); mind the JSpecify placement rules on arrays and record
  components.
- Broker ITs pin their Kafka image through the `ParsleyBrokerImage` seam; the minimum
  supported broker is 3.7.0 and CI runs both a 3.7.0 and a current-broker leg.
- Update `CHANGELOG.md` `[Unreleased]` in the same commit as the change it records.

## Naming

- `Causal*` = public API, `Parsley*` = package-private internals; seam implementations are
  named for their backing (`KafkaTopics*`, `StoreBacked*`). Codified in `package-info.java`.
- Run every new or renamed identifier through the academic naming test in
  `docs/reference/naming.md`: exact literature match → adopt and cite; near miss → do not
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
  `docs/foundations/invariants.md` (I1–I9, cited throughout Javadoc and tests).
- No silent feature drops: a refactor carries existing capabilities over or explicitly lists
  what it drops and why.
- Pre-1.0: prefer clean breaking changes over deprecation aliases.
- Prefer existing configuration surfaces over new mode-specific semantics.
- Model a feature's data flow end to end before building it; an existing primitive may
  already express it.
- Keep concrete `final` public classes concrete. A sealed-interface + static-factory
  conversion of a single-implementation class was proposed and rejected as ceremony;
  Java's non-public constructors already encapsulate adequately. Propose interface
  indirection only for a demonstrated polymorphism need.
- Through 0.x, public-API ergonomics is an active workstream, not just coverage: exercise
  the surface with realistic usage, surface naming/boilerplate/discoverability friction,
  and propose breaking refinements (batched into one minor). The API locks at 1.0.

## Working process (for agents)

- Present a plan and get approval before writing code on any non-trivial task (more than a
  single-file, single-method change). When planning, stress-test the design against
  restart/crash, multi-topic, serde/schema, and EOS edge cases before presenting; state
  infeasibility plainly with the technical reason rather than papering over a gap.
- For large or multi-session changes, follow the big-task skill (`.claude/skills/big-task`):
  checkpoint the plan to disk before implementing, land one green commit per task, delete
  the working file on completion.
- Carry sustained implementation directly in the main session; reserve subagents for
  research and search fan-out. Continuity comes from on-disk context, not spawned agents.
- Durable context lives in the repo: this file, `.claude/skills/`, `docs/`, Javadoc, and
  code comments. Task state lives in GitHub issues (`gh`). Do not keep a private task
  ledger or convention notes outside the repo.

## Documentation

- Plain full sentences, technical tone, no selling. No em-dash or semicolon sentence
  fragments. Do not duplicate examples across pages.
- A "docs audit" covers `docs/**/*.md`, `README.md`, and the root `overview.html`, not just
  Javadoc.
- Code samples in `docs/guide/getting-started.md` and `docs/guide/streams.md` are compile-pinned by
  `DocsSamplesTest` via verbatim mirrors; the doc fences carry HTML comments naming their
  mirrors. Keep them in sync when editing either side.
