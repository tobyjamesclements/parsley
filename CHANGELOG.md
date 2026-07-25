# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **A "Why the stamp is four clocks and not one" section in the channels module page.** The page
  described what each persisted clock holds without stating why each has to exist, leaving the
  outbound stamp's shape to be inferred from the field notes. The new section gives the rationale
  layer: the stamp must dominate everything that happened-before the record being stamped, the
  contiguous frontier covers most of that past, and the channel clocks, carried ancestry, own
  outputs, and highest-delivered clocks each close one disjoint route by which a real cause escapes
  it. Each subsection states the failure that removing that clock would produce, including the
  dependency of the delivery gate's ignore branch on the channel fold (I2 and I9), the stamp
  regression a scope change would cause without carried ancestry (I3), the cross-partition and
  cross-sink-topic gap own outputs closes, and the unclaimed immediate cause an above-gap delivery
  would leave without the highest-delivered projection.
- **A `diagram-walkthrough` skill that reads the sequence diagram against the source.** The diagram
  numbers every arrow, but turning a step number into the method it stands for was manual. The skill
  asks where the reader wants to start, then walks the calls one at a time, giving the declaration's
  `file:line`, its real signature, where each argument came from earlier in the diagram, the step
  number that carries the return, and the rationale the Javadoc already records. Its `steps.py`
  reproduces mermaid's `autonumber` by counting message lines in the `.mmd`, so step N in the script
  is step N in the render, and it pairs each call with its matching return rather than treating the
  two as unrelated arrows. The skill treats the code as authoritative and instructs that a diagram
  label contradicting the source be reported as a diagram defect.
- **A sequence diagram of the three-layer call path, in `mermaid/two-channel-topology/`.** The
  diagram source is a standalone `two-channel-topology.mmd` so a renderer can watch it directly,
  with the prose alongside it in `README.md`. The
  protocol pages describe each layer's requests and indications in prose, but nothing traced one
  task's actual calls end to end. The diagram walks a two-source, one-processor, one-sink topology
  through init, a held record, the release cascade behind its cause, and an inbound null message,
  naming the real methods on `ParsleyChannels`, `ParsleyCausalBroadcast`, and `ParsleyGossip` at
  each step. Every call carries an activation bar and an explicit return, the calls into Kafka
  included, so the synchronous call stack on the single task thread is visible and a void method's
  return point is not left implicit. The Kafka Streams task thread is itself a participant, since it
  is the caller behind `init()` and every `process()`, without which the processor would run the
  init sequence with no activation bar of its own. The asynchrony in the design is end to end rather than per
  call, so the deferred producer acknowledgement is drawn as its own arrow into
  `ParsleyOwnOutputRegistry` between two phases, which is both what `foldAcknowledgedOutputs()`
  later drains and the reason an outbound stamp cannot carry its own coordinate. Documentation
  only, with no protocol or production-code change.

### Changed
- **The frontier store value is now a named `ParsleyFrontierState` carrying a wire-version byte,
  and its key was renamed `"f"` to `"frontier"` (breaking; state reset required on upgrade).** The
  monolithic hand-serialised `"f"` blob in `ParsleyChannels` is extracted into a
  `ParsleyFrontierState` record that owns the byte layout. A single leading version byte that
  hard-fails on a mismatch replaces the former trailing-optional section scheme, matching
  `ParsleyVectorClock` and `ParsleySerializer`. This drops the implicit cross-layout truncation
  tolerance (relied on only by test helpers) in favour of an explicit fault at init, consistent
  with the pre-1.0 no-upgrade-path stance (O6): a value written by an older layout is rejected
  rather than reinterpreted. Because both the key and the value bytes change, an existing
  `{ns}-frontier` store and its changelog must be reset on upgrade. No protocol or causal-semantics
  change: the seven persisted sections and their contents are unchanged. `docs/reference/wire-format.md`,
  the naming register, and the channels module page are updated (the last carried three stale `"f"`
  references, two of them a link to the renamed wire-format heading).

### Fixed
- **The topology simulator's two liveness guards no longer disagree on what a storm is (#32,
  test-side).** The deep random-topology sweep surfaced a shared-sink cycle whose settle `drain()`
  never terminates, reported as a non-quiescing I6 relay. It is not one: the null-message relay
  quiesces on that shape (a `p = 0.0` pin now proves it), and the storm is business feedback
  amplification with a loop gain above one, a configuration pathology the explorer is meant to skip
  as supercritical. The `drain()` wall-clock liveness guard threw a generic "non-quiescing relay"
  error that a tight run timeout let pre-empt the record-count guard, so a supercritical topology
  was escalated to a hard relay-bug failure instead of being skipped. Both guards now defer to one
  classifier keyed on the storm's log composition (null-heavy is a relay loop and fails, business-
  heavy is supercritical and is skipped), so the verdict no longer depends on which guard trips
  first or on the timeout. The mis-premised `@Disabled` repro is replaced by two pins: a
  `p = 0.0` shared-sink-cycle relay-quiescence pin and a minimal supercritical-classification pin.
  The gossip module docs gain a note that relay quiescence is distinct from business feedback
  amplification. No protocol or production-code change.

## [0.2.0] - 2026-07-24

### Added
- **Two new internals pages: the named-invariant catalogue and the naming register.**
  `docs/internals/invariants.md` states I1–I9 — the invariants Javadoc and tests cite by
  number — which previously had no definition in the repository; `docs/internals/naming.md`
  records the visibility convention, the academic naming test, and the register of naming
  decisions with their literature citations. The causal consistency model page gains a
  "Rejected designs" section recording why epoch consistent cuts and hold-until-admitted joins
  were removed, so they are not re-derived. Project instructions for coding agents landed as
  `.claude/CLAUDE.md` with a big-task workflow skill at `.claude/skills/big-task` (both now
  tracked; the rest of `.claude/` stays ignored).
- **A randomized protocol explorer over the topology simulator** (test-side). The T2.4 property
  harness (`ParsleyTopologySim`) now records every scheduler step as a replayable trace
  (`ParsleySimTrace`, with a text format for copy-pasteable repros) and can re-execute a recorded
  or delta-debugged schedule PRNG-free; failing schedules are minimised by `ParsleySimShrinker`
  (shortest failing prefix, then ddmin, same-failure-signature preserving). New fault modes
  compose with the existing seeded scheduling: dirty restarts (no final ack fold; the I8
  end-offset seed carries the recovery), consumer offset rollbacks into the already-delivered
  region (the A11 redelivery net, with a new no-duplicate-delegate-delivery invariant), and
  random A5/A6 scope-change restarts. `ParsleyTopologyGen` generates seed-deterministic random
  topologies classified by structural feature, and `ParsleyRandomTopologyPropertyTest` sweeps
  them under rotating fault profiles with population-level vacuity guards — system-property
  scalable from the CI default (~1s) to deep sweeps; `ParsleySimSoakTest` (opt-in,
  `-Dparsley.sim.soak=true`) adds a 500k-step leak-and-stall soak. Cross-JVM seed
  reproducibility fixed on the way (pollable-input iteration was salted `Set` order), plus two
  simulator fidelity gaps the explorer itself surfaced: null-message sends now ack and fold into
  `ownOutputs` like any send, and node (re)starts now run production's post-init
  `drainAfterRestore` pass.
- **Mid-run topic recreation now fails the application fast (T3.4, assumption E1).** Topic names
  resolve to their stable Kafka UUIDs once, at task initialisation, so deleting and recreating a
  causal topic while an application runs would silently rebind causal coordinates: a recreated
  *input* whose new log re-passes the member's committed offset resumes fetching with no client
  error, labelling the new incarnation's records with the old identity, and a recreated *sink*
  turns every producer-ack fold into a monotone no-op, so stamps quietly stop claiming the node's
  own new outputs. `CausalStreams` now runs a background topic-identity poll comparing the
  broker's current topic IDs — inputs and sinks — against what each task resolved at
  initialisation; on a detected change or deletion every task fails fast before ingesting or
  stamping anything further (`CausalTopicRecreatedException`). Detection is bounded by the poll
  interval (5 seconds), not instantaneous, so live recreation of a causal topic remains an
  operational error — the guard makes it loud and bounded instead of silent and indefinite.
  Restarting after a recreation stays safe: identity is re-resolved at initialisation, and the
  old incarnation's history reads as lost, never reordered.
- **Genesis cohort barrier and an authoritative member-app roster for topology-epoch coordination.** A
  coordinated domain now always establishes a committed *genesis* epoch — a consistent cut with an empty
  floor — rather than running uncoordinated until the first explicit transition. Genesis does not commit
  until the whole founding cohort has declared (every app in the roster, present with its full task set),
  so no founder is left behind and later mis-admitted at a non-empty floor (which would strip a fan-out
  coordinate's genesis-era records — a silent effect-before-cause). A new required-for-multi-app config,
  `parsley.coordination.member-apps`, lists every `application.id` in the domain; it is the authoritative
  membership, declared identically by every app. After genesis, a roster change is admitted only once
  every committed member has re-declared the new roster (redeploy the incumbents naming the newcomer);
  apps that declare incompatible rosters refuse to commit or admit until the configs agree (fail-closed
  for progress — the domain keeps delivering under the last floor). When unset, an app defaults to a
  single-app roster of its own id, so a lone causal node coordinates with no extra configuration. Founders
  do not block at genesis (the empty floor is safe to consume from immediately); every later join blocks
  until an epoch computed with it commits. The epoch-events wire format changed (new record tags); a domain
  coordinated by an older Parsley must reset its epoch-events topic, which is safe before genesis commits.
- **`parsley.coordination.domain-topics`** (comma-separated; only meaningful alongside
  `parsley.coordination.epoch-events-topic` — the reverse is not required, so an existing coordinated
  deployment needs no change) declares the full coordinated domain's topic set — every member's inputs
  and sinks, external sources included. `CausalTopology#assemble` uses it to auto-wire, for each stage, a
  domain topic that stage does not otherwise consume or produce as an extra, raw `byte[]`/`byte[]` source
  feeding that stage's own processor node, so `validateFullMeshCoverage` (see above) can pass without a
  hand-wired, redundant business subscription. `ParsleyProcessor` recognises a passthrough record by its
  own source topic (never a header): it flows through the ordinary completeness gate exactly like any
  other channel, contributing its causal progress to the frontier, but is never handed to the delegate —
  every *other* record a passthrough delivery happens to release from the shared buffer as a side effect
  still reaches the real delegate correctly, since there is only ever one delegate per processor node.
  Verified end to end against a real broker, including a genuine two-application cyclic topology
  (A→B→A, B's visibility into A's root topic supplied entirely by passthrough) delivering correctly —
  the headline capability this whole redesign exists to enable.
- **`mvn test` (and therefore CI) now fails on a unit-test coverage regression.** Jacoco's `check` goal
  gates the overall bundle at 80% instruction / 75% branch coverage — a few points below the current
  86.0%/80.9% baseline, so routine refactors have headroom but a real drop fails the build. Scoped to the
  bundle total rather than per-class: several classes are exercised only by the Testcontainers ITs this
  report excludes (matching the existing mutation-testing exclusion), so a per-class minimum would fail
  the build on files with no real gap.
- **Dead-letter sink: the only liveness escape from causal delivery, fired solely on proven
  impossibility.** `CausalTopology#assemble` gives every stage its own dead-letter sink node (never one
  sink shared across stages — that would union their Kafka Streams node groups), all writing to one
  topic name: `parsley.deadletter.topic` if set, else `{application.id}-deadletter`.
  `CausalStreams#start()` provisions that topic (partitions from the new `parsley.deadletter.partitions`,
  default 1) before starting the underlying `KafkaStreams`, tolerating a concurrent creation race.

  A record is dead-lettered only when its dependencies are *proven* unsatisfiable, never on pressure or
  time: a poison record (undecodable on the forward path), an unresolvable causal-dependencies header at
  ingest, or a dependent of either. A dead-lettered coordinate's frontier can never legitimately advance
  again, so it is recorded in a new durable, changelog-backed **orphan index** (`ParsleyOrphanIndex` /
  `RocksOrphanIndex`, mirroring `ParsleyForwardedIndex`); `ParsleyEngine` then worklist-scans this node's
  own buffer for anything depending on that coordinate at or beyond its floor and dead-letters those too,
  recursively — Lamport transitivity in reverse. This is local to one node's own buffer: a *different*
  node still buffering on the same doomed coordinate just sees a channel that stopped advancing,
  indistinguishable from ordinary lag, until a forced epoch-floor advance (a later change) resolves it
  DAG-wide. Without a dead-letter sink configured (the low-level `ParsleyProcessors` builder path, unless
  `.deadLetterSink(...)`/`.sinkNodeNames(...)` are called explicitly), a proven-impossible record still
  fails the task fast, exactly as before this change.

  The dead-letter record carries raw bytes (a poison record's value can never be reconstructed as `V`) and
  new headers: `parsley-deadletter-reason` (`POISON`/`UNRESOLVABLE_CLOCK`/`ORPHAN_CASCADE`),
  `parsley-deadletter-source-topic`/`-source-topic-id`/`-source-partition`/`-source-offset`, and, for an
  unresolvable clock, `parsley-deadletter-original-dependencies` (the undecodable bytes, verbatim, for
  operator forensics). `CausalAudit` gains `recordDeadLetter(topic, partition, offset, reason)`, fired for
  every dead-lettered record including a cascade victim that was never itself a deserialization/clock
  failure. `ParsleyMetrics` gains a `dead-lettered` rate-total sensor.
- `ParsleyCoordination` — the public handle that turns on **topology-epoch coordination**, so a causal
  topology can evolve (add/replace a stage, recompile) across a well-defined epoch boundary without a
  new node dragging obsolete pre-epoch history into causal time. Create one over a shared
  single-partition epoch-events log topic and register it with every participating stage via
  `CausalStreams.Builder#withCoordination` / `ParsleyProcessors.Builder#withCoordination` (mirroring
  `withQuiesce`); call `requestEpochTransition()` to evolve the running topology through a boundary,
  and `close()` in shutdown. The coordination is **leaderless**: every instance folds the totally
  ordered epoch-events log identically (a per-round elected owner computes each epoch's floor as the
  min over running members' completeness), and the floor propagates **in-band** via markers that
  relay edge-by-edge through the DAG, so each node adopts it through the overlapping-epoch transition.
  Entirely **optional** — without a `ParsleyCoordination` a topology runs in epoch 0, exactly as
  before. A node **deployed into an already-running** topology blocks at startup until an epoch
  computed without it commits, then adopts that floor and replays its inputs from the start with
  pre-epoch history stripped, so it never drags the shared floor down; on a configurable timeout it
  fails to retry rather than proceed on an unknown floor. A **gone** member (a decommissioned or
  crashed app) cannot freeze the domain: a round that waits too long for it **evicts** it through the
  log after a configurable timeout, and — since a complete round is committed by any node, not a
  single owner — a gone owner cannot freeze it either. A clean decommission uses
  `ParsleyCoordination.leave()`; a restart keeps the member in the domain and returns. The topology's
  **external source topics** (entry points produced outside the topology, on which no in-band marker
  arrives) are **derived from the log**, not configured: every stage declares its input channels and
  sink topics on join, and a topic some member consumes but no member produces is an external source.
  Declare sink topics via `CausalStreams.addSink(...)` (automatic) or the new
  `ParsleyProcessors.Builder#sinkTopics(...)` on the low-level path.
- `CausalStreams` — the topology-owning high-level causal API (Layer 2), composing
  `ParsleyProcessors` internally rather than reimplementing the causal engine. Builds a `Topology`
  for a single causal stage — one or more `ParsleyBuffer` sources feeding a causal-decorated
  processor, forwarding to one or more named sinks — so it drops straight into
  `new KafkaStreams(topology, props)`. Use it instead of the low-level `ParsleyProcessors` decorator
  whenever a topology needs sink-side guarantees the decorator alone cannot provide: a uniform sink
  partitioner, co-partitioning validation across sinks (not just inputs), and a `cleanup.policy`
  check (below). Path integrity — no non-Parsley processor spliced between causal nodes — holds by
  construction: the builder exposes no way to add one.
  - `CausalStreams.Builder#withPartitioner` applies one `StreamPartitioner` uniformly to every sink
    a stage declares (default: Kafka's own key-hash partitioner), so causal sinks in the same stage
    can never drift onto different partitioners. Must read only the key — a watermark carries a
    null value and reuses its triggering record's key, so a value-based partitioner cannot route it.
  - A delivered record the delegate forwards to only one named sink still has its stand-in
    watermark (emitted when the delegate forwards nothing for a given input) reach every sink
    connected to the processor node — Kafka Streams' own broadcast behaviour for an unqualified
    `context.forward`, now exercised through a real multi-sink topology.
  - `ParsleyProcessors.builder(...)` rejects a `userSupplier` that is already a
    `ParsleyProcessorSupplier` with an `IllegalArgumentException`, instead of silently building a
    nested double-decoration that would buffer and stamp every record twice and corrupt the
    frontier. The guard lives at this single entry point, so `CausalStreams` (which calls it
    internally) is protected with no separate check.
- `parsley.topology.validation` — startup validation of topology misconfigurations a causal
  processor can detect: its causal input topics not sharing a partition count, which makes
  co-partitioning impossible, and, when built through `CausalStreams`, that stage's sink topics too
  — both their partition counts (folded into the same parity check) and their `cleanup.policy`
  (checked for `compact`, since a protocol watermark is a null-value record wire-indistinguishable
  from a compaction tombstone and can be compacted away before a slow consumer reads it). `warn`
  (default) logs a mismatch and continues, `strict` fails the task fast, `off`
  disables the checks entirely (no admin round-trip). A bare `ParsleyProcessors` decorator only ever
  sees its own input topics, so the sink-side checks apply only through `CausalStreams`. Each sink
  is resolved independently, so one sink that cannot be described (e.g. not yet created) never
  masks a genuine misconfiguration on a different sink in the same stage, even under `strict`.
  `ParsleyTopicAdmin` gained a `cleanupPolicies` method to support this.
- `ParsleyQuiesce` — a shared handle for coordinating graceful shutdown across every causal task in
  one application instance. Register it with `ParsleyProcessors.Builder#withQuiesce` /
  `CausalStreams.Builder#withQuiesce`; call `requestQuiesce()` from your own shutdown path and poll
  `isSafeToClose()` before calling `KafkaStreams#close`. A registered task keeps processing exactly
  as it does today — it only reports itself drained once its buffer empties through the ordinary
  delivery path (a held record's dependency becoming satisfied by a later message), never by
  fabricating completeness. This is a stall-avoidance optimization, not a correctness requirement:
  every held record is already changelog-backed and survives an ungraceful stop regardless.
- `CausalDependencies.isWatermark(ConsumerRecord)` — identifies a protocol watermark so a plain
  Kafka client consuming a Parsley-produced topic can fold its carried completeness frontier with
  `observe` while skipping it as a business record. `observe` now folds a watermark's carried
  frontier only and never its own position, matching engine-side handling so client and engine
  frontiers stay consistent.
- `CausalBufferLimit.unbounded()` — a new limit that never evicts. Records are held until their
  causal dependencies are satisfied regardless of depth or wait time. Intended for deployments
  where uncoordinated producers make bounded limits impractical and causal ordering must never be
  violated. Callers must monitor buffer depth; if a dependency can never be satisfied (e.g. the
  producing topic was deleted), records accumulate without bound on the RocksDB state store and
  the Kafka changelog.
- `CausalBoundedBufferLimit` — a new public sealed interface that refines `CausalBufferLimit` and
  is implemented by all evicting limit types (`ofSize`, `ofDuration`, `first`). The `first()`
  factory now accepts `CausalBoundedBufferLimit` arguments rather than `CausalBufferLimit`,
  making `first(unbounded())` a compile error.

### Changed
- **The published POM's developer metadata carries the author's real name, and a new enforcer gate
  guards the Central-required metadata.** The `<developers>` entry now reads `Toby Clements` (held
  in a `developer.name` property that flatten resolves into the deployed POM) rather than repeating
  the `tobyjamesclements` GitHub handle as the name, and the `<description>` is aligned with the
  docs lead sentence. A second `maven-enforcer-plugin` execution, `enforce-central-metadata`, fails
  the build at `validate` if `name`, `description`, `url`, `scm`, a license name, or the developer
  name is blank (or if the developer name regresses to the bare handle), so a metadata regression
  surfaces on every `mvn verify` instead of at the tag-triggered Central deploy.
- **Two internal methods are rewritten for expressiveness with Java 21 language features.**
  `ParsleyProcessor.deliveryTimeoutMs` now dispatches on the configured value with a `switch`
  expression (a guarded `case String text when ...` and an explicit `case null, default`) instead
  of a sequence of pattern-`instanceof` returns, and `KafkaTopics.resolveVia` names the
  `ExecutionException` cause once rather than calling `getCause()` three times. Behaviour is
  unchanged.
- **The documentation site is restructured to lead with its causal-consistency foundations and
  the three protocols.** The academic material is promoted from the former `Reference → Internals`
  into two top-level sections: `Foundations` (the causal-consistency model and the named
  invariants) and `The three protocols` (channels, causal broadcast, gossip, with the layered
  overview as the section landing page). Practical pages regroup under `Using Parsley`, and
  implementation reference (the processor embedding, wire format, naming) under `Reference`,
  joined by a new `Bibliography`. Files move out of `docs/internals/` into
  `docs/{foundations,protocols,guide,reference}/` accordingly; every cross-link and the
  `mkdocs.yml` navigation is updated, and the Material theme gains navigation tabs, section index
  pages, and footnote-rendered citations. The `docs/getting-started.md` and `docs/streams.md`
  path references in `DocsSamplesTest` follow the pages to `docs/guide/`. Page prose is rewritten
  section by section in the changes that follow; the `Concepts` page is absorbed and the
  `Performance` page dissolved into the protocol pages and Configuration there.
- **The landing pages lead with the academic framing.** The docs home page, the root Javadoc
  `overview.html`, and `README.md` now open by situating Parsley in the causal-consistency
  literature (Lamport's happened-before relation and vector clocks), state the two classical
  assumptions Kafka stream processing breaks (total visibility; reliable FIFO channels), and
  present the three-protocol stack with its lineage before the public-API orientation. The
  `overview.html` typical-usage sample adopts the `c1`/`c2`/`c3` topic naming the rest of the
  docs samples use.
- **The Foundations section is rewritten and split into four pages.** The former single
  causal-consistency page becomes `Causal consistency` (happened-before, vector clocks, the two
  classical assumptions Kafka breaks, the layer-to-assumption mapping, and the rejected epoch and
  hold-until-admitted designs), a new `The delivery gate` page (the two-branch gate, why local
  delivery is required, why ignoring unconsumed coordinates is sound, the outbound stamp, and the
  fail-closed violation model), and a new `Environmental assumptions` page (E1–E3). `Named
  invariants` gains an academic framing. Primary sources are cited as page footnotes into the new
  Bibliography. Inbound links across the site are repointed: gate and stamp references now target
  the delivery-gate page and E1–E3 references the assumptions page.
- **The three protocol pages are rewritten and the standalone Performance page is dissolved into
  them.** The protocols overview becomes the section landing page, carrying a consolidated cost
  model table whose rows link to the protocol page that owns each mechanism, and refined
  further-reading links to the four Foundations pages, the processor, wire format, and naming. Each
  protocol page gains a Cost section: state persistence and restore on `channels`, the per-record
  clock walks, buffer drain, and crossing-wait produce serialization (with the
  `producer.linger.ms` remedy for multi-forward delegates) on `causal-broadcast`, and null-message
  volume on `gossip`. Each page cites its lineage as a footnote (Hadzilacos–Toueg, BSS,
  Chandy–Misra–Bryant, Demers). Stale "internals overview" and mislabelled model links are
  corrected. `docs/performance.md` is deleted; its cost model now lives with the protocols and its
  metrics guidance with Configuration.
- **The Using Parsley guide is rewritten in the academic register and the Concepts page is
  absorbed.** Getting started gains a "Consuming a causal topology's output" section carrying the
  consumer-side frontier and null-message handling that lived on Concepts, and cross-links the edge
  frontier to the vector-clock foundations. Streams integration opens by placing the stage inside
  the three protocols and repoints its E2/E3 references to the specific assumptions. Incremental
  adoption, Configuration, and Troubleshooting gain academic framings and links into Foundations;
  Configuration gains a "Performance and tuning" section pointing to the protocols cost model and
  the `producer.linger.ms` remedy. `docs/concepts.md` is deleted: its theory already lived in
  Foundations and Streams, and its unique practitioner material moved to Getting started. The five
  compile-pinned samples are unchanged. Inbound Concepts links are repointed to Streams and Getting
  started.
- **Reference is completed with a Bibliography and reframed pages.** The former internals "Streams
  integration" page becomes "The processor", framed explicitly as the Kafka Streams embedding of the
  three protocols rather than a fourth protocol. Wire format and naming gain leads linking into the
  protocols and the new Bibliography, which collects every source the Foundations, protocols, and
  naming pages cite (Lamport 1978; Fidge and Mattern 1988; Schwarz–Mattern 1994; BSS 1991;
  Hadzilacos–Toueg 1994; Cachin–Guerraoui–Rodrigues; Bryant 1977; Chandy–Misra 1979; DeVries 1990;
  Cai–Turner and Wood–Turner; Demers et al. 1987; Wuu–Bernstein and Sarin–Lynch; COPS 2011), grouped
  by theme, with the page footnotes linking into it.
- **The Javadoc is made far more concise and API-focused, staying standalone.** The public
  `Causal*` types and the package overview are trimmed to state behaviour and the API contract
  rather than re-derive the causal model: `CausalClock` drops the repeated relay/fan-in catalogue
  and the internal null-message-fold reasoning from its method docs; `package-info` drops the
  internal-protocol-module section (which linked package-private types a public reader cannot
  navigate to) and compresses the rest; the builder chain and `CausalStreams` shed restated
  rationale and links to package-private types. The tone matches the documentation site, but the
  Javadoc reads on its own without it. The verbose internal `Parsley*` module comments are condensed
  in the same pass. `mvn javadoc:javadoc` stays clean under doclint.
- **NullAway on the test sources is promoted from WARN to ERROR (closes #27).** The test tree is
  made null-safe: exception assertions read `getMessage()`/`getCause()` through non-null
  `ParsleyTestFixtures.message`/`cause` helpers; nullable map, store, and admin lookups are wrapped
  in `requireNonNull`; test-double and helper returns that can be null are annotated `@Nullable`;
  and the few deliberately-null-input tests carry a scoped `@SuppressWarnings("NullAway")`. The
  generated `io.github.tobyjamesclements.parsley.avro` subpackage is excluded via
  `UnannotatedSubPackages`. Both the main and test compile passes now run `-Xep:NullAway:ERROR`, so
  a nullness regression in either tree fails the build.
- **The artifact now targets Java 21.** `maven.compiler.release` drops from 25 to 21: no main
  source used a post-21 API, so the 25 floor only excluded consumers pinned to an LTS. Building
  from source still requires JDK 25 for the Error Prone / NullAway toolchain, and the build now
  enforces that with maven-enforcer-plugin (JDK 25+, Maven 3.9+, dependency convergence — with
  `slf4j-api` pinned to the 2.x line over kafka-clients' 1.7.36 declaration — and duplicate
  dependency declarations banned).
- **The published POM is now consumer-facing, and builds are reproducible.** flatten-maven-plugin
  rewrites the deployed POM to drop the build machinery — most importantly the Confluent
  repository, which serves test-scope dependencies only but which every downstream build would
  otherwise consult during resolution. A fixed `project.build.outputTimestamp` makes rebuilding a
  tag yield byte-identical artifacts. The JMH annotation processor is scoped to test compilation
  instead of running over main sources. The getting-started page no longer claims `kafka-streams`
  arrives transitively — it is an optional dependency: an application using the `CausalStreams`
  runtime declares it itself, and `CausalClock` edge stamping needs only `kafka-clients`. The
  stale POM comment justifying the optional flag with class names deleted in the redesign is
  rewritten against the current types.
- **BREAKING: the exceptions that escape into the application's uncaught-exception handler are now
  public API, under one root.** Parsley's fail-closed throws always propagated out of Kafka Streams
  into the handler `CausalStreams.setUncaughtExceptionHandler` exposes, but the types were
  package-private — a handler deciding thread-replace vs shutdown could only string-match class
  names, and the source-coordinate diagnostics on the shared base were unreachable. The hierarchy
  is now public and unified: `CausalDeliveryException` (the root; extends `RuntimeException`) —
  with `CausalCoordinateException` (abstract; exposes the `topic`/`topicId`/`partition`/`offset`
  quartet) over `CausalBufferDeserializationException` and `CausalVectorClockResolutionException`,
  and `CausalTopicRecreatedException` and `CausalPendingAckException` directly under the root. The
  renames follow the public-API naming convention (`Parsley*` → `Causal*`), the two types that
  extended `IllegalStateException` no longer do (the ad-hoc second root is gone; a handler that
  matched on `IllegalStateException` must match `CausalDeliveryException` instead), constructors
  stay package-private (only Parsley raises them), and each type's Javadoc now states whether a
  restart heals the condition. `encodedDependencies()` returns a defensive copy.
- **`CausalStreams` gains a bounded `close(Duration)`, and a failed construction no longer leaks
  its JVM-wide registrations.** The new overload budgets the whole shutdown — the causal drain
  wait, then stopping the underlying `KafkaStreams` with whatever remains, mirroring
  `KafkaStreams.close(Duration)` — for callers that cannot block unbounded, a JVM shutdown hook
  above all. Giving up on the drain never delivers a record early: a truncated drain leaves held
  records in the changelog-backed buffer to replay in causal order on the next start, so the
  no-arg `close()` remains the routine path. Separately, the constructor registers this instance
  in two JVM-wide registries (the producer interceptor resolves them from config) *before*
  building the `KafkaStreams` instance, whose constructor throws on a bad Streams configuration —
  and a failed construction hands the caller no instance to `close()`, so both registrations
  leaked for the JVM's lifetime. The constructor now rolls them back on the way out.
- **`CausalStreams` passes through `metrics()` and `setStateListener`.** Parsley registers its
  per-task sensors (records buffered and released, deserialization and clock-resolution failures,
  ignored out-of-scope dependencies, and the rest of the `parsley` group) on the wrapped
  instance's registry, but the facade offered no in-process way to reach them — an operator had to
  go through JMX. `metrics()` now exposes the registry and `setStateListener` the state
  transitions, alongside the existing `state()` and `setUncaughtExceptionHandler`; the facade
  still deliberately stops short of re-exposing the whole `KafkaStreams` surface.
- **The naming convention is stated in `package-info.java`.** Public types are `Causal*` (with the
  reflectively-instantiated `ParsleyOwnOutputInterceptor` as the documented forced exception);
  package-private machinery is `Parsley*`, except implementations of a `Parsley*` seam interface,
  which are named for their backing (`KafkaTopics`, the `StoreBacked*` stores). The four
  backing-named internals were flagged as drift in an architecture review; they are a deliberate
  idiom, so the convention now says so rather than the classes being renamed.
- **Javadoc doclint is on (`all,-missing`).** Broken links, malformed HTML, and bad references now
  fail the Javadoc build instead of rotting silently; only exhaustive `@param`/`@return` tagging
  stays unchecked, since the documentation style here is prose. Turning it on surfaced real drift
  in `overview.html`: the usage example still called `build()` on the builder and the text still
  said a builder declares "one or more" causal stages — both stale since the one-stage redesign
  made the fluent chain's terminal `.build()` (on `CausalProcessedStream`) the only way to produce
  a topology. The example and both descriptions are corrected, the section headings sit at the
  level doclint expects for an overview page, and the entry-points table gains its caption.
- **Adversarial-review follow-ups.** The strict partition-parity failure now names the
  `parsley.topology.validation=warn` opt-down for intentionally mismatched (re-keyed fan-out)
  topologies; the held-records-from-a-removed-input failure and its troubleshooting entry state
  what happens when the removed topic was also deleted (redeclaring the recreated successor purges
  the records as destroyed history rather than draining them); two Javadoc leftovers from the
  strict-sink-resolution change are corrected (`declareSinks` no longer describes best-effort
  resolution, and the ack fold's missing-translation skip is documented as a defensive guard, not
  a multi-stage filter — a topology is exactly one stage); and the `delivery.timeout.ms`
  resolution's happy paths are pinned directly.
- **Hygiene sweep.** `CausalStreams` now works on a copy of the caller's `Properties`, so
  constructing two instances from one object can no longer duplicate the producer-interceptor
  entry or cross-wire registry ids. Test-only machinery left main: `ParsleyCausalBroadcast` keeps
  only its full constructor and `ParsleyChannels` only its store-backed one (the in-memory and
  predicate-defaulting convenience shapes moved to a test fixture factory, and the unused
  `trackChannels=false` mode is deleted). `ParsleyGossip`'s constructor takes the broadcast core
  alone and reads the channels module through it. Duplicate codecs consolidated: the dead
  string helpers in `ParsleyByteUtils` are deleted and `ParsleySerializer` uses
  `ParsleyByteUtils`' UUID codec, retiring `ParsleyHeader`'s copy. An orphaned Javadoc block is
  reattached to `validateCleanupPolicy`. And `performance.md` gains a fourth cost category —
  crossing-wait produce serialization: a multi-forward delegate pays roughly
  N × (linger + replication round trip) per invocation, because each business forward's stamp
  waits for the previous forward's acknowledgement; with Kafka Streams' default
  `producer.linger.ms` of 100 ms this dominates every other documented cost, so the section
  carries the tuning guidance (lower linger for multi-forward delegates) and why a per-partition
  exemption is impossible (a business forward's destination partition is unknowable at stamp
  time).
- **BREAKING: `parsley.topology.validation` now defaults to `strict`.** A detectable topology
  misconfiguration — the causal topics not sharing a partition count, or a sink whose
  `cleanup.policy` includes `compact` — now fails startup unless the deployment explicitly opts
  down to `warn` or `off`. Both misconfigurations were deferred failures anyway: a parity mismatch
  crash-loops the protocol-marker produce at runtime, and a compacted sink can silently lose null
  messages. A topology that *intentionally* mismatches partition counts (for example fanning a
  source into a wider, re-keyed sink) must now set `warn` explicitly. Also strict:
  a malformed `delivery.timeout.ms` override now fails startup naming the key and value instead of
  silently falling back to 120 s — it bounds the crossing wait and the stall diagnostic, so a typo
  must not quietly become the default (an absent key still defaults to Kafka's 120 s).
- **Null messages carry the triggering record's timestamp instead of the wall clock.** Kafka
  Streams advances downstream stream time from every polled record's timestamp before any
  processor classifies it, so a wall-clock-stamped null message emitted during a reprocessing run
  over historic event times yanked downstream delegates' windows, grace periods, and suppressions
  to now. All three emission paths now stamp the trigger's own timestamp: the delivered record's
  on the non-emitting path, the buffered record's on the heartbeat path, and the received null
  message's own on the relay path — downstream stream time then advances only as the data's time
  does. The retention trade is documented in the gossip internals page: a segment holding only
  null messages looks old to time-based retention exactly when its triggers are old (a backfill),
  which retention on causal topics must already cover (E2's existing sizing constraint), and an
  undersized retention now fails as `AutoOffsetReset.none()`'s loud stall rather than silent
  event-time corruption.
- **Restored held records whose source topic left scope get an explicit disposition at startup.**
  Previously a supported redeploy could strand them: a held record from a removed input crashed
  the task on an untyped serde error at every restart (recoverable only by a full reset), or hung
  `close()` forever if its dependencies never resolved, and a held record from a recreated input's
  old incarnation could deliver a destroyed record and re-enter its purged coordinate into the
  frontier. Now, at initialisation: a current input's records restore unchanged; a recreated
  input's old-incarnation records are purged with an INFO log (deleted history — never delivered,
  never reordered); and a removed-but-alive input's records fail startup loudly, naming the topics
  and counts and the two remedies (redeclare the input so they drain through ordinary delivery, or
  perform a full reset). See the new troubleshooting entry.
- **A null message with an undecodable clock header, or on an unregistered topic, now fails the
  task.** Both branches previously warned and continued: an undecodable carried clock was treated
  as empty (folding nothing while still delivering the offset — permanently dropping the emitting
  peer's progress claims from the channel fold, so later stamps under-claimed them), and an
  unregistered-topic null message was skipped (committing the offset past a record on a channel the
  node claims not to know). Both now mirror the business path exactly: the present-but-undecodable
  header throws `CausalVectorClockResolutionException`, the unregistered topic throws the same
  `IllegalStateException` as business ingest, the transaction aborts, and the record is refetched
  on restart. An absent header is unchanged: an empty carried clock whose offset is still
  delivered — a producer that stamps nothing claims nothing.
- **BREAKING: a declared sink topic must exist before the application starts.** Sink UUID and
  end-offset resolution at startup is now strict: a sink that cannot be resolved — the topic does
  not exist, or the admin call fails — fails startup with an `IllegalStateException` naming the
  sink and the remedy, the same treatment inputs already get. Previously both failures logged a
  warning and continued, which silently disabled own-output stamping for that sink for the task's
  entire run (and, for a failed end-offset read, skipped the seed that covers the previous run's
  final-transaction acknowledgements) — stamps then under-claimed the node's own outputs, and a
  downstream consumer of two sinks could deliver an effect before its cause. Broker auto-creation
  on first produce is no longer a supported path for causal sinks; create all topics, sinks
  included, before first start.
- **Redesign cleanup (T4.3).** Dead code left behind by the coordination-subsystem deletion is
  removed: the unused string-set wire helpers in `ParsleyByteUtils` (only the deleted epoch roster
  section read or wrote them), the unreferenced `ParsleyChannels.channelGet` accessor, the
  test-only `ParsleyHeader.isInternal()` predicate (the intake rule it mirrored is enforced via
  `INTERNAL_PREFIX` and covered end-to-end by the topology tests), and the uncalled
  `withConfigs(Map)`/`withConfig(Properties)` convenience overloads on the internal processor
  builder. `rescope`'s Javadoc, stranded above an unrelated accessor when the former-sink heal
  methods were inserted between it and its method, is reattached. Remaining pre-redesign prose is
  corrected: the builder's "sharing a coordination log" guidance (there is no coordination log —
  multi-stage pipelines are applications chained topic to topic), three "passthrough" asides, and
  a Javadoc link still naming the renamed `CAUSAL_DEPENDENCIES` header constant.
- **BREAKING: the protocol vocabulary moves to its academic names — wire format and public API
  (T4.1).** Parsley's progress marker is a *null message* in the Chandy–Misra–Bryant sense (a
  timestamp-carrying record whose value is literally null), and the type stamped on records is a
  *vector clock*, so both now say so. On the wire: the marker header `_parsley_watermark` is
  renamed `_parsley_null_message`, and the clock header `parsley-causal-dependencies` is renamed
  `parsley-causal-clock` (encodings unchanged). In the public API: `CausalDependencies` is renamed
  `CausalClock` — the type plays both classical vector-clock roles (attached to a record it is the
  message timestamp VT(m); accumulated at an edge it is the process clock VT(p)) and
  "dependencies" misdescribed the accumulating half — and `isWatermark(record)` is renamed
  `isNullMessage(record)`. No compatibility aliases and no migration path: pre-1.0 versions have
  no upgrade path (upgrades are fresh starts), so old-header records are simply unreadable by this
  version. Documentation vocabulary updated throughout.
- **The record path is decoupled from topology-epoch coordination; the `parsley.coordination.*`
  keys are inert (T3.2, decisions D3/D4/D7 — breaking).** Every epoch consultation is out of the
  hot path: the interim below-floor dependency strip is deleted (`normalize` is now a pure
  self-cycle strip — no floor can rise under a held record, so the gate's re-evaluation is of a
  pure function), the frontier's epoch-anchored seeding/delivery/bridge guards are gone (a
  channel's baseline is its first-seen offset alone), the epoch boundary/snapshot markers are no
  longer emitted, relayed, or recognised (null messages keep their path unchanged), and `init()`
  no longer joins a coordination runtime or blocks on any barrier — a joiner just starts
  consuming, replay self-gating into causal order through the ordinary hold-back queue. The
  coordination configuration keys are accepted but wire nothing (a warning is logged when one is
  present; `CausalStreams.requestEpochTransition()` now always throws, naming the removal) — they
  are deleted outright, with a loud startup failure, in the next release, together with the
  now-orphaned subsystem sources. Two deliberate behavioural reverts ride along: a sink
  partition-count mismatch under the default `warn` validation no longer escalates to a hard
  startup failure when coordination keys are present (the escalation rode on the subsystem;
  `strict` still fails fast, and the produce-time consequence is documented), and the
  `parsley.coordination.*` cross-checks ("domain-topics without epoch-events-topic") no longer
  fail startup — an inert key cannot be misconfigured. The persisted frontier blob drops its
  epoch section (a format break; pre-1.0 has no upgrade path — upgrades are fresh starts, so
  pre-T3.2 state is never read). New broker ITs certify what replaces the join barrier: a joiner
  replaying a two-partition topology mid-run delivers every historical cause before its derived
  effect with zero coordination, and a late joiner consuming an input and its derived topic
  orders every fully-historical pair correctly — the exact hole floored replay stamps used to
  open.
- **The delivery gate is now the two-branch dispatch: consumed coordinates gate, everything else
  is ignored (T3.1, decision D1).** A dependency on a coordinate this node consumes (an input
  channel of the task, on the partition it owns) must be covered by the node's own contiguous
  delivered frontier, exactly as before. A dependency on any other coordinate — an unconsumed
  topic, a partition another task owns, a reflected claim on the node's own sink — is now
  *ignored*, unconditionally, instead of failing the task:
  `ParsleyUnreachableDependencyException` and the fail-fast dispatch (invariant I7) are removed,
  and the `unreachable-dependency-errors` sensor is replaced by `deps-out-of-scope-ignored`
  (one count per ignored coordinate). Ignoring is sound by the transitivity theorem the Phase 2
  work certified: with transitively complete stamps (I2) carried by unconditional merges (I9),
  any consumed causal ancestor of a record is claimed directly in that record's own clock, so an
  unconsumed entry only ever proxies ancestry the clock already states — ordering observable at
  the node is unchanged, while topologies the fail-fast made impossible (independent sources
  joined across an unconsumed intermediate topic, cross-partition funnel claims, uncoordinated
  cycles) now just work. The interim gate-side own-sink strip is deleted with it: a self-consumed
  sink's claims are genuinely gated (closing the shared-sink blindspot, where a claim about
  *another producer's* record on a shared sink was vacuously satisfied), and an unconsumed sink's
  reflected claims fall to the ignore branch. Cross-partition references on a consumed topic — a
  co-partitioning misconfiguration signal — shift from hard runtime failure to startup validation
  (`parsley.topology.validation`, unchanged) plus the ignore metric. New broker ITs cover the
  unconsumed-intermediate join, the shared-sink ordering fix, a two-app cycle with zero
  coordination, clockless producers (causally minimal by definition — no declaration needed), and
  the funnel's deferred delivery-order half; the property harness drops its interim fail-fast
  guards and adds a differing-scope chain sweep that certifies the ignore branch end to end.
- **Causal sources are consumed with `auto.offset.reset=none`, and their first-start offsets are seeded
  before start.** The frontier skip-bridge (see Fixed) treats an offset the consumer never returned as a
  transaction marker, which is only sound if the consumer can never silently jump forward over lost
  records — retention or `deleteRecords` outrunning a lagging consumer. Every causal source is now declared
  with `AutoOffsetReset.none()`, so Kafka Streams fails the task fast on an out-of-range committed offset
  instead of resetting over the loss. To keep a genuine first start working under `none()`,
  `CausalStreams.start()` seeds each source partition's log-start offset before the group is joined,
  refusing to seed when surviving causal state (a changelog topic) shows the missing offset is expiry
  rather than a true first start. A compacted source topic, and a record-skipping deserialization or
  processing exception handler, are also rejected at assembly, since both punch the same consumer-visible
  holes the bridge would misread as markers.
- **A causal topology declares exactly one stage, enforced at compile time.** `CausalStreamsBuilder` no
  longer accumulates stages: the fluent chain `stream(...).process(...).to(...)` now terminates in
  `CausalProcessedStream.build()` rather than a `build()` on the builder (the builder keeps only a
  package-private `build()` as an internal seam). There is no public term to open a second stage, and a
  second `process(...)` on one builder is rejected, so a topology is exactly one causal stage — matching the
  node model, where a causal node is one Parsley task (one stage on one partition). A multi-stage causal
  pipeline is deployed as multiple applications, each one stage, sharing a coordination log. Existing
  single-stage code is unaffected apart from calling `.build()` at the end of the chain instead of on the
  builder.
- **Internal class/API naming cleanup (no behaviour change).** Package-private renames only; no public
  API surface is affected. The changelog-backed store implementations `RocksBufferStore` /
  `RocksCandidateIndex` / `RocksForwardedIndex` — not RocksDB-specific (they run over any Kafka
  `KeyValueStore`, in-memory ones included) — become `StoreBackedBufferStore` /
  `StoreBackedCandidateIndex` / `StoreBackedForwardedIndex`. The source-registration record
  `ParsleyBuffer` becomes `ParsleySource` (it registers a source topic + serdes; it is not a buffer,
  and the old name collided with `ParsleyBufferStore`, the real buffer); its builder methods
  `addBuffer` / `addBuffers` become `addSource` / `addSources`. `ParsleyQuiesce`'s epoch-drain second
  role is split out into a dedicated, lighter `ParsleyQuiesceTracker` (no shutdown-arming;
  `allDrained()` reflects drain state alone), leaving `ParsleyQuiesce` solely the `CausalStreams`
  graceful-shutdown gate. The builder-only holder `ParsleyProcessors` is folded into
  `ParsleyProcessorSupplier.builder(...)` / `ParsleyProcessorSupplier.Builder`, removing a class.
  Internal design docs and `overview.html` updated to match.
- **Internal marker-forward and completeness naming cleanup (no behaviour change).** The triplicate
  `forwardWatermark`/`forwardEpochSnapshot`/`forwardEpochBoundary` in `ParsleyProcessor` collapse to one
  `forwardMarker(header, value, key)`; the misleadingly-named `stampFrontier` field (and
  `ParsleyProcessorContext`'s `frontier` constructor parameter) become `stampCompleteness`/`completeness`,
  since the stamped clock is the node's completeness, not its delivery frontier. The field's stale
  "read off-thread by the epoch runtime" comment is corrected — it is task-thread-confined (the
  off-thread publication rides `commitHook::committed`), so the field is no longer needlessly `volatile`.
- **Documentation drift swept.** `ParsleyEpochBoundary` no longer credits a "Topology Co-ordinator"
  (the protocol is leaderless: source-layer tasks inject, peers relay); `ParsleyEpochEvent` counts
  its five events (not four) and, with `ParsleyEpochLog`/`ParsleyEpochSnapshotPublisher`, describes
  the commit as any-node-appends-the-deterministic-fold rather than an owner's or coordinator's
  decision; member ids are documented as `application.id/taskId`, not bare task ids;
  `ParsleyBufferStore#remove` no longer references the deleted `entries()`; `ParsleyMessage` no
  longer claims `_parsley_src_*` routing headers or header-encoding at the buffer boundary (the
  serializer writes typed framing fields); `ParsleyConfig`'s domain-topics key describes the actual
  same-node passthrough wiring, not a "dedicated passthrough processor node";
  `ParsleyClock#mergeMin`'s contradictory "ignoring … is kept" wording now says what it does; and a
  stale `ParsleyKafkaEpochTransport` class name in the pom's coverage note is corrected.
- **Dead code and documentation residue swept out.** `ParsleyClock#missing` (its only production use
  was a computed-then-discarded local; the buffer-hold log line now prints the gating frontier
  restricted to the record's dependency coordinates instead), `ParsleyBufferStore#entries()` (no
  production caller — the drain path reads `indexEntries()` + `get()`; tests reworked likewise),
  `ParsleyTopicAdmin#createTopic` (orphaned when dead-letter topic provisioning was removed; its
  Javadoc still referenced "the outbox topic"), and an uncalled `ParsleyProcessorSupplier` constructor
  are deleted. Documentation residue from the retired coordinator-broadcast model is rewritten: the
  `ParsleyHeader` epoch-marker constants and `ParsleyEpochSnapshotPublisher` no longer describe a
  "Topology Co-ordinator" writing markers to every channel (the protocol is leaderless — source-layer
  tasks inject, peers relay, the log's fold decides); `ParsleyProcessors`' class Javadoc no longer
  reads as public-API documentation with an example no user can write (it is package-private wiring
  driven by `CausalTopology`); and `ParsleyProcessor#stampFrontier`'s `volatile` is documented as
  load-bearing (the epoch runtime's background thread reads it via `registerLocalCompleteness`), not
  "belt-and-suspenders".
- **Breaking: `CausalAudit` is removed entirely.** Per-record audit callbacks
  (`recordForwarded`/`recordHeld`/`recordReleased`/`recordDeserializationFailure`/
  `recordClockResolutionFailure`/`recordUnreachableDependencyFailure`/`processorInitialized`/
  `processorClosing`) are a production/compliance concern — routing events to a SIEM or durable audit
  trail — not something the causal-broadcast completeness gate or the topology-epoch coordination
  protocol need to prove themselves correct. `CausalAudit`, its safe-delegation wrapper `ParsleyAudit`,
  `ParsleyProcessors.Builder#withAudit`, `CausalProcessedStream#withAudit`, and `ParsleyStageSpec`'s
  `audit` field are all gone; every constructor that threaded an audit parameter (`ParsleyEngine`,
  `ParsleyProcessor`, `ParsleyProcessorSupplier`) drops it. `ParsleyMetrics` — the aggregate, always-on
  counters wired into the Kafka Streams metrics registry — is unaffected and remains the way to observe
  the algorithm's behavior. Deferred, not abandoned, matching the dead-letter entry below.
- **Breaking: dead-lettering and the orphan index are removed; delivery is unconditionally fail-closed
  on any error.** The dead-letter sink, `ParsleyOrphanIndex`/`RocksOrphanIndex`, and the orphan-cascade
  worklist scan (`ParsleyEngine#orphan`) added below are gone: a poison record, an unresolvable
  causal-dependencies header, or a dependency naming an unreachable coordinate now always fails the
  task fast — exactly the behaviour that already existed as the fallback when no dead-letter sink was
  configured — instead of being diverted. This closes the loop the "Breaking: fail-closed causal
  delivery" entry below left open (it said an explicit dead-letter path "will replace the fail-fast
  behaviour in a later change"; this change reverts to fail-closed-only instead). Motivation: the
  orphan-cascade mechanism added real complexity to compensate for a problem classical causal-broadcast
  systems (CBCAST, virtual synchrony) never had to solve, since they assumed a reliable transport; at
  this pre-1.0 stage the priority is proving the core algorithm and ergonomics on the simplest correct
  model, not carrying that complexity. Re-introducing dead-lettering (or an equivalent) is deferred, not
  abandoned.

  `CausalAudit#recordDeadLetter` is removed (the one genuinely public-API break). `ParsleyMetrics`'s
  `dead-lettered` sensor and `recordDeadLetter()` are gone. `ParsleyConfig`'s `parsley.deadletter.topic`
  and `parsley.deadletter.partitions` keys, and `ParsleyHeader`'s six `DEADLETTER_*` wire headers, are
  removed. `CausalTopology#assemble` no longer wires a per-stage dead-letter sink node, and
  `CausalStreams#start()` no longer provisions a dead-letter topic. `ParsleyProcessors.Builder` loses
  `deadLetterSink(String)`. `ParsleyCandidateIndex#findCandidatesRequiringAtLeast` (the orphan cascade's
  reverse-scan) is removed as dead weight alongside it.
- **The completeness gate required every one of a node's input channels to independently corroborate a
  coordinate before it counted (`ParsleyClock#intersectMin`), which permanently excludes a coordinate
  genuinely present on only one channel** — the documented "no ancestor with its own descendant" fan-in
  restriction existed only to route around this, and a node's private, unshared coordinate could be
  starved forever by an unrelated sibling channel that had no reason to ever mention it.

  `ParsleyFrontier#completeness` now takes the max-merge of a node's own frontier and every channel's
  advertised clock instead of intersecting them — a single genuine witness suffices, matching the
  Birman-Schiper-Stephenson CBCAST delivery condition instantiated directly on Kafka's own
  `(topicId, partition)` coordinates. `ParsleyClock#intersectMin` is deleted (its only caller); no wire
  format change. The channel-clock update that feeds this merge now happens only at the moment of a
  record's own genuine, gated delivery — never pre-loaded from the record's own claimed dependencies —
  so every message, including a node's own broadcast, is checked against this node's actual, already-
  proven state, never against a stamp the record itself supplies. The "no ancestor with its own
  descendant" restriction in `docs/internals/causal-consistency.md` is retired: under max-merge, a node
  consuming both an ancestor and its own descendant is ordinary, safe vector-clock composition.
- **A dependency on a coordinate this node has no input channel for at all (an undeclared topic, or a
  partition a different task instance owns) was silently dropped from the gate check and treated as
  satisfied** — sound only if the coordinate is genuinely, permanently irrelevant, but this node cannot
  actually prove that; it can only prove it has no way to check. Guessing "satisfied" traded an
  unbounded stall for an unproven delivery, which the causal-safety contract does not permit.

  A new `ParsleyClock.CoordinatePredicate` scope on `ParsleyEngine` now fails such a dependency closed
  instead: dead-lettered (`DeadLetter.Reason#UNREACHABLE_DEPENDENCY`) when a dead-letter sink is
  configured, or a hard task failure (`ParsleyUnreachableDependencyException`) otherwise — checked
  independently of whether dead-lettering is enabled, since scope is a static, structural fact, not
  something a missing DLQ should suppress. New `ParsleyMetrics#recordUnreachableDependencyError` and
  `CausalAudit#recordUnreachableDependencyFailure` hooks mirror the existing poison/clock-resolution
  failure signals. This removes the "carry an unconsumed coordinate through the stamp ungated, for a
  downstream node to enforce ordering against later" relay pattern some topologies relied on — that
  record now fails at the first node that cannot verify the coordinate, rather than passing through;
  route the coordinate through a genuine input branch instead (see "Independent inputs" in
  `docs/internals/causal-consistency.md`).

- **Topology-epoch coordination never checked that a multi-app DAG's declared subscriptions actually
  formed a full mesh** — a member could join and run indefinitely with real gaps (a downstream app never
  consuming an upstream app's own input topic), silently relying on the very vacuous-satisfaction
  behavior the fail-closed change above removes. Once removed, such a gap surfaced only as a data-path
  crash loop, discovered per record instead of at startup, or a round that silently hung forever.

  `ParsleyEpochLog` gains `domainTopics()` (∪ every declared member's inputs and sinks),
  `missingSubscriptions(memberId)`, and `isFullMeshSatisfied()` (true iff every running member's own
  subscriptions cover the whole domain) — the last is now a conjunct of `isRoundComplete()`, so an epoch
  can never commit while any running member cannot actually see the full domain. `ParsleyEpochRuntime`
  mirrors `domainTopics()` for cross-thread readers and logs a `WARN`/`INFO` transition when the mesh
  becomes insufficient or recovers. `ParsleyProcessor.init()` adds a startup self-check
  (`validateFullMeshCoverage`), called immediately after `awaitJoinCommit`, that fails fast — mirroring
  the existing `validatePartitionParity` coordination precedent, escalating the default `warn` mode to
  strict — when this member's own declared topics do not cover the known domain. A genuine multi-stage
  pipeline (app A produces a topic app B alone consumes) requires each member to also cover the topics
  only a sibling touches; see the new `parsley.coordination.domain-topics` passthrough wiring below for
  covering such a topic without a direct, redundant business subscription.

- **A received watermark or epoch marker (snapshot/boundary) was always relayed downstream unconditionally,
  even when it taught this node's channel nothing it did not already know** — sound only on an acyclic
  topology, since a marker's own delivery is itself a "genuine advance" by the old logic, so a cycle (a
  full mesh, or any marker-only passthrough channel) would ping-pong the same marker forever, formally
  identified as the one real gap in the max-merge model's termination proof.

  `ParsleyEngine.onWatermark` now takes the marker's own offset and marks that position genuinely
  delivered unconditionally — `seedIfFirstSeen`, then `deliver`, then `propagate`, exactly like a business
  record's own coordinate — so a marker-only channel's frontier still advances even though no business
  record ever flows on it (previously such a channel could never contribute to this node's own
  completeness). It returns a new `WatermarkOutcome` reporting whether the channel's carried clock
  genuinely changed. `ParsleyProcessor`'s three marker handlers (`handleWatermark`, `handleEpochBoundary`,
  `handleEpochSnapshot`, via the shared `advanceChannelClockFromMarker`) now relay downstream only when
  that report is `true` — gated on record kind (a marker's own delivery is never itself a reason to relay
  further), not on whether any business data changed, so a node that has already converged with its peers
  has nothing new to say and simply stops, without needing per-edge "already relayed this" bookkeeping.
  Source-layer marker injection (`injectSnapshot`/`adoptAndInjectBoundary`, driven by the coordination log
  rather than a received marker) is unaffected — it was already correctly gated on genuine epoch/round
  advance by its own counters.
- **Breaking: `CausalTopology#assemble` (and therefore `CausalStreams`) now requires
  `processing.guarantee=exactly_once_v2`, unconditionally.** The write-ordering fixes throughout
  `ParsleyEngine`/`ParsleyFrontier` (frontier-before-buffer-removal, orphan-before-buffer-removal,
  persist-before-prune) narrow an at-least-once torn-write window to a benign tear direction, but two
  separate changelog topics have no cross-store atomicity under at-least-once — a crash during the
  commit-time flush can, rarely, ack one topic's batch and lose the other's, so their "always tears
  toward the benign side" claims overclaimed slightly. Exactly-once-v2 wraps every state-store changelog
  write, every produced record, and the consumer offset commit into one Kafka transaction, so a crash
  genuinely cannot tear one write from the other at all — the same way a transactional producer requires
  `enable.idempotence`/a transactional id rather than treating it as optional hardening. Assembling a
  topology without it now fails fast with `IllegalStateException`, never gated by
  `parsley.topology.validation` (a correctness requirement, not a topology-shape lint).
- **Breaking: `CausalTopics` (since renamed `ParsleyTopics`) is no longer public; `CausalDependencies.using`/`builder` gain
  `Properties`/`Map<String, Uuid>` overloads directly.** `CausalTopics.of(Admin)` dated to an earlier
  design where Parsley avoided owning any Kafka client lifecycle at all — the caller constructed and
  closed its own `Admin` and handed it in. That no longer matches the rest of the public API (`CausalStreams`
  already owns its `KafkaStreams` instance, provisions topics, and owns quiesce/coordination internally),
  so the resolver type is now an internal implementation detail of `CausalDependencies` rather than a
  separate public type: `CausalDependencies.using(props)` / `.builder(props)` resolve topic UUIDs
  internally, and `.using(Map<String, Uuid>)` / `.builder(Map<String, Uuid>)` remain the broker-free path
  for tests. A `Properties`-backed resolver holds no live connection between calls — each distinct topic
  name is resolved (and cached) through a fresh, short-lived Kafka admin client opened and closed for that
  one lookup — so there is nothing for a caller to construct or close.
- **Breaking: `CausalAudit.recordDeserializationFailure`/`recordClockResolutionFailure` drop their
  trailing boolean.** `dropped`/`failed` were always hardcoded constants (`false`/`true` respectively)
  carrying no information; the new `recordDeadLetter` (above) is the actual disposition signal now that a
  proven-impossible record has a real second outcome besides failing the task.
- **A processor node's plain, unaddressed forward is no longer always a Kafka Streams broadcast.**
  Attaching a dead-letter sink — registered with `Serdes.ByteArray()` — as a second child of a stage's
  processor node means the zero-arg `context.forward(record)` Kafka Streams itself provides would also
  broadcast a business/control forward to it, throwing `ClassCastException` on the very next record.
  `ParsleyProcessorContext`'s one-arg `forward` and `ParsleyProcessor`'s watermark/epoch-marker forwards
  now address every declared business sink by name instead whenever a stage has one; with no dead-letter
  sink configured (every low-level `ParsleyProcessors` caller that hasn't opted in), the plain Kafka
  Streams broadcast is unchanged.
- **Breaking: concise, topology-level public API — `CausalStreamsBuilder` / `CausalTopology` /
  `CausalStreams`.** The public surface collapses to three roles mirroring Kafka Streams'
  `StreamsBuilder`/`Topology`/`KafkaStreams`. `CausalStreamsBuilder` declares one or more causal stages
  (`stream(topic[s][, keySerde, valueSerde])` — deferring to the runtime's default serdes when omitted —
  `.process(supplier)`, `.to(topic[, keySerde, valueSerde])`, `.withPartitioner`/`.withAudit`); combine
  streams declared with different serdes with `CausalStream#merge`. `.build()` produces a `CausalTopology`
  — a specification, not yet a real Kafka Streams `Topology`. The `CausalStreams` name is repurposed from
  today's topology-owning builder (removed) to the **runtime**: `new CausalStreams(topology, props)` /
  `.start()` / `.close()`, mirroring `new KafkaStreams(topology, props)`. Unlike the Kafka Streams DSL,
  sources/sinks take plain `Serde`s rather than `Consumed`/`Produced` — neither exposes its serdes for
  reading back, and Parsley's causal buffer needs the real `Serde` to round-trip a held record.

  `ParsleyQuiesce` and `ParsleyCoordination` are no longer public, user-constructed handles — `CausalStreams`
  owns one of each internally. Graceful causal drain is now unconditional and automatic: `close()` always
  waits for every task's buffer to drain, then (if `parsley.coordination.epoch-events-topic` is configured)
  permanently decommissions this instance's members before stopping the underlying `KafkaStreams` — so
  there is no restart/leave distinction for a caller to get wrong (a restart now always rejoins as a fresh
  member and waits to be re-admitted; slower, never unsafe). Evolve a running, coordinated topology through
  an epoch boundary with `CausalStreams#requestEpochTransition()`. `application.id` supplies the epoch
  member identity, as before.

  `ParsleyProcessors`, `ParsleyProcessorSupplier`, and `ParsleyBuffer` are demoted to package-private — they
  survive as `CausalStreamsBuilder`'s internal engine wiring. All prior `CausalStreams`/`ParsleyProcessors`
  capability carries over: multiple input topics with per-topic serdes, multiple named sinks, a uniform
  key-only sink partitioner, `CausalAudit`, and the startup co-partition + sink `cleanup.policy` validation
  (`parsley.topology.validation`).
- **Breaking: fail-closed causal delivery; buffer limits, eviction, and failure policies removed.** Causal
  delivery is now strictly fail-closed — there is no configuration that trades causal order for liveness.
  `CausalBufferLimit` (and `ofSize`/`ofDuration`/`first`/`unbounded`) is removed along with all buffer
  eviction: the causal buffer is unbounded and changelog-backed, so a record whose dependencies are not
  yet satisfied waits (spilling to disk) rather than being force-forwarded out of causal order.
  `addBufferStore(name, limit)` becomes `addBufferStore(name)`. The three `parsley.*.failure.policy`
  settings (`parsley.buffer.eviction.failure.policy`, `parsley.buffer.deserialization.failure.policy`,
  `parsley.clock.resolution.failure.policy`) and their `continue` mode are gone; the only remaining
  Parsley setting is `parsley.topology.validation`. An undecodable buffered record, or an undecodable
  dependencies header, now fails the task closed (the record is never dropped or forwarded on an unknown
  premise); an explicit dead-letter path — removing such a record from the causal execution path rather
  than delivering it as causally valid — will replace the fail-fast behaviour in a later change. The
  `CausalAudit` eviction events (`recordViolation`, `recordEvictionLimitExceeded`) and the
  eviction/violation metrics are removed.
- **Breaking (topology epochs): block-until-drained membership; timeout eviction removed.** An epoch
  transition now blocks until every running member has published its snapshot, for an unbounded time,
  instead of evicting a silent member after a timeout. Evicting an absent member and committing a floor
  without it could strand records it still held below that floor and release them before their causes (a
  causal-safety violation); block-until-drained never does. `ParsleyCoordination.create(...)` no longer
  takes an `evictionTimeout` — it takes a `ParsleyMembershipStrategy` (default
  `ParsleyMembershipStrategy.blockUntilDrained()`), a seam for future exclusion/recovery algorithms.
  Publication of a member's frontier is now driven off the folded log rather than a one-shot in-band
  marker, so a member that restarts mid-round re-publishes and cannot deadlock the round. There are **no
  timeouts** in the coordinator: the `joinTimeout` is also gone — a joining task now blocks unbounded until
  its epoch commits rather than failing after a deadline (`create(...)` no longer takes a `joinTimeout`,
  and `DEFAULT_JOIN_TIMEOUT` is removed). Blocking never proceeds on an unknown floor, so it cannot violate
  causal safety. Consequence: a crashed member blocks the next epoch transition — and any new join — until
  it returns; ongoing current-epoch processing is unaffected. Supersedes the earlier timeout-eviction +
  concurrent-redelivery behaviour.
- **Breaking: `ParsleyMembershipStrategy`/`ParsleyBlockedRound` are no longer public.** The seam for a future
  exclusion/recovery algorithm still exists internally, but with a single implementation
  (`blockUntilDrained()`) and no external caller ever supplying one, keeping it public only advertised an
  extension point nothing used. `CausalStreams`'s public constructor is now just `(topology, props)`; the
  3-arg overload taking an explicit `ParsleyMembershipStrategy` is removed.
- **`ParsleyCoordination.leave()` now drains before departing.** A graceful decommission quiesce-drains the
  node (blocks until its causal buffer empties through the ordinary delivery path), then appends the
  `Leave`, then requests a new epoch over the remaining members in which it is no longer a member — so a
  leave never strands un-drained buffered records ("only a drained node is excluded"). It returns without
  waiting for that epoch to commit, so a decommission is not coupled to the other members' liveness.
  Contract: stop feeding the node new input before decommissioning.
- **Breaking (semantics): strict completeness gate across all input channels.** A record is now
  delivered only once *every* input channel of the processor has confirmed *every* coordinate the
  record depends on. The delivery gate is a single check, `completeness().dominates(deps)`, where
  `completeness()` is the per-coordinate minimum across all input channels (each channel's advertised
  dependencies plus its own contiguous delivered position). The previous model scoped a dependency
  out when the processor did not consume that coordinate (treating it as vacuously satisfied); that
  scoping is removed, because it was unsound at a reconvergence point and let a lagging or recovering
  branch introduce an earlier-ordered record after the fact.

  This imposes a **topology contract**: every input branch of a node must observe (consume and
  watermark) every coordinate any branch's records depend on, or records depending on an unconfirmed
  coordinate are held indefinitely. In particular, a join of fully independent sources will hold a
  record depending on a coordinate an unrelated input never observes, and **a node must not consume
  both a topic and a topic derived from it** (the ancestor channel can never confirm the descendant).
  See `docs/internals/causal-consistency.md`.
- **Breaking (wire).** Forwarded records carry the producing node's completeness frontier in the
  `parsley-causal-dependencies` header. Nodes emit *protocol watermark* records — a null value, keyed
  with the triggering input record's key, marked with a `_parsley_watermark` header carrying that
  frontier — for every consumed message that produces no business output (a dropped/buffered record
  that advances completeness, or a delivered record the delegate did not forward), so completeness
  propagates contiguously through layers that produce no business output. The watermark reuses the
  triggering record's key so it routes to the same partition that record's output would, keeping
  completeness propagation correct across a sink boundary; it is identified only by the header, never
  by its key. A non-Parsley consumer of such a topic sees tombstone-shaped records and must skip them. The per-input-channel clocks that back `completeness()` are stored alongside the
  contiguous frontier clock in the single `"f"` value of the existing `{ns}-frontier` store, so no
  additional changelog topic is introduced.
- Documentation reframed to describe Parsley's guarantee as causal delivery order for Kafka
  Streams processors, given specific conditions (co-partitioned topics, closed processor effects).
  The previous framing ("causal consistency for Kafka") overstated the scope of the guarantee.
- The "holding" debug log line now identifies the held record by topic UUID rather than topic name,
  and logs the actual frontier value (restricted to the record's dependency coordinates) rather than
  a per-record shortfall. The frontier is monotonically non-decreasing for a given coordinate, so
  it can be tracked across log lines without confusion. `deps` and `frontier` use the same UUID
  format and coordinate set so they can be read in parallel. Example:
  `Holding UUID-0 @2 (buffer depth: 1, deps: ParsleyClock{UUID-0@8}, frontier: ParsleyClock{UUID-0@7})`

### Changed (internal)
- **The L3 gossip module is extracted as `ParsleyGossip` (T4.1, design §1b).** The null-message
  receive path (deliver the message's own offset, fold its carried clock stamp-side only) and the
  emission of this node's own null messages move out of `ParsleyCausalBroadcast`/`ParsleyProcessor`
  into one package-private module, so the I6 relay rule — relay a received null message onward iff
  its carried clock taught this node something outside its total knowledge — is stated exactly
  once. Behaviour is unchanged; the write-only `lastSeenKey` field (its reader died with the
  coordination subsystem) is removed.
- **The node tracks its own acknowledged output positions: the `ownOutputs` clock (T2.2, decision
  D2).** Every `CausalStreams` instance now injects a `ProducerInterceptor`
  (`ParsleyOwnOutputInterceptor`) into its stream producers through the public
  `producer.interceptor.classes` prefix — appending to, never replacing, any user-configured
  interceptors — plus a minted registry id under the same prefix, wiring producer acks for declared
  sink topics into a per-instance concurrent registry (`ParsleyOwnOutputRegistry`). Before every
  stamp, the single stamping site drains the registry into a new `ownOutputs` vector clock owned by
  `ParsleyChannels` (`acknowledge(topicId, partition, offset)`, max-fold, monotone), persisted as a
  new optional trailing section of the frontier `"f"` blob and seeded at init from each resolved
  sink's end offsets (`endOffset - 1`, the last appended position) — a deliberate over-claim that is
  conservative-sound (invariant I8) and heals the blob's one-transaction ack lag across a crash.
  Registry granularity follows T2.1's carry-forward: acked offsets are a global per-coordinate max
  (a sibling task's higher offset on a shared sink folds as an I8-sound over-claim, so no
  send-to-task ack routing is needed), while pending-send tracking is per producer — which under
  `exactly_once_v2` means per StreamThread, so the crossing wait resolves "this task's pending
  sends" from the current thread alone. The crossing-wait primitive
  (`awaitQuiescentExcept(topic, partition, timeout)`) enforces T3.0 A8's implementation invariant by
  construction: it returns normally only when no send to another own-sink coordinate is
  unacknowledged, and throws — failing the EOS transaction, never stamp-and-proceed — on timeout or
  on an acknowledgement failure observed while waiting. Destroyed coordinates (a recreated
  input-that-is-also-own-sink) are purged from the clock at rescope, per I9's one permitted removal.
  The outbound stamp is byte-identical to before — `completeness ∪ ownOutputs` and the crossing
  wait's stamping-site call land with T2.3, which also deletes the stamp-side own-sink strip. No
  public API or wire format changes.
- **The own-output acknowledgement mechanism is validated against a real broker (T2.1).** A new
  broker integration test, `ParsleyProducerAckMechanicsIT`, confirms the three mechanics the Phase 2
  own-output design depends on, ahead of building it: a `ProducerInterceptor` installed purely
  through the public `producer.interceptor.classes` config prefix reaches the exactly-once stream
  producer and reports the exact committed `(topic, partition, offset)` of each sink send, on the
  producer's network thread rather than the stream thread; `KafkaProducer.flush()` — the call a
  Streams commit makes — returns only after every prior send's callback has completed, so a stamp
  taken after a flush cannot miss an own-output coordinate; and aborting a transaction with sends
  outstanding fails each of them with exactly one callback, so a wait fed by those callbacks is
  always released and can fail the transaction instead of stamping with an unverified position.
  Test-only: no main sources, public API, or wire formats change.
- **The L2 causal-broadcast module is named: `ParsleyEngine` becomes `ParsleyCausalBroadcast`.** Third
  structural step of the three-protocol redesign (T1.2b, decisions D5 and O4). The class is the
  receive/deliver core of Birman–Schiper–Stephenson causal broadcast, so it now carries that name,
  presented in the Cachin–Guerraoui–Rodrigues module style with a new `broadcast(record)` request —
  the timestamp-assignment half of BSS `broadcast(m)`, attaching the completeness stamp read live at
  stamp time. `broadcast()` is the **single stamping site**: the delegate-facing stamping proxy
  (`ParsleyProcessorContext`) and the protocol-marker path (`forwardMarker`) both route through it,
  collapsing the two previously independent stamping sites (snapshot-field vs live-completeness,
  which coincided only because a delegate's forwards never mutate completeness) into one structural
  equivalence; the `stampCompleteness` snapshot field is deleted. Stamp content is unchanged until
  Phase 2 adds `ownOutputs`. Behaviour-identical; no public API or wire format changes.
- **Dependency-clock normalisation is a single L1 step: `ParsleyChannels.normalize`.** Second
  structural step of the three-protocol redesign (T1.2, decision D3). The self-cycle removal and the
  below-floor strip relocate from the engine's per-gate preprocessing (`effectiveDependencies`) into
  one `normalize(rawDeps, sourceCoordinate)` request on the channels module, so no gate code path
  consults epoch state directly (invariant I5: after normalisation, no clock inside L2 carries a
  self-reference). The below-floor clause is interim — it is deleted with the epoch floors in T3.2.
  The gate-side own-sink strip stays in the engine until T3.1's two-branch gate replaces it.
  Behaviour-identical; no public API or wire format changes.
- **The L1 channels module is named: `ParsleyChannels`.** First structural step of the three-protocol
  redesign (T1.1). `ParsleyFrontier` folds into a new `ParsleyChannels` class — the
  Kafka-to-reliable-FIFO-channel adaptation, presented in the Cachin–Guerraoui–Rodrigues module style
  with the classical operation names: `receive` (density seeding and commit-marker bridging),
  `delivered` (contiguous frontier advance), `frontier()`, and the Phase 2 stubs `acknowledge`/
  `ownOutputs()` for own-output tracking. `ParsleyClock` is renamed `ParsleyVectorClock` (it is a
  vector clock — Fidge 1988, Mattern 1988 — indexed by channel rather than process, a stated
  variant). Behaviour-identical; all classes are package-private, so no public API or wire format
  changes.

### Removed
- **The JMH benchmark suite is deleted; `docs/performance.md` now asserts complexity from the
  code instead of claiming empirical confirmation.** The suite (`HeaderEvaluationBenchmark`,
  `BufferReleaseBenchmark`, `StateRestorationBenchmark`, their infra, the `benchmarks` Maven
  profile, and `docs/internals/benchmarks.md`) had drifted after the three-protocol redesign into
  measuring paths production no longer runs: frontier restore exercised a bare-clock `"f"` format
  that the compound `ParsleyChannels` blob replaced, and the drain benchmarks ran channel state on
  an in-memory double, skipping the per-advance blob persist. No CI leg ran it, so it verified
  nothing while lending the docs false empirical authority. Capabilities dropped: the
  run-it-on-your-hardware capacity-planning workflow (replaced by the advice to measure your own
  topology end to end and watch the metrics), and empirical detection of complexity regressions
  in the drain path (accepted risk; it was never exercised in CI). The rewritten page also
  corrects the cost model for the redesign: the dominant per-record term is the O(C · w) channel
  state persist on every advance, outbound clock width grows with the node's causal history
  rather than being bounded by the task's source partitions, and gossip null-message volume is
  documented as its own category.
- **BREAKING: the `parsley.topology.validation` key is deleted; Parsley now has zero
  configuration keys.** The startup topology checks always run and always fail fast — there is no
  `warn`/`off` opt-down, and no `parsley.properties` key is read at all. Startup fails with
  `IllegalStateException`, naming every offending key, if any `parsley.*` key is present in the
  Streams `Properties` or a `parsley.properties` classpath resource (the same fail-loud treatment
  the removed `parsley.coordination.*` keys already had). The checks themselves became precise
  enough to need no opt-down: source topics must share a partition count (a causal-safety
  requirement, previously softenable by `warn`); a sink must be at least as wide as the widest
  source — the former source/sink parity check wrongly flagged the funnel shape (narrow sources
  fanning into a wider, re-keyed sink), which was the one legitimate use of `warn` and now passes
  with no opt-down, while a narrower sink (which can only crash-loop the marker produce at
  runtime) always fails at init; compacted sinks always fail, like compacted sources already did.
  Capabilities dropped with the key: nothing viable — every topology `warn`/`off` admitted either
  cannot run at all or violates causal safety — plus `off`'s saving of the sink-describe admin
  round-trips at init (a one-time startup cost).
- **BREAKING: the topology-epoch coordination subsystem is deleted (decisions D4 + D7).** Causal
  safety never depended on it: the two-branch delivery gate (consumed dependencies gate on the
  local frontier; all others are soundly ignored under invariants I2 + I9) is the whole safety
  story, and joins need zero coordination — a fresh application starts consuming and its replay
  self-gates into causal delivery order. Removed outright: the epoch-events log and its
  deterministic fold, epoch floors and transition windows, snapshot/boundary markers and their
  header kinds, the genesis cohort barrier, the member-app roster, the join barrier, leave-drain,
  the commit-time completeness snapshot store, the `mergeMin` floor fold, and the domain-topics
  passthrough sources (fourteen main classes, among them `ParsleyCoordination`,
  `ParsleyEpochRuntime`, `ParsleyEpochLog`, `KafkaEpochTransport`, and
  `ParsleyQuiesceTracker` — shutdown quiesce via `CausalStreams.close()` is
  membership-independent and survives unchanged). `CausalStreams.requestEpochTransition()` no
  longer exists. The `parsley.coordination.*` configuration keys are deleted, not renamed:
  **startup fails loudly** naming the offending key when one is present. An existing coordinated
  deployment upgrades by deleting its coordination configuration; behaviour becomes strictly more
  available (no join barrier, no genesis wait). Two capabilities are knowingly dropped with the
  subsystem, as misconfiguration detection rather than safety: the split-domain loud failure
  (cross-deployment coupling between compliant apps is causally sound) and fail-fast on unknown
  coordinates (replaced by the `deps-out-of-scope-ignored` metric and startup topology
  validation). The `docs/internals/topology-epochs.md` page is deleted with the machinery it
  described.

### Fixed
- **The three gauge metrics now carry the same tags as the rate/total sensors.** The gauges
  (`buffer-depth`, `buffer-oldest-buffered-at-ms`, `records-held-above-highest-received`) were
  tagged `parsley-id=<application.id>-<taskId>` with no thread tag, while the rate/total sensors
  in the same `stream-parsley-metrics` group were tagged with the bare task ID plus `thread-id` —
  so a dashboard grouping by `parsley-id` saw two id formats for one task. All metrics in the
  group now share one scheme: `parsley-id` = the task ID, `thread-id` = the registering stream
  thread. The tag scheme is documented in the configuration page's metrics section and pinned by
  `ParsleyMetricsTest`.
- **`records-released-total` now counts released records, not drain passes.** The sensor's total
  stat counts observations, and `recordReleased` recorded once per drain pass with the release
  count as the (ignored-for-counting) value — so a pass releasing five records advanced the total
  by one, under-reporting releases and making the total incomparable with `records-buffered-total`.
  It now records once per released record, matching the documented meaning. Found by the new
  `ParsleyMetricsTest`, which pins the full metrics contract (names, group, tags, gauge
  descriptions, and per-method counting semantics) against `docs/configuration.md#metrics`.
- **Null-message gossip now quiesces on every topic cycle: the I6 relay trigger is restricted to
  consumed channels.** The explorer's first sweep found (and `ParsleyGossipCycleQuiescenceTest`
  pinned) a liveness defect: on a cycle of three or more nodes, a member that neither produces
  nor consumes some cycle channel knew it only through carried-clock custody (I9), one gossip lap
  stale, so the relay-on-any-new-knowledge rule fired every lap and each relay appended the
  coordinate that sustained the next — an idle deployment generated null-message traffic forever
  at one message per loop latency per channel (delivery-order safety was unaffected). The relay
  rule now follows the Chandy–Misra–Bryant trigger discipline the null messages come from: a
  received null message obliges a relay only when its carried clock advances this node's total
  knowledge *on a channel it consumes at its own task partition*
  (`Reception.advancedConsumedChannel`, formerly `learnedSomethingNew`) — coordinates whose
  first-hand coverage (the contiguous frontier) physically catches up to every appended offset,
  so a fact there can oblige at most one relay before it is covered for good. Custody — a claim
  on a channel the node neither consumes nor produces, a sibling's appends on a shared sink, a
  foreign partition of a consumed topic — still folds into the stamp unconditionally (I9 is
  untouched) and rides every later emission, but never itself obliges a relay. Withholding the
  relay starves nothing: the delivery gate waits only on the local frontier of consumed
  channels, and every stamped claim sits at or below a really-appended offset that arrives
  regardless of gossip (I8). Net traffic strictly decreases everywhere (a two-node cycle now
  settles in one advertisement, with no echo). The quiescence guarantee and its one
  fair-scheduling qualification on chorded cycles are documented in the gossip internals page;
  the previously storming three-node cycle, the chorded cycle, and an all-shared-sinks cycle
  (which disqualified the looser consumed-or-produced trigger) are pinned as quiescence
  regressions, and the random-topology generator produces multi-node cycles and shared sinks
  onto already-consumed topics again, with a population vacuity guard proving the multi-node
  shapes appear in every sweep.
- **A dropped or repurposed sink no longer under-claims the node's own final-transaction outputs
  (T3.4, invariant I2).** The persisted frontier blob always trails the final transaction's
  own-output acknowledgements (state stores flush before the producer flush completes acks), and
  the initialisation-time end-offset seed healed only the *currently* declared sinks — so a
  redeploy that turned a sink into an input, or dropped it while a third party still consumed it,
  restarted with stamps missing the node's own last outputs there, and a downstream consumer of
  that topic plus another of the node's sinks could deliver a derived effect before its cause.
  The blob now records the declared sink set, and initialisation heals every *previous* sink that
  is no longer one: end-offset acknowledgement when the topic survives under its recorded UUID,
  purge when it is provably destroyed (deleted, or recreated under a new UUID), and a loud
  initialisation failure when it cannot be resolved at all — never a silent under-claim.
- **The outbound stamp now claims records delivered out of order above a contiguous-frontier gap:
  the `highestDelivered` clock (T2.4, invariant I2).** Delivery within a partition is deliberately
  not head-of-line-blocking, so a later record can be delivered to the delegate while an earlier
  one from a different producer is still held — but the stamp previously carried only the
  contiguous frontier (stuck below the gap), the delivered record's *dependency clock* (via the
  channel fold), and the node's own *outputs*. The delivered record's own coordinate appeared
  nowhere, so an output derived from it failed to claim its true cause, and a downstream consumer
  of both topics could deliver the effect before the cause — the input-side sibling of the
  own-output gap #22 closed. `ParsleyChannels` now keeps `highestDelivered`, the max projection of
  the delivered vector that non-head-of-line delivery splits off from the contiguous frontier:
  observed on every delivery, folded into `stamp()` only (never the delivery gate, and never
  `completeness()`, which the interim epoch-floor publication still reads), and deliberately not
  persisted — its above-frontier content is exactly the forwarded index's marks, which commit in
  the same EOS transaction as the frontier blob, so a restart reconstructs it losslessly. A scope
  shrink re-homes it into the carried-ancestry clock like any delivered causal past (A6); a
  recreated topic's old UUID leaves it outright (E1). The implied claim on the held gap offsets
  below an above-gap entry is an offset-prefix over-claim of real appended positions —
  delay-only, sound by invariant I8. Invariant I2 is restated to match what the D1/D7 proof
  always needed: the stamp dominates the dependency clocks *and the coordinates* of every
  delivered event.
- **T2.4 property harness: I2/I3/I9 certified under randomised interleavings.** A new in-memory
  multi-node simulator (`ParsleyTopologySim`) drives real `ParsleyChannels` +
  `ParsleyCausalBroadcast` instances over store-backed persistence through the production entry
  points — business receive, null-message receive, the single stamping site with the crossing-wait
  and ack-fold seams, in-place restarts rebuilt from the durable stores, and `rescope` — under
  seeded-random schedules, while tracking exact delegate-visible causal histories
  (Schwarz–Mattern) as ground truth. Continuously asserted: I2 in both forms (stamps dominate
  delivered dependency clocks + coordinates, and the ground-truth causal past), I3 (successive
  stamps vector-monotone, across restart boundaries too), I9 (stamps carry unconsumed-channel
  ancestry — the transitive chain claims the origin coordinate at every hop, and a node fed only
  null messages still stamps ancestry it never consumed), ground-truth causal delivery order (no
  effect before its consumed cause at any delegate), own-output stamp coverage (D2), and liveness
  (every seed drains to empty hold-back queues). The scope-change properties (T3.0 A5/A6) run the
  same sweeps across scope-shrinking and scope-growing restarts, including growth onto a former
  own sink ("skip what you already claimed"). Reverting the `highestDelivered` fix kills all five
  invariant properties — the harness is the certification the T3.1 gate switch (D1) leans on.
  Interim, until T3.1: business topologies keep every consumer's scope covering claimable upstream
  coordinates (the sim states this once, in `assertInterimDepCover`); differing-scope business
  chains join the sweep when the two-branch gate lands.
- **The outbound stamp is now `completeness ∪ ownOutputs`, and the stamp-side own-sink strip is
  gone (#22; T2.3, decision D2).** Every stamped record — business forwards and null messages alike,
  through the single stamping site — now carries the node's own acknowledged output positions, and
  an inbound clock's own-sink coordinates fold into the advertised channel clocks unstripped
  (invariant I9: the gate may ignore, the merge may not). This closes the two own-sink stamp holes:
  a stage whose sink topic is also consumed by a distinct downstream app no longer erases that real
  ancestor from its other outputs' stamps (#22 — the third party could deliver effect before
  cause), and a producer's second output is now provably after its first (the two-output diamond).
  Before stamping, the task runs the **crossing wait** (O1; per-(topic, partition) granularity per
  T3.0 A7): it blocks until no own-sink send to another coordinate — a different topic **or a
  different partition of the same topic** — is unacknowledged, so a send that process-order-precedes
  the record cannot be missing from its stamp; on timeout (bounded by the producer's
  `delivery.timeout.ms`) or on an observed ack failure it throws and the EOS transaction dies —
  never stamp-and-proceed (A8). A business forward's wait conservatively excludes nothing (its
  destination partition is unknowable at stamp time; over-waiting only folds more acked positions —
  monotone-sound by I8); a marker's wait excludes its exact destination set. Null-message relay is
  now knowledge-based (invariant I6): a carried clock is relayed onward only when it teaches the
  node something outside `frontier ∪ channel clocks ∪ carried ancestry ∪ ownOutputs`, replacing the
  per-channel comparison and the strip's old cycle-settling role (a reflected own coordinate is
  dominated by `ownOutputs`, so cycles still quiesce). A scope-growth rescope now seeds an added
  input from the ownOutputs-inclusive stamp value, so an added input that was formerly this node's
  own sink skips the prefix its stamps already claimed ("skip what you already claimed", extending
  T3.0 A5). New observability: `reflected-claims-above-own-outputs` (I8 diagnostic — an own-sink
  claim above the own-output view; never a failure) and the `records-held-above-highest-received`
  gauge with a WARN log (T3.0 A9 — records held past `delivery.timeout.ms` on a dependency above
  the channel's highest physically received offset, the signature of a claim nothing received can
  satisfy; fail-safe, unbounded delay made visible). Wiring this surfaced an ack-ordering race in
  the T2.2 interceptor, found live by `CausalFanOutScopedFrontierIT`: the callback cleared the
  pending count (waking a crossing-wait waiter) before folding the acked offset into the registry,
  so the released stamp could miss exactly the coordinate whose ack released it; the interceptor
  now folds before it acknowledges, and its Javadoc records the order as load-bearing. The interim gate-side own-sink strip stays
  until T3.1's two-branch gate, which also picks up the A7 funnel's downstream-delivery IT — the
  cross-partition claims this change (correctly) puts on the wire are exactly what the interim
  fail-fast gate rejects at a task owning a different partition (the same recorded
  interim-consequence class as T1.3's re-homed stamps). Wire note: stamps gain entries (own-sink
  coordinates); no header or blob format changes.
- **Scope-change safety: a redeploy that changes the input-topic set no longer mishandles surviving
  causal state (#21; T3.0 attacks A5/A6).** The persisted frontier blob now records the declared
  input set (topic name → UUID) and a **carried-ancestry clock**, both as trailing sections, so a
  restart is distinguished from a scope change by "input set unchanged since the blob", not by blob
  presence alone. On a shrink, a removed input's delivered ancestry — its frontier entry and its
  channel's advertised clock — re-homes into the carried-ancestry clock that every outbound stamp
  keeps merging: stamps still dominate the retired channel's history, where the old scope prune
  silently dropped it (an under-claim that could reorder a third party downstream). On a growth, an
  added input with surviving state seeds its frontier at the node's carried-ancestry value — never
  log-start — so the prefix at or below what this node already delivered or carried is skipped, not
  replayed as live into the surviving state (a full reset is the opt-in for processing that
  history); a recreated input (same name, new UUID) has its old, undeliverable coordinates removed
  outright. The receive path gains the matching skip guard: an already-delivered offset (at or below
  the contiguous frontier, or still marked in the forwarded index) is skipped with a new
  `replays-skipped` metric instead of being forwarded to the delegate again. The pre-start offset
  seeder now permits the added-input redeploy — a topic this group has never committed on, while
  another source topic is committed, seeds to log-start (the skip guard makes the replay safe);
  every other surviving-state refusal is unchanged. (The interim consequence this entry originally
  noted — a downstream app failing fast on re-homed coordinates it does not consume — is resolved
  in this same release by the two-branch gate; see Changed.)
- **A mis-meshed topology-epoch member could be promoted, then wedge every future epoch round for the
  whole domain.** A member's full-mesh self-coverage (its own declared inputs and sinks covering the
  coordinated domain) was validated only *after* it had already declared itself and been promoted to a
  running member. Because the round's mesh check excludes pending joiners, a member that could not cover
  the domain was admitted anyway, then crash-looped in `init` without ever publishing — leaving the domain
  permanently unable to complete a round (the mesh check now failed on the running member, and it stayed
  forever in the unpublished set). Coverage is now validated *before* the member declares itself: a
  mis-configured member fails fast in isolation and never appends a join, so it can never be promoted into
  a domain-wedging running member. The join wait and the bootstrap wait now share one deadline so their sum
  stays within the single join budget rather than risking a silent mid-block consumer eviction, and the
  runtime logs a warning when a round stalls on running members that have not published.
- **The contiguous frontier now tracks a transactionally-produced input across EOS commit markers.** Under
  the required `exactly_once_v2`, a `read_committed` consumer never returns the offsets occupied by
  transaction commit/abort markers or aborted-transaction records, so a causal stage sees permanent holes
  in a transactional input's offsets. The contiguous frontier walk required strictly consecutive offsets,
  so it stalled at the first marker hole: every completeness stamp under-reported that input, and any
  record depending on a post-marker offset deadlocked. The engine now bridges a consumer-skipped offset —
  sound because Kafka delivers a partition strictly in offset order, so a gap below a just-received offset
  is permanently a marker or aborted record, never a business record still in flight and never a held one —
  folding it into the frontier so the walk crosses it. The per-channel highest-received offset is persisted
  in the frontier store, so the skip detection is exact across a restart.
- **An unadmittable topology-epoch join hung `init()` on the StreamThread until the broker evicted it
  into a rebalance crash-loop.** The joiner handshake must wait for the epoch that establishes its
  consistent cut (the agreed new logical time-0) to commit before it consumes — consuming ahead is
  unsound, since the joiner would race past the not-yet-known floor and act on pre-cut history. That wait
  runs on the Kafka Streams `StreamThread` during `init()`, and was unbounded: if the domain could not
  commit (an existing member down or partitioned), it silently outlived `max.poll.interval.ms`, the
  broker evicted the consumer, and `init()` reran — a silent crash-loop. The wait is now bounded to 90%
  of the effective `max.poll.interval.ms` and fails with a precise `ParsleyJoinTimeoutException` before
  the broker would evict, telling the operator to raise `max.poll.interval.ms` or investigate why the
  domain cannot commit. The stale `awaitJoinCommit` Javadoc (which sold a floor-drag rationale) now
  describes the actual consistent-cut / historical-replay reason.
- **The epoch-runtime background thread died permanently on any transport exception, losing outbox
  events.** The drive loop was `while (running) runOnce();` with no guard, so a single transport append
  or poll throwing on a transient broker blip killed the thread: `bootstrapped` stayed false (every join
  blocked forever) and no round ever committed — a silent, permanent wedge. Worse, the outbox drain
  removed an event *before* appending it, so an event whose append threw was lost outright. The loop body
  now catches, logs, backs off, and retries; and the drain is peek-then-remove, dropping an event only
  after its append succeeds, so a transient failure loses nothing enqueued.
- **`CausalStreams#close()` hung forever on an instance that owned zero tasks.** The graceful-shutdown
  drain wait spun until `ParsleyQuiesce#isSafeToClose` reported ready, but that required at least one
  registered task, and an instance with more instances than partitions (or one whose tasks all migrated
  away, each unregistering on its own close) sits `RUNNING` with none — so the dead-instance escape never
  fired and the wait never ended. `isSafeToClose` now treats an empty registration set as trivially safe:
  an instance holding no task can strand nothing. The epoch-leave drain wait that reuses the same tracker
  is unaffected — it only evaluates readiness after a local member has joined (so the set is non-empty
  there).
- **An unreachable-dependency failure mutated engine state before throwing.** `ParsleyEngine.receive`
  seeded the frontier (`seedIfFirstSeen`, which persists) and ran `propagate` — advancing the frontier
  and potentially releasing and un-buffering records into a result list — before the fail-closed
  unreachable-dependency check threw, discarding that result list. Persisted state stayed consistent
  only under the EOS batch rollback; an in-memory engine (the test doubles) had no rollback and diverged.
  The check now runs first, before any mutation. It reads only the dependency clock and the settled epoch
  floor — neither affected by the seed — so this is purely a change to the failure's timing.
- **`close()` after an interrupted `init()` NPE'd and masked the real failure.** The unbounded
  topology-epoch join wait in `init()` is interrupted on a clean shutdown mid-join, which unwinds it with
  an `IllegalStateException` — but Kafka Streams still calls `close()` on a task whose `init()` threw. The
  old `close()` unconditionally dereferenced the still-null `wiredMetrics` (NPE) and called `close()` on a
  delegate it had never initialised, burying the genuine interrupt cause. `close()` now tears down only
  what `init()` actually set up: it closes the delegate only once `delegate.init()` has returned and the
  metrics only once they are wired. (The quiesce and epoch-runtime cleanup were already safe on a partial
  init.)
- **A marker with a corrupt or absent completeness header permanently gapped the channel frontier.**
  The epoch snapshot and boundary handlers folded a received marker through a path that, on a missing or
  undecodable `parsley-causal-dependencies` header, returned early *without delivering the marker's own
  offset* into the channel's contiguous frontier. A marker occupies a real offset on its partition, so
  the gap-free absorb walk stalled below it forever — stranding every later record on that channel that
  waited on anything. `handleWatermark` already handled the identical failure correctly (empty clock,
  offset still delivered); the two paths have been unified into one `foldMarkerCompleteness` helper that
  always absorbs the marker's offset and treats a decode failure as an empty carried clock. The decode
  failure is contained to the decode; the offset delivery and everything after it stay fail-closed.
- **An idle-round epoch transition stalled forever at the second layer.** A relaying stage forwarded a
  received epoch-boundary marker downstream only when the marker's *carried completeness clock* advanced
  the channel — the same "clock-invisible markers" gate that (correctly) governs watermark and snapshot
  relay. But a boundary re-carries the completeness the preceding snapshot already advertised, so on a
  quiesced round with no traffic the boundary teaches the channel nothing new and was silently not
  relayed. The downstream then never saw the marker on that channel, its marker-on-every-channel
  transition window never closed, and the epoch floor never advanced — precisely in the idle,
  maintenance-window case topology epochs exist for, and unrecoverable (non-source tasks have no
  log-driven boundary fallback). A boundary now relays on its channel's *first sight* of it regardless
  of the carried clock (`ParsleyEpochState#onBoundary` reports whether the `(epoch, channel)` marker was
  newly recorded, surfaced through `ParsleyEngine.BoundaryOutcome#markerWasNew`), while a duplicate on an
  already-seen channel still records nothing new and does not relay, so a cyclic topology cannot
  ping-pong it. A boundary is boundary news, not merely clock news.
- **`CausalStreams#close()` could hang forever on a dead coordinated instance.** The quiesce drain
  wait already ended when the streams instance left `RUNNING`/`REBALANCING`, but the coordination
  decommission's phase-1 drain wait had no such escape: an instance that died in `ERROR` with a
  non-empty buffer spun in `ParsleyCoordination#leave` unbounded, waiting on a buffer no task would
  ever empty. `leave` now takes the same liveness probe `close()` already applies to the quiesce
  wait, and *abandons* the decommission when the instance can no longer drain — the members stay in
  the domain exactly as a crash would leave them (never evicted with an undrained buffer; "only a
  drained node is excluded"), and a later restart resumes them as running members under the
  unchanged floor.
- **The epoch-events log's history requirements are now validated at startup.** Every instance's
  fold replays the log from the beginning, so a `cleanup.policy` including `compact` or a finite
  `retention.ms` silently amputates the join/commit history — after which a restarted instance sees
  `committedEpochId == 0` and treats an established domain as a cold start. This was documented in
  `configuration.md` but never enforced, unlike the equally fatal partition-count misconfiguration.
  `KafkaEpochTransport` now describes the topic's config before building any client and fails fast
  unless the policy is non-compacting and retention is infinite, mirroring the single-partition
  check; the coordination integration tests create their epoch-events topics with
  `retention.ms=-1` accordingly.
- **A user-delegate exception during a marker-triggered release was swallowed, permanently losing the
  released records.** The epoch marker channel-clock path wrapped not just the carried clock's decode
  but the whole `onWatermark` + delivery in one `catch (Exception)` that logged *"Failed to decode
  marker completeness … ignoring"* and carried on. By the time the delegate runs, the released
  records have already left the buffer and advanced the frontier — so swallowing the failure let the
  task commit past records the delegate never processed (and never redelivers), and even absorbed the
  engine's own deliberate fail-fast exceptions (poison records, unreachable dependencies) on that
  path. The catch now covers exactly the decode, mirroring `handleWatermark`; everything after it
  fails the task, fail-closed.
- **Protocol markers were produced with timestamp 0.** A watermark/epoch marker's timestamp carries
  no causal meaning, but it does drive broker time-based retention: a sink segment holding only
  0-timestamped markers (exactly a marker-only passthrough channel) looked expired the moment it
  rolled and could be deleted before a slow consumer read it — the same completeness-loss failure the
  compaction lint exists to prevent. Markers are now stamped with the forwarding task's current wall
  clock (`context.currentSystemTimeMs()`).
- **A restart could re-trigger the baseline frontier seed past a still-held record, releasing its
  dependents before it — an effect-before-cause delivery.** `ParsleyFrontier#seedIfFirstSeen` folds
  everything below a coordinate's first-ever-observed offset into the frontier, guarded by an
  in-memory "seen" set so a later record cannot re-seed past a held earlier one — but that set did
  not survive a restart. With a held record at offset 0 (whose seed is a no-op, so the persisted
  frontier carries no entry for the coordinate), the first post-restart record on that coordinate
  re-fired the seed, folded the held record's offset into the frontier as "outside the engine's
  purview", and the resulting cascade released records depending on it before it had ever been
  delivered. `ParsleyEngine`'s constructor now replays the first-sighting seed for every restored
  held record's source coordinate (at its lowest held offset) before anything else can, so the guard
  is reconstructed from the buffer exactly as the original sightings built it.
- **Channel clocks never forgot a retired coordinate, so removing a topic from the DAG permanently
  poisoned every downstream stamp.** A channel's advertised clock is monotonic (`channelUpdate` only
  ever max-merges) and feeds `completeness()` — the outbound stamp — so a transitive entry for a
  topic that left the topology (retired, or recreated under a new UUID) was re-advertised downstream
  forever, where every receiver's fail-closed gate rejected it as an unreachable dependency: a
  permanent crash loop regenerated from the persisted store on every restart, defeating the
  topology-epochs pillar's "evolve without dragging pre-change history" claim for any evolution that
  retires a coordinate. `ParsleyFrontier#pruneToScope` now prunes each surviving channel's clock
  *values* to the scope, not just the frontier entries and channel keys — sound because under the
  ancestry/full-mesh contract every live advertised coordinate is a consumed topic on the task's own
  partition, so an out-of-scope entry inside a channel value can only be retirement garbage. A
  rolling restart after the topology change (already required to change subscriptions) now actually
  cleans the stamps.
- **`RocksForwardedIndex#pruneAtOrBelow` deleted one offset above its contract.** The store's
  `range(from, to)` is inclusive of both bounds, so an upper key of `watermark + 1` also deleted a
  legitimately marked offset at `watermark + 1` — an entry above the contiguous frontier and the
  absorb walk's very next candidate. Reachable only through a store carried over from before the
  `exactly_once_v2` requirement and self-healing on replay, but now the upper bound is the watermark
  itself, matching the method's name and Javadoc.
- **Epoch-floor publications could reflect uncommitted state, misclassifying in-epoch records as
  pre-epoch history for a fresh joiner.** A member's `FrontierPublished` rides the idempotent
  epoch-events side channel, deliberately outside the task's EOS transaction — but the published clock
  was read from the live in-memory completeness, which includes deliveries whose changelog writes and
  forwards sit in the current, uncommitted transaction. A crash before commit rolled those deliveries
  back while the publication survived, so a committed floor could exceed the member's durable
  progress. An established member is protected by the window-close rule (the transition settles only
  once the local frontier dominates the floor), but a fresh joiner settles at the floor *directly*:
  records the crashed member re-forwards after replay carry below-floor dependencies the joiner
  strips as "pre-epoch history", releasing them without ordering against causes it also treats as
  pre-epoch — the ordering hole `ParsleyEpochSnapshotPublisher`'s Javadoc documented as "still open".

  Closed via a new `ParsleyCommittedCompleteness` commit hook: a non-persistent, non-logged
  `StateStore` registered per stage solely so Kafka Streams invokes its `flush()` in every task commit
  cycle (the Processor API's only commit signal). Two slots make it crash-safe — a snapshot taken at
  flush N becomes publishable only at flush N+1, the proof that transaction N committed, so a
  flush-then-abort discards the optimistic value with the task instance. Every side-channel
  publication (`handleEpochSnapshot`, the log-driven owed-publication path, `injectSnapshot`, and the
  runtime's stalled-member auto-publish) now reads `committed()`; in-band stamps (forwards and
  watermark/marker headers) deliberately stay live, since they ride the task's own transaction and
  abort with it. The floor becomes at most one commit interval more conservative — always safe, the
  merge-min direction. Both slots seed from the init-restored completeness (rebuilt from the committed
  changelog, durable by definition).
- **A built `CausalTopology` was silently mutable through handles the user still held.**
  `CausalStreamsBuilder#build()` snapshotted the stage *list* but not the stages: calling
  `.to(...)`/`.withPartitioner(...)` on a retained `CausalProcessedStream` after `build()` — even
  after `new CausalStreams(topology, props)` — mutated the supposedly immutable topology in place.
  Stages are now frozen at `build()`; a late mutation throws `IllegalStateException`. Also hardened:
  `build()` rejects a builder with no declared stage (the topology would silently run nothing), and
  `CausalStream#merge` rejects two streams declaring the same topic (whose serdes previously collided
  silently, last-write-wins).
- **Marker handlers fabricated a source coordinate when record metadata was absent.**
  `handleWatermark`/`handleEpochBoundary`/`advanceChannelClockFromMarker`/`ingest` defaulted a missing
  `recordMetadata()` to partition 0 / offset 0, which would write causal state for a coordinate
  nothing actually consumed. Unreachable on today's call paths (all run inside `process()`, where
  Streams supplies the metadata), so it is now an invariant: absence throws `IllegalStateException`
  instead of silently corrupting partition 0's state if a future refactor ever breaks the assumption.
- **`CausalStreams#close()` hung forever when the underlying streams instance was already dead.** The
  graceful drain polled `ParsleyQuiesce#isSafeToClose()` unbounded, but that requires at least one
  registered, currently-drained task — an instance in `ERROR` (every task closed and unregistered), or
  one that never got tasks assigned, can never satisfy it, so `close()` never returned. The wait now
  ends as soon as the streams state leaves `RUNNING`/`REBALANCING`: no task can drain any further, and
  every held record is changelog-backed and survives to the next start, so closing a dead instance
  loses nothing (the drain is a stall-avoidance optimisation, not a correctness requirement — per
  `ParsleyQuiesce`'s own contract). The wait loop is extracted as the package-private
  `CausalStreams#awaitDrain` seam, with tests.
- **Every engine operation rebuilt the entire causal state — a full buffer scan, a candidate-index
  rewrite, and a frontier-blob re-persist — making each processed record O(buffer-depth).**
  `ParsleyProcessor#engine()` constructed a fresh `ParsleyEngine`/`ParsleyFrontier`/`RocksBufferStore`
  at the top of every operation (a `process()` call touched it four or more times; `deliver()` once
  per released message; the 200ms epoch poll and 5s metrics tick each again while idle). Each
  construction re-scanned every buffer key (`RocksBufferStore`'s sequence/size seeding), re-decoded
  every held record's dependency clock and re-put its candidate-index entries (each an EOS changelog
  write), re-loaded the frontier blob, and re-persisted it via the idempotent prune/seed pass — so a
  deep dependency stall, the exact scenario the buffer exists for, degraded quadratically:
  a 10k-deep buffer cost tens of thousands of redundant store writes per incoming record, dwarfing
  the O(log n + k + r) costs `performance.md` documents (its benchmarks drive `ParsleyEngine`
  directly and never saw this).

  The per-operation rebuild was a prerequisite for a separate passthrough processor node sharing the
  stores — a design that was never built: passthrough topics are wired as extra sources into the
  *same* processor node, so exactly one `Processor` instance ever touches a task's causal stores and
  a cached `ParsleyFrontier` cannot diverge from a concurrent writer. The engine is now built once at
  `init()` (`buildEngine()`) and cached for the processor's lifetime, restoring the architecture the
  performance documentation describes; `ParsleyFrontier`'s restore-time forwarded-index sweep is
  genuinely one-shot-at-load again.
- **An epoch-events append could land on a partition no fold ever reads, silently losing the event.**
  `KafkaEpochTransport#append` sent with a null key and no explicit partition, leaving placement to the
  producer's partitioner, while every reader is manually assigned to partition 0 only. On an
  epoch-events topic created with more than one partition (e.g. a broker default partition count), a
  `JoinRequested` scattered onto partition 3 was invisible to every instance — the joiner blocked in
  `awaitJoinCommit` forever — and a lost `FrontierPublished` blocked its round forever, all with no
  error anywhere. Appends are now pinned to partition 0 explicitly, and transport construction fails
  fast (`IllegalStateException`) unless the topic has exactly one partition — a startup
  misconfiguration surfaced once, not a protocol that silently degrades. `KafkaEpochTransportTest`
  gains the first tests constructing the transport itself (over Kafka's own
  `MockProducer`/`MockConsumer`); `configuration.md` now also documents the topic's full-history
  retention requirement (`cleanup.policy=delete`, `retention.ms=-1`), since every instance replays the
  log from the beginning.
- **A peer's advertised claim could satisfy the delivery gate, releasing a record before this node had
  itself delivered its cause.** The gate checked `completeness()` — the max-merge of this node's own
  contiguous frontier and every input channel's advertised clock — so a watermark or epoch marker
  arriving on one channel and carrying a claim about a *sibling* channel's coordinate (e.g. upstream U,
  which also consumes T2, advertising "T2@9 delivered") could release a held record depending on
  T2@9 while this node's own T2 consumption was still at a lower offset. The delegate then processed
  the effect before the cause it directly subscribes to — precisely the ordering violation the README
  forbids ("every Kafka Streams processor that subscribes to both topics processes A before B"). The
  claim of Birman-Schiper-Stephenson equivalence was wrong on exactly this point: BSS's delivery
  condition is over the receiving process's *own* delivered vector, never over peer hearsay.

  The delivery gate (`ParsleyEngine#isDeliverable`) now checks this node's own contiguous frontier
  exclusively, on every release path (`receive`, `drainSatisfied`, `propagate`), and the candidate
  index is built against the frontier rather than completeness so a claimed-but-not-locally-delivered
  coordinate stays indexed until the frontier genuinely reaches it. `ParsleyFrontier#tryAdvanceEpoch`'s
  window-close dominance check moves from `completeness()` to the frontier for the same reason — a
  hearsay-closed window would raise the floor and strip a held e-1 record's below-floor dependencies
  before this node had actually delivered them. `completeness()` is unchanged in shape and remains the
  outbound stamp (transitive ancestry each downstream receiver's own gate verifies locally) and the
  `channelAdvanced` relay signal; the channel-hearsay-triggered `drainSatisfied` rescans in
  `receive`/`onWatermark` are gone (a channel-clock change can no longer make anything deliverable —
  only a frontier advance or an epoch-floor rise can), and `ParsleyFrontier#channelCount` is deleted
  with them. Liveness is unaffected: a dependency on a directly-consumed coordinate is satisfied by
  this node's own (possibly passthrough) consumption genuinely catching up, which Kafka delivers
  regardless — the hearsay path only ever released it *early*, which is exactly the bug. Every channel
  fold now also uniformly strips this node's own produced coordinates (`ownSinkTopics`) — previously
  only `onWatermark` stripped them while the business-record paths folded raw clocks, so a cyclic
  topology's reflected self-position triggered a full-buffer rescan plus a spurious heartbeat on every
  business record. New `ParsleyEngineWatermarkTest` (the first engine-level `onWatermark` tests) pins
  the hearsay scenarios; `ParsleyFrontierTest` pins the epoch-window one. Docs (`concepts.md`,
  `streams.md`, `internals/engine.md`, `internals/causal-consistency.md`,
  `internals/topology-epochs.md`, `internals/overview.md`, `overview.html`) rewritten to describe the
  gate/stamp split — including retiring `internals/engine.md`'s stale receipt-time channel-update and
  `continue`-policy passages, which described code removed several changes ago.
- **A node observing its own produced coordinate reflected back to it — directly, by also consuming its
  own sink, or indirectly, via a downstream peer's stamp in a topology cycle — could fail two different
  ways.** A direct self-consumer (the tightest possible cycle) never converged: every watermark it
  received on that channel carried its own ever-advancing self-position, which the ordinary out-of-scope
  logic cannot distinguish from genuine foreign progress, so `channelAdvanced` never settled `false` and
  the marker relayed forever — an infinite loop, found via a `TopologyTestDriver` test during this
  redesign's verification pass, not merely a slow test. Indirectly, a peer's stamp reflecting this node's
  own coordinate back to it (e.g. B, in a two-node A→B→A cycle, stamping a reply with A's own `from-a`
  position) was instead wrongly rejected as an unreachable dependency, even though the coordinate is this
  node's own and therefore never actually unverifiable.

  `ParsleyEngine` gains `ownSinkTopics`, a predicate for coordinates this node itself produces
  (`ParsleyProcessor` resolves sink-topic UUIDs unconditionally at `init()`, never gated by `parsley.
  topology.validation`, since this is a correctness mechanism, not a lint). Both `effectiveDependencies`
  (the business-record gate) and `onWatermark` now strip a node's own sink coordinates from any inbound
  dependency or marker clock before every check. This is sound and deliberately narrower than — not a
  relaxation of — the general out-of-scope fail-closed rule: a claim naming this node's own coordinate can
  only ever have arisen from something this node itself already produced, since nothing else can ever
  advance it, so the claim is either already, trivially known here or could not have legitimately arisen
  at all. `ParsleyClock#retaining`'s Javadoc is updated to document this one narrower exception to its
  "never on an inbound dependency clock" rule.
- **A mismatched sink partition count only warned by default, but under topology-epoch coordination it
  crash-loops the task instead.** `ParsleyMarkerPartitioner` routes an epoch marker to this task's own
  owned partition (`taskId().partition()`) unconditionally; with a sink that has fewer partitions than
  a source, the produce fails outright at runtime, and the task restarts into the same failure instead
  of surfacing a clear, one-time startup error.

  `ParsleyProcessor#validatePartitionParity` now escalates a partition-count mismatch to a hard failure
  whenever topology-epoch coordination is configured, regardless of the configured `parsley.topology.validation`
  mode — `strict` behaves as before, and an explicit `off` still disables the check entirely (a
  deliberate, complete opt-out), but the default `warn` is treated as `strict` under coordination since
  the failure mode a warning would otherwise hide is a crash loop, not a quiet correctness gap.
- **Epoch floors were not actually monotonic across epochs, contrary to `ParsleyEpochState.onBoundary`'s
  documented assumption.** `proposeCommit`'s raw `mergeMin` of published frontiers can drag a shared
  coordinate's floor backwards: a member admitted mid-round that consumes from `earliest` publishes
  completeness far behind the already-committed floor, so the next commit's `mergeMin` on that
  coordinate reflects the newcomer's lag, not genuine progress. Every consumer of the floor tolerates a
  regression except `ParsleyFrontier#pruneStaleOrphans` — an orphan entry pruned under a floor that
  later regresses below it holds that coordinate's dependents forever again.

  `ParsleyEpochLog` now tracks the last committed `lowerBounds` and `proposeCommit` clamps the proposed
  floor to it via `ParsleyClock#merge` (the per-coordinate maximum) before returning the commit — a
  floor can only ever hold or advance, never regress, regardless of what a newly-promoted member's
  frontier reports. Every node computes the identical clamp (a pure function of the same ordered log),
  so the leaderless collect-then-commit protocol's "every node agrees" property is unaffected.
- **`ParsleyEpochRuntime`'s committed epoch id and lower bounds were two independent volatile fields, so
  a caller reading both back-to-back could observe a torn pairing.** `pollEpochCoordination` reads the id
  and then the bounds as two separate volatile reads while `runOnce` writes id-then-bounds; a commit
  landing in between yields a boundary stamped with a fresher id than the bounds it carries. The relayed
  boundary is then never re-adopted (the per-epoch guard only advances), so every consumer downstream is
  merely conservative until the next commit — not unsafe, but avoidable entirely.

  The two fields are now one `CommittedEpoch(epochId, lowerBounds)` record behind a single volatile
  reference, read together via the new `committedEpoch()` accessor wherever both are needed as a pair
  (`ParsleyProcessor`'s epoch-state initialisation and `pollEpochCoordination`'s boundary relay). The
  existing `committedEpochId()`/`committedLowerBounds()` accessors remain for callers that only need one.
- **Two classes of stale index entry were never pruned: below-watermark forwarded-index entries, and
  candidate-index entries `orphan()` discovers pointing at an already-removed record.** Neither is a
  correctness bug — both are purely cosmetic, unbounded store growth — but both are permanent once hit,
  since neither entry's coordinate can ever revisit and clean itself up naturally (a below-watermark
  offset can never be re-absorbed; an orphaned coordinate never advances again).

  `ParsleyForwardedIndex` gained `pruneAtOrBelow`, called once per restored coordinate when a durable
  `ParsleyFrontier` loads, sweeping any entry left below that coordinate's watermark (e.g. by the
  benign tear direction `deliver`'s Javadoc describes — now closed off by the `exactly_once_v2`
  requirement, but still possible in a store carried over from before that requirement existed).
  `ParsleyEngine.orphan`'s `letter == null` branch (a stale candidate whose record another step already
  removed this pass) now calls `candidateIndex.prune` before continuing, instead of leaving the entry
  indexed forever — mirroring `propagate()`'s existing stale-entry pruning, which an orphaned coordinate
  can never trigger on its own since it never advances again.
- **`orphan()`'s scan gating was floor-blind: a coordinate discovered twice in one cascade, via two
  different parent branches, at two different floors, skipped the second scan entirely.** The set
  tracking "coordinates already scanned this pass" keyed on `(topicId, partition)` alone, so once a
  coordinate was scanned at some floor, a later worklist task for the same coordinate at a *lower*
  floor was dropped without ever scanning `[lowerFloor, alreadyScannedFloor)` — a genuine dependent
  requiring an offset in that range was never dead-lettered in this pass, staying buffered until (at
  best) the next full drain, an unbounded liveness delay on an idle topology.

  `ParsleyEngine.orphan` now tracks `Map<Coord, Long> lowestScannedFloor` instead of a set, and rescans
  whenever a newly-popped task's floor is strictly lower than what's already recorded for that
  coordinate, recording the new minimum. `findCandidatesRequiringAtLeast` at a lower floor is a strict
  superset of any narrower scan already done, so already-handled records are skipped via the existing
  `seenRecords` guard; nothing is scanned twice for no reason beyond the genuinely-new range.
- **`tryAdvanceEpoch` gated on the DAG-wide committed floor, so an epoch transition could never settle
  at a non-terminal node.** The committed floor `F_e` is the `mergeMin` of every member's published
  completeness, so it names coordinates for every topic in the DAG — including topics downstream of (or
  parallel to) a given node. A node's own `completeness()` can never contain a coordinate it has no
  input channel for, so comparing it against the unfiltered floor made the dominance check permanently
  false everywhere except the terminal stage: the epoch feature's per-coordinate floor advance,
  below-floor dependency stripping, and orphan pruning on promotion were silently inert at every other
  node, and a fresh joiner (which settles directly at the full committed floor) could reach a floor an
  established member never could.

  `ParsleyFrontier.tryAdvanceEpoch` now filters the pending floor to this node's own channel
  coordinates (`ParsleyClock.retaining`, the same scoping `pruneToScope` already applies) before the
  dominance check — a coordinate this node can never observe no longer blocks the transition. A
  coordinate that *is* in scope but not yet advertised is deliberately left in the filtered floor, so
  `dominates` still holds the window for it (conservative: an absent coordinate is never dominated) —
  the transition never closes early against a channel that genuinely hasn't caught up.
- **`ParsleyOrphanIndex.markOrphaned` kept the highest floor ever recorded for a coordinate, but the
  lowest dead-lettered offset is the true, permanent floor — a coordinate's contiguous frontier freezes
  at the earliest offset ever proven unreachable, regardless of what is proven unreachable afterward.**
  Keeping the maximum resolved cross-offset dead-letters in the wrong direction both ways: a later
  dead-letter at a higher offset silently weakened an already-established floor (overwriting it upward);
  a cascade discovering a lower, truer floor for an already-orphaned coordinate was silently dropped as a
  no-op. Either way, a dependent requiring an offset between the true floor and the wrongly-recorded one
  could pass `isProvenImpossible`'s check and be buffered instead of dead-lettered — and since the
  coordinate's frontier is frozen below the true floor forever, that dependent was never released, never
  proven impossible, and never cascaded: a permanent silent wedge, exactly the failure mode
  dead-lettering exists to prevent.

  `RocksOrphanIndex` and `MockOrphanIndex` now keep the minimum floor: `markOrphaned` is a no-op only
  when an existing floor is already at or below the new one, and always establishes on a coordinate's
  first call regardless of the new floor's value (the `-1` "not orphaned" sentinel is now an explicit
  absence check, not folded into the numeric comparison). `isProvenImpossible` and `pruneStaleOrphans`
  were already written correctly for minimum semantics and needed no change.
- **The handoff grace cache was in-memory only, so a crash inside the handoff window could lose a
  departing topic's grace cycle permanently.** `pollEpochCoordination` gave a topic that just stopped
  being an external source (some member declared it as their sink) exactly one more adoption cycle from
  its outgoing self-adopter, tracked via a per-task in-memory field (`lastAdoptedExternalSourceTopicIds`)
  that reset on restart. A crash between the topic leaving the live registry and this task's next
  adoption cycle lost that memory: the post-restart poll saw empty adoption targets and silently advanced
  past the handoff epoch with no relay ever sent for it — a permanent per-epoch floor-advance gap for
  that one coordinate.

  `ParsleyEpochLog` now retains a two-slot shift register of `externalSourceTopics()` snapshots, updated
  on every `EpochCommitted` — `externalSourceTopicsAsOfPreviousCommit()` is always "the registry as of
  one commit ago," derived purely from replaying the log, which every node (including one that just
  restarted, with no other memory) reconstructs identically. `pollEpochCoordination`'s adoption targets
  are now `live ∪ externalSourceTopicsAsOfPreviousCommit()` instead of `live ∪ lastAdopted`, so the grace
  window survives a crash — the per-task in-memory field is gone.
- **Epoch marker relay depended on a business key that might not exist yet, silently stalling every
  downstream lane for a genuinely idle source-layer task.** `forwardEpochSnapshot`/`forwardEpochBoundary`
  routed by reusing `lastSeenKey` — the most recent business record's key — through whatever partitioner
  the sink used, because that was the only way to steer a record onto a specific partition. A source-layer
  task that had not yet processed a business record (notably right after a restart, whose in-memory
  `lastSeenKey` is wiped) had nothing to route on, so `injectSnapshot`/`adoptAndInjectBoundary` silently
  skipped the relay and retried on a later poll — but if that task's restored completeness already
  dominated a floor committed while it was down, its very first poll could self-adopt and promote in the
  same tick, settling with zero downstream relays ever sent. Every downstream lane then stalled until the
  task's first post-restart business record finally triggered a retry — unbounded, since the input topic's
  idle time bounds it.

  `ParsleyMarkerPartitioner` (installed by `CausalTopology` on every sink a stage declares, wrapping
  whatever partitioner — custom or default — the stage already used) now routes a marker to the
  forwarding task's own owned partition directly, via a new `ParsleyMarkerPartition` thread-local
  `ParsleyProcessor#forwardToSinks` sets immediately before and clears immediately after every marker
  forward. A business forward is unaffected — the override is only ever set around a marker's own
  `context.forward` call. Since routing no longer depends on a key at all, the `lastSeenKey != null`
  relay-skip-and-retry gates in `injectSnapshot`/`adoptAndInjectBoundary`/`pollEpochCoordination` are gone;
  every relay now goes out unconditionally, on the very first poll.
- **Dead-letter paths removed a victim from the buffer before durably recording its orphan floor, the
  same torn-write shape the delivery paths were fixed for.** `ParsleyEngine#fetchForDeadLetter` (used by
  `drainSatisfied`'s proven-impossible branch and `orphan`'s own cascade loop) and the poison/
  proven-impossible branches of `propagate`/`drainSatisfied` all called `buffer.remove` before
  `deadLetterRoot` (and therefore before `ParsleyOrphanIndex#markOrphaned`) ever ran for that record's own
  coordinate. A crash between the two writes left the record gone from the buffer with no orphan floor
  recorded — a buffered dependent on that exact coordinate was then held forever, never proven impossible,
  the same permanent-wedge shape as the dead-letter-at-ingest bug above, and the record could be lost
  without ever reaching the dead-letter topic. Every dead-letter path now marks a victim's own coordinate
  orphaned *before* removing it from the buffer, mirroring the earlier frontier-persistence-ordering fix,
  so a crash always tears toward "orphan floor recorded, victim still buffered" — resolved as a harmless
  duplicate dead-letter by the next `drainSatisfied`/restore pass. `orphan`'s worklist loop now tracks
  which coordinates it has scanned for dependents in a set local to the call, independent of
  `markOrphaned`'s return value — needed because that return value can no longer double as "first time
  seeing this coordinate" once a victim's own coordinate may already be pre-marked by the time its
  worklist task is popped.
- **An `UNRESOLVABLE_CLOCK` record dead-lettered at ingest never orphaned its coordinate, deterministically
  stranding dependents.** `ParsleyProcessor#onUnresolvableClock` dead-lettered a record whose
  causal-dependencies header could not be decoded entirely inside the processor — `engine.onRecord` was
  never called for it — so unlike the `POISON`/`ORPHAN_CASCADE` paths (which go through `deadLetterRoot`
  → `orphan` → `markOrphaned`), nothing durably recorded that the coordinate could never advance past
  that offset: the offset was never delivered, the contiguous frontier froze below it forever, and any
  buffered record depending on that exact offset or later was held indefinitely, never proven impossible
  and never cascaded. No crash needed — an intra-topic dependency was enough (a producer stamps `U@k+1`
  with a dependency on `U@k`; `U@k`'s header is undecodable; `U@k+1` buffers forever). `ParsleyEngine`
  gains `deadLetterAtIngest(topicId, partition, offset)`, the ingest-time counterpart to `deadLetterRoot`
  for a record dead-lettered before it ever became engine state: it runs the same `orphan` cascade —
  marking the coordinate permanently unable to advance past that offset and dead-lettering any
  already-buffered dependent — without re-recording the root record itself (the processor already does).
- **A replayed already-delivered offset leaked a permanent, purely cosmetic entry in the forwarded
  index.** `ParsleyFrontier#deliver` unconditionally marked the delivered offset before walking the
  contiguous absorb run, but that walk only ever scans strictly above the current watermark — so an
  at-least-once replay of an already-delivered offset (`deliver(C, k)` with `frontier(C) >= k`, e.g. a
  duplicate redelivery) marked an entry below the watermark that could never be found and unmarked
  again, growing the changelog-backed forwarded-index store unbounded. `deliver` now returns immediately
  for an at-or-below-watermark offset, before ever marking it.
- **A new sink join could permanently strand a topic's epoch-boundary marker for one transition.**
  `externalSourceTopics()` is a live, memoryless view of the coordination log's current declarations, so
  the instant a new member declares an until-now-external topic as its sink, that topic drops out of the
  DAG-wide external-source registry immediately — one full round before the declaring member is even
  running, let alone able to relay anything in-band (it structurally cannot relay the very epoch whose
  round admits it). The outgoing self-adopter used to stop adopting for that topic in the same poll it
  left the live registry, so nobody ever injected that one epoch's boundary onto it — a conservative-safe
  but real, sometimes-permanent floor stall for anything downstream. `ParsleyProcessor` now injects a
  newly-committed epoch's boundary onto the union of the live registry and the registry as of its own
  last adoption, giving a departing topic one more adoption cycle from its outgoing self-adopter. Also
  fixed two related gaps in the same mechanism: a fresh joiner's `lastAdoptedEpoch` was pre-seeded to the
  epoch that admitted it, silently skipping that joiner's own chance to relay the admitting epoch
  downstream on its own external-source inputs; and the per-epoch/per-round adoption guards used to
  advance even when the relay itself was skipped for lack of a routing key, permanently forfeiting a
  task's one chance instead of retrying once it had actually forwarded something.

  Still open: marker relay routes on a business key (`record.key()`/the last-seen key), not an explicit
  per-partition broadcast, so a topic with a genuinely idle partition can still starve that partition's
  marker — a separate, deeper design question tracked in BACKLOG.md.
- **`onRecord` could skip the drain a proven-impossible record's own channel advance had just enabled.**
  A record's admission always updates its channel's clock first, before its own disposition (deliver,
  buffer, or dead-letter) is decided — this is what lets two sibling records depending on a shared
  ancestor unblock each other without deadlocking. But when the record turned out to be proven
  impossible (dead-lettered rather than forwarded), `onRecord` returned immediately after recording the
  dead letter, skipping the channel-advance drain and epoch-transition check at the end of the method —
  so another buffered record the channel advance had just made deliverable stayed held with no further
  trigger to re-check it, potentially forever (a fan-in node with no subsequent watermark on that
  channel). `onRecord` no longer returns early from the proven-impossible branch; every disposition now
  falls through to the same tail drain.
- **A torn changelog flush under at-least-once could permanently strand a coordinate's frontier.** The
  buffer store, frontier store, and forwarded-index store are three separate changelog topics with no
  cross-store atomicity, so a crash mid-release could tear two different writes apart, in two distinct
  places: (1) `drainSatisfied`/`propagate` removed a record from the buffer before persisting the
  frontier's delivery of it, so a crash in between left the record gone from the buffer (unrecoverable
  on restart) but the frontier still showing it undelivered, permanently freezing that coordinate; (2)
  `ParsleyFrontier.deliver`'s `mergeForward` pruned absorbed forwarded-index entries before the frontier
  advance that accounted for them was persisted, so a crash in that narrower window durably lost the
  forwarded-index entries backing an advance nothing else remembered — the same permanent-wedge shape,
  entirely internal to `deliver()`, affecting even records delivered immediately (never buffered). Both
  release paths now persist the frontier/forwarded-index advance *before* removing the buffer entry, and
  `deliver()` persists the new frontier value *before* pruning the entries it absorbed — so a crash
  anywhere in either window now always tears toward an already-accepted benign outcome (an
  at-least-once duplicate redelivery, or a harmless stale forwarded-index entry below the frontier)
  instead of an unrecoverable wedge.
- **`ParsleyEpochRuntime.unregisterMember` was never called, so a rebalanced-away member could hang or be
  wrongly evicted by a later `leave()`.** `ParsleyProcessor#close` — called by Kafka Streams whenever a
  task stops running on this instance, including a rebalance that migrates it elsewhere, not just a
  genuine shutdown — never told the shared runtime the member had gone. The departed member stayed in
  `localMembers` forever: its stale (possibly non-drained) `reportDrained` state never updated again, so a
  later graceful `leave()` on this instance could hang unboundedly in its drain phase, or — if driven past
  that — append a `Leave` in its remove phase for a member actually running elsewhere, precisely the
  "excluding an un-drained member" hazard `ParsleyMembershipStrategy`'s safety invariant exists to
  prevent. `close()` now calls `unregisterMember` unconditionally, dropping the member from local
  bookkeeping the moment its task leaves this instance; a re-join on this or another instance re-adds it
  with no log event required.
- **`awaitJoinCommit` could deadlock an instance when a joiner shared a `StreamThread` with a running
  member.** Kafka Streams runs every task on a `StreamThread` — including `init()` and punctuators — on
  that one thread, so a joiner blocked in `awaitJoinCommit` inside `init()` also blocked any running
  member sharing the thread from ever running `pollEpochCoordination()`, the only place its
  `publishFrontier` was invoked. The round the joiner opened could then never complete, wedging the
  instance forever. `ParsleyEpochRuntime` now lets its own background thread — distinct from every
  `StreamThread` — publish a stalled local member's completeness on its behalf, from a live snapshot
  (`ParsleyProcessor#stampFrontier`) registered once the member's engine exists
  (`registerLocalCompleteness`). Always-safe, no timeout: completeness is monotonic and the committed
  floor is already a conservative merge-min, so publishing a possibly-stale snapshot only ever makes the
  floor more conservative, never unsafe.
- **`ParsleyEngine.propagate()` could both forward and dead-letter the same record in one cascade
  pass.** It used to collect deliverable candidates into a batch and release them only after scanning
  every candidate at that level, so a poison candidate found later in the same scan could dead-letter
  (via its orphan cascade) an entry already collected as deliverable but not yet removed from the
  buffer — the release loop then forwarded it anyway, violating `Outcome`'s "never both" contract and
  permanently corrupting the frontier/orphan-index state for its dependents. Each candidate is now
  committed to its final disposition the moment it is decided, never staged for a later batch step.
  `propagate()` also now checks `isProvenImpossible` (as `onRecord`/`drainSatisfied` already did), so a
  candidate whose direct dependency was orphaned by an unrelated cascade is dead-lettered instead of
  forwarded just because the completeness frontier — driven by cross-channel header advertisement, not
  genuine local delivery — happens to dominate it.
- **`CausalStreams#close()` could hang forever if a task's buffer emptied before `requestQuiesce()` was
  ever called.** `ParsleyProcessor#updateQuiesceState` only re-evaluates `isQuiesceRequested() && empty`
  on a buffer-depth-changing event; a task already idle-and-drained at that point recorded
  `drained=false` (quiesce hadn't been requested yet) and never got another chance to report otherwise,
  since nothing further ever changed its buffer depth. Found via the dead-letter IT below, but not
  specific to dead-lettering — any topology whose last held record drains through the ordinary path
  before shutdown could hit it. The existing periodic metrics-refresh punctuator (every 5s) now also
  re-pushes the drained state, closing the gap within one tick.
- When a Kafka topic is dropped and recreated its UUID changes, causing the old UUID to leave the
  processor's `consumedTopicIds`. Buffered records whose only dependencies named the old UUID had
  empty effective dependencies after restart and were skipped by `drainRestoredSatisfied()`. Since
  no new records arrive on the dropped topic the drain path was never retriggered, leaving those
  records stuck in the buffer indefinitely. They are now released immediately — empty effective
  dependencies mean all raw deps are out-of-scope and therefore vacuously satisfied.
- The persisted frontier (`ParsleyClock`) accumulated entries for every topic UUID ever observed and
  was never pruned. Stale entries for topics that no longer exist grew the stored clock
  unboundedly. On startup the restored frontier is now filtered to the current `inScope` predicate,
  keeping it compact across restarts.
- After a restart under `at_least_once` processing, buffered records whose in-scope causal
  dependencies were already satisfied by the restored frontier could become permanently stuck and
  eventually evicted as spurious causal violations with an empty gap (`gap: ParsleyClock{}`). The
  root cause was the engine constructor re-indexing restored buffer entries using raw stored
  dependencies instead of effective dependencies (in-scope filtering and self-reference stripped),
  causing records to be indexed only under out-of-scope dead-end coordinates that the release path
  never visits. The constructor now uses effective dependencies, and a `drainRestoredSatisfied()`
  pass runs at startup to release any record already satisfied by the restored frontier.
- Causal dependencies on coordinates a processor does not consume — a topic outside its registered
  buffers, or a partition its task does not own — are now treated as vacuously satisfied instead of
  holding the record until eviction. A producer stamps a clock spanning every coordinate it
  consumes, so a downstream processor routinely sees dependencies it can never observe; these no
  longer block, evict, or fail the task. A dependency on a coordinate the processor *does* consume
  but has not yet observed still blocks, as before.
- Multi-layer causal ordering is now sound across nodes that fan in or reconverge. A node's output
  was previously stamped with only its own frontier, discarding the inbound record's transitive
  ancestry: in a multi-hop topology (T1 → Node A → T2 → Node B → T3), a downstream node C
  subscribing to T1 and T3 could not enforce T1@x → T3@z because Node B silently dropped the T1
  coordinate. Nodes now stamp their completeness frontier — their own contiguous frontier
  max-merged with a per-coordinate minimum across every input channel's last-seen clock — so
  transitive ancestry flows through intermediate nodes, and a fan-in advertises completeness only
  up to its slowest branch rather than over-claiming with a maximum (the interim per-record
  max-merge stamp, never released, was correct only when no two input branches shared an ancestor).
  Downstream nodes still apply `effectiveDependencies` filtering, and the delivery gate gained a
  second part (`ancestorsSettled`) that holds a record until every sibling input branch that knows a
  shared out-of-scope ancestor has caught up to it — closing a reconvergence race where two branches
  sharing an ancestor could deliver out of order.
- Fixed a mutual deadlock between two sibling records that each depend on a shared ancestor and each
  arrive before the other is delivered. The engine now records a record's carried frontier on its
  source channel at receipt time, before gating, so a shared ancestor is confirmed by a sibling
  branch's business record (not only by a watermark) without either record waiting on the other.

### Documentation
- **The causal-broadcast page's stamping-order description is corrected to match the code.** The
  `broadcast()` pseudocode, its numbered walkthrough, the module-box overview, and the processor
  reference all had the crossing wait and the acknowledgement fold in the wrong order (fold then
  wait). The code and the `broadcast` Javadoc run the crossing wait first and fold second, which is
  the correct order: the wait blocks precisely until the outstanding sends acknowledge, so folding
  after captures the coordinates it was blocking for, where folding first would miss them and
  under-claim the stamp. The walkthrough now states that reason.
- **The minimum supported Kafka broker version is documented: 3.7.0.** The integration suite now
  runs against both the 3.7.0 minimum and the current stable broker line (4.3.1 at the time of
  writing) in a CI matrix; the broker image is centralized behind one test seam
  (`-Dparsley.it.kafka.image`). Stated in the README and the getting-started prerequisites.
- **The getting-started and Streams code samples use neutral channel and message names** —
  `c1`/`c2`/`c3` topics (a Kafka topic-partition is the causal channel of the literature) and
  `m1`/`m2`/`m3` messages, the naming convention of the causality papers Parsley cites — in place
  of the previous domain-flavoured examples (`orders`, `prices`, `trigger`, `priceUpdate`, ...).
  The samples are now compiled and exercised by a test (`DocsSamplesTest`), so a public-API
  change that would break a published sample fails the build.
- **The `parsley-causal-clock` wire format states that entry order is unspecified.** The encoder
  walks unordered maps, so a multi-entry clock's entries appear in no guaranteed order;
  `docs/internals/wire-format.md` now says so explicitly, and a decoder must accept entries in any
  order. The layout itself is unchanged, and is now pinned byte-for-byte by a golden-bytes test
  (`ParsleyVectorClockWireFormatTest`), so an accidental encoding change fails the build instead
  of only breaking external implementations and in-flight records.
- **A full audit pass over every documentation surface (mkdocs pages, Javadoc, `overview.html`,
  README) fixes doc/code drift and deepens the explanations.** The topology examples now build
  exactly one stage and call `build()` on `CausalProcessedStream` (the previous `builder.build()`
  example did not compile). Null messages are described as the code implements them: they carry
  the triggering record's key as information only and are routed to the forwarding task's own
  partition on every sink, bypassing the user partitioner. The metrics tables cite the sensor as
  `deps-out-of-scope-ignored`, matching `ParsleyMetrics`. `troubleshooting.md` states that startup
  rejects the built-in record-skipping exception handlers rather than presenting handler choice as
  free configuration, and explains why record-skipping is causally unsafe. `configuration.md`
  documents that the `Properties` passed to `CausalStreams` overlay the classpath
  `parsley.properties`. Design-history narration is removed in favour of present-tense
  description, `migration.md` is replaced by `adoption.md` (incremental adoption of stamping;
  unstamped records are vacuously deliverable), and `streams.md` gains Shutdown and Failure
  handling sections covering `close()`/`close(Duration)` semantics and the public
  `CausalDeliveryException` hierarchy. Citations are anchored where terms have exact literature
  meanings (Birman–Schiper–Stephenson for the delivery condition, Lloyd et al. for COPS). The
  README no longer forecasts a fault-injection suite for 1.0.
- **The documentation is rewritten around the three protocol modules (T4.2).** The internals
  section now has one page per module — a new `internals/channels.md` (coordinates and identity,
  density, own outputs, scope changes), `internals/engine.md` renamed to
  `internals/causal-broadcast.md`, and a new `internals/gossip.md` — each opening with its module
  box in the request/indication/property style the source Javadoc uses.
  `internals/causal-consistency.md` is rewritten around the two-branch delivery gate and its
  soundness argument, and now states the three environmental assumptions explicitly: E1 (stable
  channel identity), E2 (retention must not destroy causally-live history — with a new operating
  note in Streams integration and a new troubleshooting entry for the out-of-range crash-loop),
  and E3 (participation is per-path — a new precondition in Streams integration). Four pages
  (`index.md`, `concepts.md`, `configuration.md`, `overview.html`) still described the retired
  fail-fast on dependencies naming unconsumed coordinates; all now describe the ignore branch.
  `internals/wire-format.md` documents the frontier blob's carried-ancestry, declared-input,
  own-outputs, and declared-sink sections, and the metrics table gains the `replays-skipped`,
  `reflected-claims-above-own-outputs`, and `records-held-above-highest-received` sensors.
  `migration.md` and the README state that pre-1.0 versions have no upgrade path (upgrades are
  fresh starts).

### Tests
- **The random-topology sweep (`ParsleyRandomTopologyPropertyTest`) runs without a recorded trace,
  so it no longer exhausts the heap at deep scale.** Each run previously retained a per-run trace of
  every scheduler step, which is needed only to shrink a failure; across the deep tier's thousands
  of runs this dominated the heap and the sweep died with `OutOfMemoryError` (the weekly deep job's
  documented `5000 × 2000` size never actually fit). The sweep now builds every run with
  `withNoTrace()` — as the soak already did — and, because runs are seed-deterministic, recovers the
  trace on the rare failure by re-executing that one seed with tracing on before delta-debugging it.
  Per-run memory is now flat regardless of step count, so the deep tier fits an ordinary heap. A
  shared `buildSim` helper keeps the sweep loop and the failure re-run from drifting on how a run is
  constructed.
- **`ParsleyTopologySim.drain()` gained a liveness guard: a settle that will not quiesce fails fast
  with the seed rather than spinning.** A non-quiescing I6 relay previously left the drain looping
  until it exhausted the heap; it now throws once the settle exceeds `RUN_TIMEOUT_MS` (default 300s,
  tunable with `-Dparsley.sim.run.timeoutMs`). The counter only gates the clock read and draws
  nothing, so a settling run's schedule is untouched. Running the deep tier with the memory fix
  surfaced the first such case, captured as the `@Disabled` repro `ParsleySharedSinkCycleNonQuiescenceTest`
  (deep-sweep seed 92672, a 3-producer shared-sink cycle) and tracked in #32 pending root-cause.
- Added two `ParsleyEngineTest` cases verifying that a contiguous-frontier jump releases every
  buffered record whose dependency falls anywhere in the jumped range — not just records waiting on
  the final boundary offset. One test covers five records each waiting on a distinct intermediate
  offset (8–12) within a 4→12 jump; the other is a minimal reproduction with a single record
  waiting on offset 10.
