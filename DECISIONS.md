# Decisions

One record per choice the specification left open. The alternatives are the point; a record without them is a changelog.

Relying on an assumption in `SPEC.md` is itself a choice. Record which ones you took, and what breaks if one proves
false.

Records are append-only. Numbers are never reused and never renumbered; a decision that is superseded keeps its number
and says which record supersedes it.

## Template

### D0 — *what was decided*

**Context**

*what forced a choice here, and what the specification does or does not say.*

**Decision**

*what was chosen.*

**Alternatives**

*what else was considered, and why each was rejected.*

**Cost**

*what this choice makes harder, now or later.*

**Specification gap**

*if you believe the specification should have made this choice rather than leaving it open, say so here.*

## Decision log

### D1 — One Kafka Streams application per declared process

**Context**

The specification allows any number of processes to receive from the same channel (Terminology: *Channel*;
Structural 2, 18). A single Kafka Streams application cannot register one topic in two source nodes, and all its
tasks share one consumer group, so within one application a topic-partition is consumed once.

**Decision**

Each declared process becomes its own Kafka Streams application, `application.id = <prefix>-<processName>`, hence its
own consumer group with its own committed offsets and state directory. A spec *process* is then one task of that
application (Assumption 5), receiving partition *i* of each of its declared topics.

**Alternatives**

* One application for the whole declaration — rejected: two processes receiving one topic is impossible, violating
  Structural 2; also couples all processes' rebalances and failure domains.
* One application per process with shared consumer group — not expressible in Kafka.

**Cost**

N processes mean N consumer groups, N sets of Streams threads and N state directories; more moving parts to operate.
Cross-process ordering still needs no coordination — that is the point of the protocol.

### D2 — A channel is identified by (topic ID, partition)

**Context**

Assumption 2 permits assuming a channel is a topic-partition "identified such that a topic deleted and recreated
under the same name is a different channel". Metadata must name a cause's channel (Structural 11), and a stale name
must never make a new topic answer for an old one's positions.

**Decision**

Channel identity everywhere — metadata, ordering state, engine — is the Kafka topic ID (a UUID the broker assigns at
creation and never reuses) plus partition number. Names are resolved to IDs once at startup and appear only at the
edges (declaration, admin queries).

**Alternatives**

* Topic name + partition — rejected: a recreated topic would inherit dependencies pointing above its log end, blocking
  forever or, worse, matching wrong positions. Assumption 2's identification became unusable the moment metadata
  outlives topics.
* Name + creation-epoch fingerprint (log-start + first-record hash) — rejected: heuristic, and the broker already
  maintains the real identity.

**Cost**

Requires brokers with topic IDs (KIP-516; satisfied by the mandated 3.7.0+). Metadata is opaque to humans without a
resolution step. Facts gathering must map IDs to names to query offsets, with a race guard (D22).

### D3 — Causal metadata wire format: one reserved header, strict canonical binary

**Context**

Metadata must be attached to the message but outside key and value (Safety 4), documented and stable (Structural 5),
distinguishable by construction from application metadata, and must express causes solely as (channel, position)
pairs (Structural 11).

**Decision**

A single Kafka record header, key `parsley.causes`, value: version byte `0x01`, entry count (int32), then per entry
topic ID (16 bytes), partition (int32), position (int64), big-endian, entries strictly ascending by channel. The
format is frozen in `docs/wire-format.md`. Decoding is strict: unknown version, truncation, trailing bytes, disorder,
duplicates or negative values are undecodable and fail closed (Safety 7). The header is attached to every emission,
including when the cause set is empty. Application headers using the `parsley.` prefix are refused.

**Alternatives**

* JSON or another self-describing text format — rejected: larger, slower, and looser (what does a duplicate key
  mean?); canonical bytes make equality and tests exact.
* One header per cause — rejected: header ordering and duplication semantics in Kafka are weak, so atomicity and
  canonical form are lost.
* Protobuf/Avro-encoded header — rejected: drags a codec dependency into every reader of the *spec* of the format;
  the format must not depend on an implementation's internals (Structural 5).
* Omitting the header when empty — rejected: "no metadata" and "empty metadata" would be indistinguishable on the
  wire from "sender is not a parsley implementation"; a constant marker keeps provenance visible. Cost of a few bytes.

**Cost**

25 bytes fixed plus 28 per cause on every message. A version bump requires a documented migration.

### D4 — Dependencies are summarized as one maximum position per channel

**Context**

Metadata must express every cause at or above its channel's earliest retained position (Structural 15), must not grow
without bound (Structural 13), and replacing two pairs on one channel by the greater position is defined as
compression, not discarding. *Express* permits naming a position at or above the cause's.

**Decision**

The engine keeps a causal frontier: `channel → highest position that is a cause of this process's subsequent sends`.
Delivery of a message merges its (channel, position); receipt of any message merges every pair of its metadata
(happened-before passes through receipt — Terminology: *Cause*; Structural 15). Emissions carry the whole frontier.
Sends do not enter the frontier: under the spec's definitions causality flows only through delivery-before-send, and
this is also what discharges Structural 14 by construction — everything in the frontier was assigned before the
send's own position.

**Alternatives**

* Exact cause sets (all pairs, no compression) — rejected: unbounded growth, violating Structural 13.
* Direct dependencies only, with transitive closure recovered at the receiver — rejected: the receiver would need to
  read cause messages' metadata before deciding deliverability, i.e. hold messages it may never receive; and Safety 3
  plus per-channel max make transitive coverage free anyway.
* Vector clocks indexed by process — forbidden outright: metadata must not carry an identity for the sending process
  (Structural 11).

**Cost**

Over-approximation: a dependency on (c, 100) also waits for concurrent messages on c below 100 that were not causes.
This is a latency cost, never a safety cost, and per-channel FIFO (Safety 3) already imposes most of it.

### D5 — Deliverability: settled frontier from fed-or-never plus the hold-back buffer

**Context**

The deliverability decision must be a pure function of message, ordering state and the criteria (Structural 7).
Positions may never yield a message (Fault model 4), successive positions need not be consecutive (Fault model 1),
and a cause naming such a position must still eventually deliver, without relying on elapsed time (Liveness 3).

**Decision**

Per received channel the engine tracks `fedUpTo` — the highest position such that every position at or below it was
fed to this process or will never arrive — advanced by in-order receipt (a record at offset o covers everything below
o, by Kafka's per-partition order plus Host obligation 1) and by the host's read-position reports. The settled
frontier is derived: `settled(c) = holdback empty ? fedUpTo(c) : head(holdback(c)).position − 1`. A dependency
(c, p) is satisfied iff c is outside the received-channel set or `p ≤ settled(c)`. The decision is the public static
`Deliverability.decide`, the same unit the engine invokes for every delivery, testable with no host.

**Alternatives**

* Tracking delivered positions as a set with explicit gap records — rejected: more state, same information, and gap
  knowledge is exactly what fedUpTo already encodes.
* Treating a dependency on an unknown channel state as satisfied — rejected: unsound before the first receipt
  baseline exists; the settled frontier makes "nothing known" block, and initial-position facts (D9) make that
  transient.

**Cost**

Settled progress on a quiet channel depends on fact ingestion (D6, D7); a fact-source outage delays delivery (never
misorders it).

### D6 — The host's read-position report is the feed itself plus committed group offsets

**Context**

Host obligation 2 requires the host to report read positions that eventually advance past positions that never
arrive, including trailing runs. Kafka Streams offers no push channel for this. Assumption 15 permits querying the
substrate for position facts without it counting as exchange between processes.

**Decision**

Two report transports. Within the feed: each record at offset o asserts everything below o was fed or never will be.
Out of band: the group's committed offsets, queried via AdminClient — Kafka Streams commits, atomically with each
step, the consumer position for every input partition, and for a partition with no buffered records that position has
advanced past aborted batches and control records (verified in the Streams 3.9.1 sources: `StreamTask#findOffset`
falls back to `mainConsumer.position`). A committed offset therefore never covers a message received but not yet fed,
which is exactly the report Host obligation 2 defines. Log-start offsets and topic existence ride along for Safety 8
and pruning.

**Alternatives**

* Broker end offsets (LSO) as the report — rejected as unsafe: the LSO covers messages the host's consumer has
  fetched but not yet fed to the processor; treating them as never-arriving delivers effects before buffered causes,
  violating Safety 1.
* A bespoke reporting topic written by the host — rejected: there is no host hook to write it atomically, and it
  adds a channel the spec does not ask for.
* Consumer lag metrics — rejected: not transactional, not per-execution-consistent, and semantically a gauge, not an
  assertion.

**Cost**

Liveness 3's trailing-run case depends on the host committing position advances for record-less partitions. Where a
host version fails to do so, that liveness premise is unmet and delivery of the affected message stalls — safely, and
visibly in the hold-back buffer. Admin queries add background load (bounded by D20's interval).

**Specification gap**

Host obligation 2 says "report" without saying where a Kafka Streams host reports. The spec could name committed
offsets as the canonical report; every implementation on this substrate will have to rediscover this.

### D7 — An internal wall-clock punctuator ingests facts

**Context**

Structural 10 forbids providing "timers, scheduled callbacks, or any means of causing a message to be delivered other
than by receiving it from a channel", while Liveness 3 requires eventual delivery when a cause names a position that
never yields, even if no further message ever arrives at the process — and forbids *detecting* this by elapsed time.
Those combine only if fact ingestion can run without a received message triggering it.

**Decision**

Each processor schedules an internal wall-clock punctuation (interval D20) that queries position facts and feeds them
to the engine, then drains. Structural 10 is read as binding the public API: parsley offers applications no timers
and no way to cause a delivery except a channel's messages — the punctuator is not exposed, and every message it ever
delivers was received from a channel and is delivered because a *report*, not a duration, satisfied its causes. The
deliverability decision itself never sees a clock (Structural 7 holds: time appears nowhere in its inputs).

**Alternatives**

* Piggyback fact ingestion on `process()` only — rejected: a process whose channels fall silent forever can never
  free a held message whose cause sits above a trailing dead run; Liveness 3 fails.
* Reading Structural 10 as banning even internal punctuation — rejected: it makes Liveness 3 unsatisfiable on this
  substrate, and a specification is not read to contradict itself; recorded here because it is a genuine
  interpretation call.

**Cost**

Wall-clock cadence bounds how quickly a trailing-run hold is released (liveness latency only). A skeptical auditor
must be walked through the Structural 10 argument.

**Specification gap**

Structural 10 could say explicitly that internal report ingestion is not "a means of causing delivery" in its sense.

### D8 — Failing closed stops the whole process, by exception

**Context**

*Fail closed* is defined as stopping delivery "at minimum on the affected channel". Safety 7 (undecodable metadata)
and Safety 8 (discarded positions) demand it. But Structural 15 requires every send to express causes known from the
metadata of received-but-undelivered messages — and an undecodable header hides exactly those causes.

**Decision**

Fail-closed events throw `ParsleyFailClosedException` out of the processor. The step aborts (nothing it did
occurred), the Streams application for that process shuts down (uncaught-exception handler returns SHUTDOWN_CLIENT),
and restart re-fails until an operator intervenes. The blast radius is deliberately the whole process, not one
channel: after an undecodable receipt, any further send might under-express causes, and a downstream process could
then deliver an effect before its cause — a Safety 1 violation laundered through "just one channel". Stopping
everything is the only sound reading.

**Alternatives**

* Quarantine the affected channel, keep delivering others — rejected as unsound: sends from other channels' deliveries
  under-express (Structural 15), see above.
* Dead-letter the undecodable message and continue — rejected: that converts a failure into delivering past the
  failed message, expressly forbidden (Terminology: *Fail closed*; Safety 3).
* Park the message and keep the process up but mute (receive-only, no deliveries) — rejected: equivalent to stopping,
  with more machinery and an amber state operators will misread.

**Cost**

One poisoned message halts a whole process until operated on. That is the specified trade: availability is spent on
never weakening the guarantee.

**Specification gap**

"At minimum on the affected channel" invites the channel-scoped reading; the spec could note that Structural 15 makes
anything narrower than the process unsound once metadata is unreadable.

### D9 — `auto.offset.reset=none`, with initial positions pre-committed while the group is empty

**Context**

Safety 8: a read position at or resuming below the earliest retained position must fail closed, never treat discarded
positions as empty. Kafka consumers by default *reset* on out-of-range — silently skipping — and Kafka Streams
resolves resets per source, defaulting to the consumer config. A fresh process also needs a defined first position
(Structural 12's below-first-receipt baseline).

**Decision**

The main consumer runs with `auto.offset.reset=none` and sources carry no per-source reset policy, so any out-of-range
or offset-less partition kills the task (verified against Streams 3.9.1 `StreamThread#resetOffsets`). Initial
positions are made explicit instead: at startup, for received partitions with no committed offset, the runtime
commits the declared initial position (earliest = log-start, latest = end; declared per channel, default earliest)
using the admin API while the group is empty. The engine independently fails closed when a log-start fact exceeds
`fedUpTo + 1` (defense in depth, and the testable seam).

**Alternatives**

* Per-source EARLIEST/LATEST reset — rejected: mid-run truncation beyond the committed position would silently jump
  the gap, exactly the Safety 8 violation.
* Engine-only detection via periodic log-start facts — rejected as sole mechanism: racy (a truncation between polls
  followed by a consumer auto-reset feeds messages the engine cannot distinguish from a legitimate gap); as
  defense-in-depth it stays.
* Handling first start with reset policies and only guarding mid-run — rejected: two mechanisms where one (explicit
  initial commit) covers both.

**Cost**

Startup requires admin access and fails hard when a partition appears mid-run (partition expansion) until the next
full start pre-commits it — a crash-recover path, documented. Operators lose the familiar reset knob by design.

### D10 — Re-feeds below the session floor are dropped; contradictions within an execution fail closed

**Context**

Safety 2 binds at-most-once delivery across the whole lifetime. Host obligation 5 defines re-feeds after restart
(exactly the uncommitted). But hosts can be operated: an offset rewind replays committed positions; and a host bug
could feed out of order within an execution.

**Decision**

The engine snapshots `fedUpTo` at initialisation as the *session floor*. A record at or below `fedUpTo`: if its
position is above the floor, this execution itself already covered it — that contradicts Host obligation 1/2 and
fails closed (OUT_OF_ORDER_FEED); if at or below the floor, it is a replay of a committed past — it was delivered
then, so it is dropped silently, preserving Safety 2 under operator rewinds and offset expiry alike.

**Alternatives**

* Fail closed on any at-or-below-fedUpTo feed — rejected: makes routine operator replays and group-offset expiry
  after a channel rejoins (Structural 16) permanently fatal, with no safety gain.
* Drop silently in both cases — rejected: an in-execution contradiction is a real host breach; obligations say breach
  detection must fail closed rather than degrade.

**Cost**

A genuinely new message at a position the host earlier reported as never-arriving (a false report) is indistinguishable
from a replay and is dropped: under that host breach, liveness for that message is lost — but no unsafe delivery ever
happens. Recorded in EVIDENCE.md under Host obligations.

### D11 — Parsley owns the KafkaStreams lifecycle; nothing weaker is constructible

**Context**

Substrate 3: exactly-once (`exactly_once_v2`, `read_committed`), neither overridable by an application. Structural 9:
no public operation whose documented use can violate safety.

**Decision**

`Parsley.start()` builds and owns every `KafkaStreams`. The topology, the properties, and the raw streams handle are
never exposed. `ParsleyConfig` refuses the owned keys (`processing.guarantee`, any `isolation.level`, any
`auto.offset.reset`, group identity, auto-commit) at build time, so the failure is immediate and attributable.

**Alternatives**

* Expose the `Topology` for the application to run — rejected: running it without EOS would be one properties file
  away, a documented path to a safety violation (Structural 9 forbids the operation's existence, not just its use).
* Validate at start() only — rejected: later validation reports the same error further from its source.

**Cost**

Applications cannot embed parsley processors in a larger Streams topology, tune per-source, or manage the streams
instance themselves. Interactive queries and custom lifecycles need facade extensions rather than raw access.

### D12 — The seam: `Effects handle(Delivery, StateReader)`; reads live, writes returned

**Context**

Structural 3: the seam passes only the delivered message and the application state, and accepts effects only through
the returned value — messages to send and state to persist — applied within the step. Structural 8: applications
read *and write* their state, including through host state facilities, not exclusively owned. Assumption 16 permits
assuming logic is a pure function of (message, state).

**Decision**

`Handler<K, V>` receives a typed `Delivery` and a read-only `StateReader` over the declared stores (live views of the
host's RocksDB stores), and returns `Effects`: typed emissions and typed state writes. The implementation applies
writes to the host stores and forwards emissions within the step, after the handler returns. Within one step,
earlier deliveries' writes are visible to later deliveries' reads (they are applied to the store immediately).
Nothing else is handed in: no context, no producer, no clock, no store mutators.

**Alternatives**

* Passing live mutable store handles — rejected: writes would be effects accepted otherwise than through the returned
  value, violating Structural 3 verbatim, and untracked writes blur the step boundary.
* Fully value-passing state (state in, new state out) — rejected: unusable for large keyed state, and it walls the
  application off from host state facilities, straining Structural 8.

**Cost**

Range scans and iteration are not in the v1 `StateReader`; handlers batch writes rather than mutate in place, which
reads unnaturally for some application styles.

### D13 — An application payload its own serde cannot decode fails the step

**Context**

Delivery hands the application decoded key and value. The spec does not address an application codec that throws on
its own data (not parsley metadata — Safety 7 does not apply).

**Decision**

Deserialization failure at delivery throws and fails closed. Skipping would deliver past the message (Safety 3);
delivering raw bytes would break the typed seam (Structural 4).

**Alternatives**

* Dead-letter and continue — rejected: delivering past, forbidden.
* Deliver a null value — rejected: indistinguishable from a real tombstone; silently corrupts application semantics.

**Cost**

A schema mistake halts the process (crash-loop until fixed). With Schema Registry codecs this is the behaviour their
users already expect.

### D14 — Application stores are byte stores; serdes apply at the seam; changelog-name serde scoping

**Context**

Structural 18 requires any number of typed stores per process. The host's store facility is byte-oriented at bottom;
serdes need a "topic" name for scoping (Schema Registry subjects).

**Decision**

Each declared store is a persistent, changelogged `KeyValueStore<Bytes, byte[]>`. `StateReader`/`Effects` apply the
declared serdes, scoped by the store's changelog topic name (`<applicationId>-<store>-changelog`), matching what
Kafka Streams itself would use — so external tooling reading changelogs agrees on subjects.

**Alternatives**

* Typed stores via Streams serde-wrapped builders — rejected: ties the seam's write path to store internals; byte
  stores keep ordering state and application state on one mechanism and make the reserved-prefix separation obvious.

**Cost**

Per-access serde work (same as Streams' own wrapped stores); interactive-query users must apply serdes themselves.

### D15 — Emissions inherit the delivered message's timestamp and default partitioning

**Context**

The spec never mentions timestamps except to forbid deriving deliverability from them (Structural 7). Emissions must
land on partitions somehow; the declaration names topics (Assumption 14).

**Decision**

An emission's record timestamp is the delivered message's timestamp (the Streams convention). Partition choice is the
default: murmur2 of the serialized key for keyed messages, sticky otherwise — indistinguishable from the application
producing directly with its own serdes.

**Alternatives**

* Wall-clock emission timestamps — rejected: injects nondeterminism into replays for zero spec value.
* An explicit partition parameter on `send` — rejected for v1: widens the API surface Structural 9 must defend;
  keyed partitioning is the Kafka norm the spec's assumptions lean on.

**Cost**

Applications needing custom partitioning cannot express it yet.

### D16 — Deterministic drain: channels in total order, to fixpoint

**Context**

2-safety 2: a restart must not be observable in what is delivered or in what order. After a crash the host re-feeds
the same messages; only a deterministic scheduler reproduces the same delivery sequence from the same state.

**Decision**

The drain loop repeatedly scans received channels in `ChannelId` order, delivering every deliverable head, until a
full pass delivers nothing. Identical state and feed always yield identical delivery order.

**Alternatives**

* Delivering in receipt order across channels — rejected: receipt interleaving across partitions is not stable across
  restarts, so replays could reorder concurrent deliveries.
* Randomised or fairness-rotating scans — rejected: same nondeterminism, plus it invalidates the replay argument the
  restart tests rely on.

**Cost**

A busy low-ordered channel is drained before a busy high-ordered one within a pass (starvation is impossible — the
fixpoint pass structure guarantees every deliverable head delivers each round).

### D17 — Ordering state: one reserved store; held bodies persisted only if still held at step end

**Context**

Held messages must survive restarts (Liveness 5) because the host commits read positions past records the engine is
still holding. Ordering state must be where application state cannot alter it (Structural 8).

**Decision**

One reserved store `__parsley.ordering` per process holds format version, per-channel `fedUpTo`, the causal
frontier, and the hold-back bodies keyed `(channel, position)` for in-order prefix scans. A message delivered in the
same step it arrived never touches the store; `flushHolds()` persists survivors before the step can commit. The store
is changelogged, so restoration follows the host's committed-state rules (Host obligation 5).

**Alternatives**

* Always write-then-delete every message through the store — rejected: doubles changelog traffic for the common
  immediate-delivery case with no semantic gain.
* In-memory holds with re-feed on restart — rejected: the host does not re-feed committed read positions, so held
  messages would be lost outright (Liveness 5 violation; the sabotaged engine proves the tests catch it).
* A second Kafka topic as the hold queue — rejected: another channel, another failure domain, and the store already
  commits atomically with the step.

**Cost**

Held messages are written twice (store + changelog) and bodies live in the changelog until delivered; a large
backlog inflates restore time.

### D18 — Reserved prefixes: `parsley.` for headers, `__parsley.` for stores

**Context**

Structural 5 requires metadata distinguishable by construction from application metadata; 2-safety 1 requires
application state never influence ordering.

**Decision**

Header keys starting `parsley.` are refused on emissions; store names starting `__parsley.` are refused in
declarations. The seam exposes only declared stores, so ordering state is unreachable from application code.

**Alternatives**

* Trust conventions without enforcement — rejected: "distinguishable by construction" means the collision cannot be
  constructed, not that it is discouraged.

**Cost**

A legacy application already using such names must rename.

### D19 — Toolchain: Kafka clients/Streams 3.9.1, Java 21, Maven, JUnit 5

**Context**

The spec fixes brokers ≥ 3.7.0, Kafka Streams as host, Java 21, Maven. Client library versions are open.

**Decision**

Kafka 3.9.1 client and Streams libraries (current 3.x stable; behaviours this design leans on — topic IDs in admin
describe, `StreamTask` position-commit for record-less partitions, `resetOffsets` honouring `none` — were verified
against its sources). JUnit 5 for tests; an embedded KRaft cluster (Kafka's own test kit) for integration tests.

**Alternatives**

* Kafka 4.x clients — rejected for now: same protocol against 3.7+ brokers but a fresh major with removed APIs;
  nothing needed from it. Revisit freely — the adapter surface is small.
* 3.7.x pin to match the broker floor — rejected: client compatibility is backward; newer client fixes are free.

**Cost**

The verified-behaviour notes are per-version; a Streams upgrade re-runs the integration suite to revalidate them.

### D20 — The facts interval is configurable, one second by default

**Context**

D7's punctuator needs a cadence. The spec forbids time in the deliverability decision, not in liveness plumbing.

**Decision**

`ParsleyConfig.factsInterval` (default 1s) sets the punctuation and admin-query cadence. It bounds only how quickly
holds blocked on quiet channels release; no safety property references it.

**Alternatives**

* Hard-coding — rejected: admin load vs. release-latency is a deployment trade-off.
* Adaptive backoff — rejected for v1: cleverness in liveness plumbing buys little and complicates the audit.

**Cost**

A knob an operator can set badly (too low: admin load; too high: release latency). Neither direction is unsafe.

### D21 — A deleted topic settles its remaining positions; dead incarnations are vacuous

**Context**

Structural 13 says a cause can no longer matter when its channel no longer exists. Deliverability must also cope with
deps on channels that are gone: a deleted received topic stops feeding forever; a recreated topic is a new channel
(D2).

**Decision**

Topic existence is checked by topic ID; `UnknownTopicIdException` is terminal (IDs are never reused). A dead received
channel's `fedUpTo` becomes +∞ — every remaining position "will never yield a message this process receives" — which
frees dependents while the hold-back buffer still clamps the settled frontier below any yet-undelivered held message.
A dependency naming a dead incarnation's ID is outside the received-channel set and thus vacuous. Frontier entries on
dead channels are pruned.

**Alternatives**

* Blocking forever on deps into dead channels — rejected: pure liveness loss; the liveness premises excuse nothing
  here because the *dependent's* channel is alive.
* Failing closed on received-channel deletion — rejected as the default: deletion mid-run is an operator act on the
  channel, not corrupted state; held messages remain deliverable and everything already received still settles. (A
  spurious "dead" verdict from a lying metadata service would fail closed at the next feed, not deliver wrongly.)

**Cost**

Trusting topic-ID terminality: a broker that reused IDs would break this (none do; KIP-516 guarantees uniqueness).

### D22 — Log starts are attributed to a topic ID only under a describe-after-query identity check

**Context**

Admin `listOffsets` addresses topics by name, but pruning (Structural 13) is by channel = topic ID, and pruning a
cause wrongly is forbidden ("MUST NOT discard any other cause"). Between resolving a name and querying its offsets,
the topic could be deleted and recreated.

**Decision**

`AdminFactsSource` describes by ID, queries offsets by name, then describes by ID again and only attributes a
log-start fact to a channel whose name→ID mapping held across the query. Unconfirmed rounds report nothing — stale
facts are always safe (they only under-prune and under-advance).

**Alternatives**

* Trusting one describe — rejected: the race window is small but the failure (mis-pruning a live cause) is a safety
  violation, and the guard costs one extra metadata call.

**Cost**

An extra describe per facts round; facts lag one round during topic churn.

### D23 — Self-dependency is impossible by construction; adversarial self-deps hold forever

**Context**

Structural 14: a message sent to a channel the process also receives from must not depend on itself or above. And a
hostile peer could craft metadata violating the truthfulness assumption (Assumption 13).

**Decision**

No enforcement is needed for own sends: the frontier contains only positions assigned before the send appends, so its
pairs are strictly below the new offset (offsets assign in append order; expressed positions were assigned earlier —
Structural 12 chains this through peers too). The property is asserted by the simulation oracle on every committed
send. A foreign message that nonetheless depends at-or-above itself simply never satisfies and is held forever with
everything behind it — a fail-closed posture under an assumption breach, not a livelock of the process.

**Alternatives**

* Clamping self-referential deps to (own position − 1) — rejected: silently reinterprets metadata; truthful senders
  cannot produce it, so the clamp would only ever launder corrupt or hostile input into a delivery.

**Cost**

A breach of Assumption 13 on a received channel stalls that channel (visible as a growing hold-back buffer) rather
than producing an operator-facing failure.

### D24 — A step is the host's transaction; several deliveries may share one step

**Context**

*Step* is defined as the host's unit of atomic commitment, "one or more consecutive deliveries". Kafka Streams under
EOS commits on its commit interval, batching everything since the last commit into one transaction.

**Decision**

Parsley adopts the host's transaction as the step and performs any number of deliveries (and their emissions and
state writes) within it, draining to fixpoint on every receipt and fact ingestion.

**Alternatives**

* Forcing one delivery per transaction (`commit.interval.ms` tricks or requestCommit) — rejected: the spec explicitly
  allows multi-delivery steps; single-delivery commits multiply transaction overhead for no criterion.

**Cost**

An abort rolls back a batch of deliveries, so redelivery batches too — all within Safety 2's "rolled back has not
occurred".

### D25 — Structural 16 mechanics: refusal by held-scan; leave/rejoin by retained state

**Context**

The received-channel set may change between executions. Delivered causal past must survive a channel leaving and must
not be re-entered on rejoining; an execution removing a channel with held messages must be refused.

**Decision**

At initialisation the engine scans persisted holds; any hold on a channel outside the new declaration refuses the
execution (fail closed; the store is untouched, so restoring the channel un-refuses it). `fedUpTo` and frontier
entries are retained for channels that leave (never dropped). A rejoining channel resumes from its committed offsets;
if those expired, the pre-commit of D9 re-establishes a position and the session-floor dedupe of D10 drops any
replayed already-delivered messages — the past is not re-entered.

**Alternatives**

* Garbage-collecting state for departed channels — rejected: exactly the "dropped causal past" Structural 16 forbids,
  and it breaks rejoin dedupe.

**Cost**

State for long-departed channels lingers (bounded by channels ever received; prune-on-channel-death still applies).

### D26 — Assumptions taken, and what breaks if one proves false

**Context**

Every SPEC assumption relied on is a choice (Structural 20).

**Decision**

All sixteen are taken. The load-bearing ones and their failure modes:

* **A2 (channel = topic-partition with recreation-proof identity)** — carried by topic IDs (D2). If IDs were reused,
  dead-incarnation vacuity (D21) could misfire; no broker reuses them.
* **A5 (process = task, lifetime across assignment/revocation/restart)** — if Streams resumed a task without full
  re-initialisation, the session floor (D10) and engine rebuild would be wrong; Host obligation 4 promises it never
  does, and an abort-and-continue host is exactly what the simulator refuses to model.
* **A10 (retention covers consumer lag)** — if false, a cause ages out before its effect delivers; Safety 8 machinery
  (D9) turns that into fail-closed, not misdelivery.
* **A13 (metadata expresses causes truthfully)** — if false, under-expression misorders deliveries at other processes
  (undetectable locally, by design of the model); over-expression or self-reference stalls (D23). Nothing local can
  strengthen this.
* **A15 (position facts queryable; not exchange)** — the whole facts design (D6, D22) stands on it. Without it,
  Structural 1 would forbid the admin queries and Liveness 3 would be unsatisfiable here.
* **A16 (logic pure through the seam)** — effects that bypass the seam are invisible to causal order and outside
  every guarantee; the seam (D12) is shaped so bypassing requires deliberate effort.
* A1, A3, A4, A6–A9, A11, A12, A14 are taken as the plain Kafka mappings they state; each failing would mean the
  substrate is not Kafka as specified.

**Cost**

Recorded per item above.

### D27 — Naming: `candidate.parsley:parsley`, packages `candidate.parsley.*`

**Context**

The artefact needs coordinates; the existing implementation's coordinates must be neither consulted nor collided
with (AGENTS.md restriction 1 — nothing of the existing repository, docs, or artefact was read for this build).

**Decision**

Maven `candidate.parsley:parsley`, packages `candidate.parsley.core` (host-independent protocol),
`candidate.parsley.api` (public surface), `candidate.parsley.kafka` (Streams adapter).

**Alternatives**

* Reverse-DNS of a real domain — none exists for this candidate; inventing one implies ownership.

**Cost**

A rename ripples through packages if this ever publishes for real.

### D28 — Emissions always carry the causes header, even when empty

**Context**

Safety 6 makes headerless messages cause-free and immediately deliverable, so an empty frontier could legally omit
the header.

**Decision**

Always attach. A constant invariant ("every parsley-sent message carries the header") is testable, keeps provenance
on the wire, and removes an if-branch from every reader of the format.

**Alternatives**

* Omit when empty — rejected: saves ~25 bytes on the rare cause-free send at the price of a weaker invariant.

**Cost**

A few bytes on cause-free messages.

### D29 — Null keys and values bypass serdes; tombstones pass through

**Context**

Safety 4: no value required where the application sent none. Serde implementations disagree on null handling.

**Decision**

Null in is null out, on both paths, without invoking the application's codec: a null-key or null-value record
delivers as null; a null emission field produces a null record field.

**Alternatives**

* Delegating null to serdes — rejected: several registry serdes throw or write non-null bytes for null, silently
  breaking tombstone semantics downstream.

**Cost**

An application whose serde deliberately materialises nulls (rare, e.g. null-object encodings) loses that behaviour
at the seam.

### D30 — Diagnostics: the decision unit reports its blockers

**Context**

Structural 7 requires the decision be separately callable; nothing specifies its output shape beyond the verdict.

**Decision**

`Deliverability.decide` returns either `Deliverable` or `Held(blockers)` where each blocker names the channel, the
required position and the settled position. Pure data out — no logging, no side channel — so tests and operators see
*why* a message waits.

**Alternatives**

* A bare boolean — rejected: every debugging session would reimplement the explanation from state dumps.

**Cost**

None of substance; the type is two records.

### D31 — A joining channel starts above the delivered causal past

**Context**

Found by the adversarial audit (docs/audit/): a process that delivers an effect while the cause's channel is outside
its received set (the dependency is vacuous there — Liveness 4) and later gains that channel would deliver the cause
*after* the effect, violating Safety 1's whole-lifetime order. Structural 16's "causal past a process has already
delivered … MUST NOT be re-entered when a channel joins" addresses exactly this, but needs a mechanism.

**Decision**

The engine now maintains, separately from the send-frontier, a persisted *delivered causal past* per channel: the
maximum of delivered positions and of the causes expressed by delivered messages — never causes learned only from
still-held metadata. At initialisation, each received channel's `fedUpTo` is raised to its delivered-past entry
before the session floor is taken, so positions at or below it are dropped as already-covered rather than delivered.
Entries prune on the same terms as frontier pairs (below log start, or channel dead).

**Alternatives**

* Using the send-frontier as the clamp — rejected as unsound: the frontier includes causes of messages received but
  still held; clamping with it would falsely settle positions a held message is still waiting for and deliver an
  effect before its cause.
* Blocking the join (refusing executions that add a channel named in past metadata) — rejected: needless
  unavailability; the spec explicitly sanctions skipping ("MUST NOT be re-entered"), not refusing.
* Doing nothing and reading Safety 1 per-execution — rejected: Safety 1 says delivery order "is judged over its
  whole lifetime rather than per execution".

**Cost**

One more persisted long per channel ever implicated in delivery. Messages below the delivered past on a joining
channel are consciously never delivered there — that is the specified trade.

### D32 — A channel is declared dead only after consecutive confirming rounds

**Context**

Found by the audit: Kafka's unknown-topic responses are retriable (metadata propagation), but `AdminFactsSource`
treated a single unknown-by-ID describe as terminal death. A wrong death verdict prunes frontier causes — forbidden
("MUST NOT discard any other cause", Structural 13) — and, on a received channel, settles positions that may still
arrive.

**Decision**

Death requires the unknown verdict to hold for three consecutive fact rounds for that topic ID; below the threshold
the round reports nothing about the channel. Stale or withheld facts are always safe (they only delay pruning and
release). The engine additionally fails closed if a channel recorded dead is ever fed again (topic IDs are never
reused), so even a wrong verdict can never cause a silent skip — it degrades to a loud stop.

**Alternatives**

* Trusting one response — rejected: the failure it invites is a safety violation, the guard costs only latency.
* Distinguishing error codes finer (retriable vs. not) — rejected: the streak is robust to misclassification in
  either direction and simpler to audit.

**Cost**

Dead-channel release and pruning lag up to three fact intervals.

### D33 — Topic names are bound to channel identity in ordering state; recreation refuses

**Context**

Found by the audit: consumer-group read positions are keyed by topic *name* and survive topic deletion and
recreation, while the recreated topic is a different channel (Assumption 2, D2). At startup the runtime skipped
pre-commit for names with existing offsets, so a recreated topic with enough records would be resumed mid-log —
silently skipping retained messages of the new channel, which Safety 8 exists to forbid, with neither
`auto.offset.reset=none` nor the log-start check firing.

**Decision**

The engine records, in ordering state, the channel identity first seen under each declared topic name. An execution
whose declaration resolves a bound name to a different identity fails closed (`CHANNEL_IDENTITY_CHANGED`): the
name's group offsets belong to a dead channel and cannot be trusted; an operator must reset state and offsets
deliberately. Residual exposure: a process's *first-ever* execution against a group that already holds offsets for a
recreated name (application-id reuse) cannot be distinguished locally and is documented as operator responsibility.

**Alternatives**

* Provenance in group-offset metadata — rejected: Kafka Streams overwrites offset metadata with its own on every
  commit, so the stamp would not survive normal operation.
* Runtime-level detection only — rejected: the runtime cannot see task-level state, and the store already survives
  exactly as long as the progress it must vouch for.

**Cost**

One small store entry per declared name. Recovering from a deliberate recreate-and-replay now requires an explicit
state/offset reset instead of silently resuming — which is the point.

### D34 — No deliveries from within processor initialisation

**Context**

Audit nit: draining from `Processor#init` could forward emissions from inside initialisation, a Kafka Streams path
nothing exercises; were a Streams version to reject it, a restored hold whose causes were satisfied by init-time
facts would crash-loop the task at startup.

**Decision**

`init` only seeds facts into the engine. Draining — and therefore delivering and forwarding — happens exclusively in
`process()` and punctuation, both well-trodden host paths. Cost: a message released purely by init-time facts waits
at most one facts interval.

**Alternatives**

* Keeping the init-time drain — rejected: an unexercised host path on the delivery route buys at most one interval
  of latency.

### D35 — A read_committed probe settles trailing never-yielding runs; corrects D6

**Context**

The adversarial audit refuted part of D6: Kafka Streams commits a partition's consumer position only for partitions
in the task's `consumedOffsets`, which is empty for a partition with no processed record in the *current* task
lifetime. After a restart, a held message whose cause names a trailing aborted run therefore stalls forever — the
committed-offset report never covers the run (Liveness 3), and a later retention pass over it turned into a spurious
fail-closed. D6's "verified in the Streams 3.9.1 sources" was true only of the same-lifetime case the original
integration test exercised.

**Decision**

The facts source gains a probe: a group-less read_committed consumer that, while the process holds messages, seeks
each received channel to just above the engine's fed-or-never frontier and polls once. The first real record's
offset — or the consumer position having advanced past aborted batches when the poll returns nothing — bounds a run
of positions that will never yield a message, and is reported like any read-position report. This is querying the
substrate for position facts (Assumption 15), not exchange between processes; the deliverability decision stays pure.
Probing is skipped entirely when nothing is held. `EndToEndIntegrationTest#causeOnTrailingAbortedRunResolvesEvenAfterARestart`
pins the exact restart case that stalled.

**Alternatives**

* Relying on the host's committed offsets alone (original D6) — refuted as above; the liveness premise excuse would
  have made a routine restart a permanent stall, which is within the implementation's power to avoid.
* Broker end offsets (LSO) as the report — still unsafe for the reasons D6 records: the LSO covers real records the
  host has not yet fed.
* A persistent probing group with its own committed offsets — needless: probe results feed ordering state, which is
  already durable.

**Cost**

One extra consumer per process application; up to one short poll per blocked channel per facts round while messages
are held.

### D36 — When prior state exists, missing group offsets restart from earliest

**Context**

Audit finding: group offsets can expire while a process is stopped. The declared initial position (possibly LATEST)
was applied to any partition without committed offsets — on an expiry restart that pre-commits the log end, silently
skipping retained unread messages, observable as a restart that changes what is delivered (2-safety 2).

**Decision**

The declared initial position applies only to a genuinely first start. The runtime detects prior executions by the
existence of the process's ordering-store changelog topic; with prior state, missing offsets are re-established at
*earliest* — re-fed already-delivered messages are dropped by the engine's session floor (D10), and unread retained
messages are fed rather than skipped. The concurrent-bootstrap overwrite variant of the finding is fenced by the
broker itself: offset alteration on a non-empty group is rejected.

**Alternatives**

* Honouring the declared LATEST on every start — the refuted behaviour.
* Persisting "first start done" in group-offset metadata — overwritten by Streams' own commits (same reason as D33).

**Cost**

An expiry restart with LATEST channels re-feeds history (dropped as duplicates) instead of skipping — slower, never
lossy. One extra describe per process per start.

### D37 — Exception handlers are unoverridable configuration

**Context**

Audit finding (Structural 9/19): `ParsleyConfig.streamsProperty` accepted Kafka Streams' processing, deserialization
and production exception handlers; a continue-style handler converts failing closed into dropping a message or an
emission and committing the step anyway — a documented public path to violating Safety 3/7 and Structural 19.

**Decision**

The three handler config names join the unoverridable set, suffix-matched under any prefix. The deny-list posture
(rather than an allow-list) is retained deliberately: the owned set is the configs that bear on the guarantees, and
each addition is recorded; an allow-list would make every harmless broker/tuning knob a compatibility decision.

**Alternatives**

* Allow-listing known-safe keys — safer in principle, rejected for the maintenance surface; revisit if the deny-list
  grows again.

**Cost**

Applications cannot install custom handlers even for logging; they observe failures through the uncaught-exception
path instead.

### D38 — The runtime refuses stranded held messages by reading the changelog

**Context**

Audit finding (Structural 16): the engine's refusal runs at task initialisation, but removing a topic can shrink the
task set so that the task holding messages on the removed channel is never instantiated — the refusal never fires and
the held messages are silently stranded.

**Decision**

At startup, when prior state exists, the runtime reads the process's ordering-store changelog end to end, compacts it
in memory to latest-value-per-key, and refuses the execution (`CHANNEL_REMOVED_WITH_HELD_MESSAGES`) if any live held
entry names a channel outside the new declaration. The interpretation of store keys is the public, pure
`OrderingStateInspector` (unit-tested); the engine-side refusal remains as the second line.

**Alternatives**

* Keying held state in a runtime-readable side topic — a second write path to keep atomic with the step; the
  changelog already is that topic.
* Documenting the gap — rejected: a silently stranded message is a liveness loss invisible to the operator, the
  worst failure shape this project has.

**Cost**

Startup reads the whole (compacted) changelog once per process; startup latency grows with held backlog.

### D39 — Received topics may have unequal partition counts

**Context**

Audit finding (Structural 20): an implemented open choice was unrecorded. Kafka Streams builds tasks per partition
number across a subtopology's topics; a topic with fewer partitions simply has no partition *i* for high-numbered
tasks.

**Decision**

Declarations may mix partition counts. A task's received-channel set contains partition *i* of exactly those declared
topics that have a partition *i* (`ParsleyProcessor.init` filters against resolved partition counts); the engine and
all guarantees operate on that per-task set. Assumption 14 (a declaration stands for the induced processes) is what
makes this a faithful reading.

**Alternatives**

* Requiring equal partition counts (co-partitioning) — rejected: the spec never asks for it (joins are explicitly
  out of scope, Assumption 11), and it would refuse valid arrangements.

**Cost**

Operators must remember that cross-topic partition alignment is positional; a key present in topic A partition 2 and
topic B partition 0 lands in different processes — ordinary Kafka behaviour, now recorded.

### D40 — Correction to D32's blast-radius claim

**Context**

D32 claims "even a wrong verdict cannot silently discard — it degrades to a loud stop". The audit showed the claim
holds only for *received* channels (the dead-channel feed guard): for a cause channel the process does not receive,
a wrong death verdict silently prunes frontier entries, and later sends under-express those causes (Structural 15).

**Decision**

The claim is corrected, not the mechanism: the three-round debounce (D32) remains the guard for non-received cause
channels, and the residual — broker metadata reporting a live topic ID as unknown for three consecutive fact rounds —
is accepted and recorded as substrate-breach territory. EVIDENCE row Structural 13 states the split honestly.

**Alternatives**

* Never pruning non-received channels on death alone — rejected: Structural 13 requires a means of discarding causes
  on channels that no longer exist, and without death-pruning such entries are immortal.

**Cost**

None beyond honesty; the residual risk window is unchanged, now correctly described.

### D41 — The simulation oracle observes the feed; owed delivery is excused only by ground-truth delivered past

**Context**

The hardening assessment (ASSESSMENT 3.1) proved the suite could not see omission: the oracle learned of a receipt
only when the engine returned ACCEPTED, and the sim's read position advanced before `onReceive`, so an engine that
consumed a position and silently discarded its message erased the evidence of the loss — a drop-every-position-3
mutation left all 380 tests green. At the same time, a fed-but-undelivered message is not always a violation: the
join clamp (D31) is *required* to drop re-fed positions at or below the delivered causal past (Structural 16), and
operator rewinds legitimately re-feed delivered messages.

**Decision**

`SimProcess` reports every fed message to the oracle before the engine sees it, and the causes of everything fed
join the causal past whether or not the engine keeps the message (happened-before passes through receipt —
Structural 15 — so the oracle now owns the property the unit test
`causesOfAJoinClampDroppedMessageStillBindSends` pins). A message fed in a committed step is owed delivery unless
its position is at or below the per-channel maximum of the process's delivered causal past — delivered positions
and the *true* causes of delivered messages — snapshotted at the start of the execution that fed it. The snapshot
timing matters: a message dropped in the same execution that could have delivered it is never excused. The
SILENT_DROP sabotage mode re-applies the assessment's mutation permanently; its targeted meta-test and sweep margin
prove the catch.

**Alternatives**

* Keeping acceptance-gating and pinning drops with unit tests — rejected: the oracle's whole value is judging the
  engine from outside; a unit test enumerates drops someone thought of, the feed-observing oracle catches the ones
  nobody did.
* An instance-set exemption (excuse exactly the instances the oracle knows delivered or in causal past) — rejected:
  D31's clamp is deliberately a per-channel *summary*, so it also drops concurrent gap-fillers below the maximum;
  the set-based exemption flagged those sanctioned drops immediately (property seeds 43, 261, 292 while building
  this).
* Deriving the exemption from expressed metadata, as the engine's own clamp does — rejected as circular
  (constraint 4.5): an over-expressing engine would inflate its own excuse. Ground truth and expression coincide in
  random runs because the generator's compliant senders express exact positions.

**Cost**

The oracle's exemption is tighter than the engine's clamp wherever upstream expression legally rounds a cause up.
A future targeted scenario combining legal round-up with a joining channel would flag the engine's sanctioned drop
as a liveness loss; if that shape is ever wanted, this exemption must be widened deliberately rather than silently.

### D42 — The oracle bounds expression from above, judged at send time

**Context**

ASSESSMENT 3.4: the oracle checked under-expression, unassigned positions and self-dependency, but never compared
expressed pairs against true causes from above — merging `fedUpTo` (assigned non-causes) into every emission stamp
survived all 300 property seeds. Over-expression is not a wire-format violation, but it feeds downstream
delivered-past clamps (D31), silently widening what joining channels will never deliver, and Structural 12 was
checked against `lastAssigned` at *commit* time, so appends interleaving between send and commit excused
genuinely-future expressions.

**Decision**

The oracle tracks, per process, the highest position per channel among (a) instances the process delivered and
(b) metadata pairs of everything fed to it — its legitimate expression inputs, mirrored from ground truth. Every
committed send is checked against this bound and against the world's last assigned position per expressed channel,
both snapshotted at the moment of sending. Expressing any pair above either bound, or on a channel with no bound at
all, is a violation. The OVEREXPRESS sabotage mode re-applies the assessment's mutation permanently; a targeted
meta-test and the sweep margin (96 of 120 seeds) prove the catch.

**Alternatives**

* Exact-equality against the engine's expected frontier — rejected: expressing a position at or above a cause's is
  legal (Terminology: *Express*), and received pairs legally rounded up by upstream *must* be re-expressed as
  received (Structural 15), so equality is not a spec property.
* Deriving the bound from the engine's own emissions — rejected as circular (constraint 4.5).

**Cost**

The bound inherits upstream's legal round-up, so an origin's inflation is caught at the origin while a faithful
downstream re-expression is not flagged — correct, but it means the check's power concentrates where the lie is
born, not where it propagates.

### D43 — The generator reaches refusals; the harness survives them; catch margins are asserted per mode

**Context**

ASSESSMENT 3.2: the random generator could not produce truncation, channel death or declaration changes — the
truncation check, both prune arms and dead-channel settling were dead code across all 300 seeds, and the sabotage
sweep had provably zero margin for three of its eight modes. Worse, a mid-run fail-closed aborted the entire run
without final checks, so widening the vocabulary would have waived every check in exactly the runs that exercise
refusals.

**Decision**

Four vocabulary additions (log truncation biased toward the committed-read boundary and one past it; whole-topic
deletion, restricted to topics no process sends to; declaration changes whose restart may be refused; external
producers attaching undecodable headers), plus multi-partition topics sharing one topic UUID so ChannelId ordering
and store prefix scans see adjacent channels. A `ParsleyFailClosedException` out of any engine-driving operation
aborts the step with crash semantics and records the refusal; the run continues. Safety checks are never waived;
liveness checks are waived only for a process that failed closed or has a received channel wedged below the
earliest retained position (there the host cannot read — the liveness premise fails, as `auto.offset.reset=none`
stops the real runtime). Quiescence never restarts a refused process (a refusal recurs identically); the random
loop may, after possibly changing the declaration. Two refusal obligations are judged from world truth at
quiescence: positions discarded beyond a process's covered position (its high-water committed read, or its
delivered-past clamp) require a recorded refusal, and no process may commit a read past an undecodable header. The
sabotage sweep asserts a per-mode catch margin with floors set at half the values measured when built
(IGNORE_CAUSES 39, NO_FIFO 11, REDELIVER_REFEEDS 81, UNDECODABLE_AS_ABSENT 64, SKIP_RECEIPT_MERGE 72, DROP_HELD 62,
IGNORE_TRUNCATION 35, IGNORE_REMOVED_CHANNELS 27, SILENT_DROP 33, OVEREXPRESS 96, of 120 seeds).

**Alternatives**

* Modelling the consumer's out-of-range kill as a host-level process death — rejected: it downs honest and
  sabotaged engines alike, collapsing IGNORE_TRUNCATION's margin to the diagnosis alone; keeping the wedge as a
  stalled-but-alive state lets the obligation check separate an engine that refused from one that sailed on.
* Judging the truncation obligation from the engine's `fedUpTo` — rejected as circular; the high-water committed
  read plus the oracle's ground-truth clamp is host-side truth and tolerates operator rewinds.
* `caught > 0` sweeps — rejected: margin is bounded by generator shape, not seed count; a collapsed margin must
  fail the build even while a targeted meta-test still passes.
* Killing topics some process still sends to — rejected for now: emissions to dead topics would need a sender-side
  failure model the protocol layer does not define; revisit when the runtime's dead-send behaviour (1.4/1.2 work)
  is settled.

**Cost**

Floors are tied to the seed range and generator shape: any future generator change must re-measure and re-set
them, and the wedge waiver deliberately forgoes liveness coverage in wedged runs (measured and accepted — the
margins above are with these waivers in force).


### D44 — Channel death needs corroboration; recreation is affirmative; the debounce is time-based

**Context**

The hardening assessment refuted two beliefs this log recorded. First, D32/D40 treated `UnknownTopicIdException`
from a describe-by-id as conclusive after three rounds, calling a live id describing unknown that long
"substrate-breach territory" — demonstrated false on a real broker: a DENY-Describe ACL makes the broker answer
describe-by-id with unknown-topic (masking existence), while describe-by-name answers with an authorization error.
An ACL change is routine operations, and under the old mechanics it permanently discarded a live cause
(ASSESSMENT 1.2). Second, the three-"round" debounce was pooled across every task of the application — one facts
source serves them all — so at N tasks the window shrank to interval × 3/N, and one task's live round reset a
streak another built. Separately, the feed path's name-based subscription adopts a recreated topic's records under
the old identity mid-run, with every existing guard startup-time or rounds-delayed (ASSESSMENT 1.1).

**Decision**

Three rules, one evidence model (`AdminFactsSource`, `PositionFacts.recreatedChannels`, engine `onFacts`):

* **Recreation is affirmative evidence, acted on immediately.** An id's last-known name resolving to a *different*
  id proves the old topic dead: ids are never reused, and a stale metadata view can serve an old binding but
  cannot invent a new one. A received channel so recreated fails the process closed mid-run
  (`CHANNEL_IDENTITY_CHANGED`) — the feed path can no longer be trusted to carry the old channel; a frontier
  channel prunes at once, with no debounce.
* **Death is absence of evidence, so it needs corroboration and a debounce.** An id is dead only when
  describe-by-id is unknown *and* its last-known name is gone too (or bound to another id), continuously for
  3 × factsInterval of monotonic-clock time. `TopicAuthorizationException` on the name is denial, never death:
  the timer resets, the round reports nothing about the channel, and — settling the assessment's open question —
  a denial-masked received topic never reaches the end-of-channel sentinel, so held messages are not released
  against positions that may still arrive. An id with no known name has nothing to corroborate against and
  keeps the time-debounce alone, at a four-fold window: it may be foreign injection, but it may equally be a
  legitimately new topic whose id reached this process through upstream metadata before the admin client's view
  caught up, and wrongly killing a live cause costs more than letting a foreign id linger. The four-fold window
  also charges a routine restart: name bindings live only in memory, so a frontier cause whose topic was deleted
  while the process was down comes back as an id with no known name, and its death confirmation waits the
  four-fold window where a running process would have used the single one. Accepted: the cases are
  indistinguishable at runtime without persisting name bindings, and the cost is bounded settle latency, never
  safety.
* **The debounce is time-based on a monotonic clock, shared deliberately.** Time-based measurement makes the
  window independent of task count, and a live sighting by any task rightly resets it for all — the topic
  evidently exists. The clock is monotonic (nanoTime-derived), never the wall clock: a stepped wall clock would
  collapse the window into a premature dead verdict whose end-of-channel settle is persisted and survives
  restarts (review finding S6). Time here is liveness plumbing, like D7's punctuator: the deliverability
  decision still never sees it. Verdicts are sticky — recreation in particular is re-reported every
  round, so every task's engine performs its own refusal regardless of which round first detected it, and a
  round lost between threads loses nothing (review findings: the one-shot verdict downgraded other tasks'
  refusals to silent settles) — and tracking state is evicted only when no task has asked about an id for
  several confirmation windows, never against a single caller's set (whose per-task frontiers would erase each
  other's timers). Death is sticky but not final: a declared id keeps its name binding past death, and the name
  is re-checked each round, so a topic recreated *after* its death was confirmed is upgraded to the recreation
  verdict rather than reported dead forever — without the upgrade the engine would settle the channel and then
  die on the new incarnation's feed with a feed-order diagnosis instead of the identity refusal and its remedy.

Tracking state is bounded by time, never by any one caller's view: an id's evidence and verdicts are evicted
only when *no* task has asked about it for a generous horizon (eight confirmation windows, floored at five
minutes) — trimming against a single caller's topic set would let tasks with differing frontiers erase each
other's timers and verdicts — and the declared topics' ids are pinned outright, since their name bindings are
the only evidence that can classify a later recreation as recreation while a task sits out a long rebalance.
This supersedes D32's round-counting mechanics (the debounce intent stands) and corrects D40: the
residual it accepted as substrate-breach territory was reachable by routine operations and is now closed by
corroboration. Verified against a real StandardAuthorizer
(`IdentityIntegrationTest#deniedDescribeOnAFrontierTopicDoesNotPruneItsCause`,
`#deniedDescribeOnAReceivedTopicDoesNotReleaseHeldMessages`,
`#midRunRecreationOfAReceivedTopicStopsTheProcess`).

**Alternatives**

* Trusting the by-id describe with more rounds — rejected: no round count distinguishes masking from death; the
  by-name answer does, at one extra describe per round for unknown ids only.
* Failing closed on denial — rejected: denial is a permission state an operator can revert; refusing would turn a
  transient ACL mistake into an outage, and stale-safe silence (report nothing) already protects order.
* Per-task facts sources to fix the pooled streak — rejected: multiplies admin clients and probe consumers per
  task for no evidence gain; the shared, time-based timer gets the semantics right at no cost.
* Delivery-gating every record on a describe-after-fetch identity confirmation — rejected: adds up to one facts
  interval of latency and a hold-persistence round trip to every message to close a window that only a
  delete-and-recreate of an actively-consumed topic opens. The residual — records fetched between recreation and
  the next facts round are adopted under the old identity — is bounded by the facts interval plus metadata
  propagation, ends in the fleet failing closed as confirmations land, and the restart path was already refused
  (D33). Recorded honestly rather than papered over; the substrate offers no per-record identity (consumer
  records carry no topic id, and both incarnations start at leader epoch 0).

**Cost**

One describe-by-name per round covering the currently-unknown ids; recreation and death verdicts lag the facts
interval; the sub-interval adoption window above.

### D45 — An in-execution feed regression fails closed, wherever it lies relative to the session floor

**Context**

D10 dropped any feed at or below the session floor as a replay of a committed past. The assessment's recreation
probe showed the same silent-drop path swallowing a *new incarnation's* records (new messages at new offsets 0–4
dropped as duplicates of the old channel's floor, ASSESSMENT 1.1) — and within one execution, Host obligation 1
promises increasing position order, so any regression is a host/substrate breach, not a replay.

**Decision**

The engine tracks the highest position fed per channel within the current execution (in-memory by design — the
execution's feed order is the thing being checked). A feed at or below it fails closed (`OUT_OF_ORDER_FEED`).
D10's silent replay-drop now applies only to in-order feeds at or below the floor — the shape a genuine
post-restart replay actually has. Pinned by
`ProcessEngineTest#inExecutionFeedRegressionFailsClosedEvenBelowTheSessionFloor`.

**Alternatives**

* Keeping the unconditional below-floor drop — rejected: it converts detectable identity confusion and host
  breaches into silent message loss; obligations say detection must fail closed.
* Persisting the last-fed position — rejected: across executions the host legitimately re-feeds (rewinds,
  expiry); only the in-execution order is promised.

**Cost**

None in the legitimate paths: replays arrive in order at execution start and still dedupe silently.

### D46 — A dead channel with undelivered held messages fails the process closed; corrects D21

**Context**

D21 let a dead received channel settle (`fedUpTo` to the end-of-channel sentinel) with held messages still
deliverable "as their causes settle". The assessment demonstrated the hole (ASSESSMENT 1.4, SPEC Safety 9), and
this tree's widened property generator then reproduced it live (seed 134) before this fix: an upstream process
that delivered from the dead channel legally prunes it from its metadata (Structural 13), its next emission
expresses nothing about it, and a downstream process still holding an undelivered message from that channel
delivers the effect past the held cause — a lifetime causal-order inversion with no local failure anywhere.

**Decision**

When a facts round reports a received channel dead while its hold-back buffer is non-empty, the engine fails
closed (`CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES`): the held messages' place in causal order cannot be
preserved locally once senders may have pruned, and Safety 9 makes the detected case binding. The refusal recurs
identically on restart (hold and verdict are both durable) until an operator intervenes. With nothing held from
it, a dead channel settles exactly as before (D21's liveness reasoning stands). The undetectable half — the
effect arriving where the dead channel's message was never received — remains excused by Assumption 17, as D26
treats Assumption 13.

**Alternatives**

* Retaining dead-channel pairs in expression instead (never prune while anyone might hold) — rejected: retention
  is only unbounded at the origin, which re-expresses and re-seeds every downstream copy forever (the
  assessment's constraint 4.1); the fail-closed resolution avoids the cost entirely and is the minimal mechanism
  the entry names.
* Delivering the dead channel's holds but quarantining other channels' arrivals behind them — rejected: ordering
  arrivals against the holds requires exactly the expression the dead verdict lets senders discard; there is no
  sound local rule.

**Cost**

Topic deletion under live holds — an Assumption 17 breach by the operator — now stops the process loudly instead
of degrading silently. On the real host the engine's refusal races Kafka Streams' own death on the deleted source
topic (the rebalance triggered by the metadata change fails with a missing-source-topic error), and the foreign
failure usually wins; either way the process stops before delivering past the holds, and the engine-level refusal
is the guarantee the simulator and unit tests pin. Pinned at engine level
(`ProcessEngineTest#deadReceivedChannelWithHeldMessagesRefusesRatherThanSettling`), scenario level
(`TargetedScenarioTest#effectFromPrunedDeadChannelCannotDeliverPastItsHeldCause`), and by the oracle meta-test
(`SabotageMetaTest#deliveringPastDeadChannelHoldsInvertsCausalOrderAndTheOracleSeesIt`).

### D47 — The bootstrap scan diagnoses identity change before judging removal

**Context**

`refuseStrandedHeldMessages` compared held channels against the *current* resolution's ids. A topic deleted and
recreated under a still-declared name across a restart therefore misreported `CHANNEL_REMOVED_WITH_HELD_MESSAGES`
— "which the new declaration no longer receives" — though nothing was removed, prescribing a declaration fix
where D33's deliberate reset is the remedy (ASSESSMENT 1.3).

**Decision**

The ordering state's name-to-identity bindings (D33) travel in the same changelog the scan already reads.
`OrderingStateInspector` interprets them (`nameBindings`, `identityChangedTopics`, pure and unit-tested), and the
runtime refuses with `CHANNEL_IDENTITY_CHANGED` before any held-versus-declared comparison; only then are
genuinely stranded channels judged removals. Bindings are compared by topic id: every task binds the same name to
its own partition, so the aggregated changelog's binding value is per-task, but the topic id — the identity that
matters — agrees across them.

**Alternatives**

* Comparing held ids against bound names inside the stranded check — rejected: the pre-check subsumes it, fires
  even with nothing held (earlier, cheaper than the engine-level D33 refusal at task initialisation, which
  remains as the second line), and keeps the stranded check's logic untouched.

**Cost**

One more pure interpretation of the same compacted changelog view. Pinned by
`OrderingStateInspectorTest#recreationUnderAStillDeclaredNameIsAnIdentityChangeNotARemoval` and
`IdentityIntegrationTest#recreationAcrossARestartIsDiagnosedAsIdentityChangeNotRemoval`.

### D48 — Initial positions are committed through group membership, never by admin alteration

**Context**

ASSESSMENT 1.5: `alterConsumerGroupOffsets` succeeds against any empty group, so a bootstrap paused for an
arbitrary duration (Fault model 2) can wake after a newer lifetime has bootstrapped, processed, committed and
stopped — the group is empty again — and overwrite the newer offsets with stale ones. With a LATEST channel the
stale bootstrap commits the current log end, silently skipping retained unread messages (2-safety 2, Safety 8).
Kafka offers no conditional alter; the broker premise the assessment demonstrated is that commits made *through
group membership* are generation-fenced while admin alters are not.

**Decision**

`GroupMembershipCommitter`: the bootstrap joins the process's own group as a consumer and performs the whole
read-compute-commit sequence inside that one membership — subscribe (partitions paused and explicitly positioned
on assignment, so no fetch happens and no reset policy exists to consult), read the committed offsets, compute the
missing initial positions, `commitSync` the explicit map, leave. A pause long enough for another lifetime to
interleave either outlasts the session (the member is fenced out; the commit throws with nothing written) or has
its generation bumped by the newer joiner (the commit is rejected). A *live* Streams member makes the join fail on
the assignor protocol mismatch, refusing the bootstrap outright — correct, since another lifetime is running. The
committer's session and poll-interval timeouts default to seconds, not the consumer's five minutes, so a crashed
bootstrap never holds up a successor for long (Operational 2). Every future pre-commit site must use this helper —
the assessment's inheritance clause is recorded here as a standing rule.

**Alternatives**

* Re-validating and re-listing around the alter — rejected: the pause can land between any two statements
  (Fault model 2); the window shrinks but never closes. The mechanism had to change.
* A transactional offset commit (producer `sendOffsetsToTransaction`) — rejected: it also requires group metadata
  from an active membership to be fenced, at which point the plain membership commit is the same fence with less
  machinery.

**Cost**

The bootstrap participates in group membership when — and only when — an admin-side read shows offsets missing:
a closed Streams application's members linger in the group until their session times out (Streams does not leave
on close), so an unconditional join would collide with them on every quick restart. The read-only fast path
writes nothing, so it needs no fence; whenever a commit will happen, the authoritative read-compute-commit runs
inside the membership. When offsets are missing *and* Streams members linger, the join fast-fails on the group's
protocol rather than waiting, so the committer retries inside its deadline until the lingerers age out — a
genuinely live application keeps refusing and the bootstrap fails, correctly (review finding S5 corrected this
record's earlier wait-them-out wording). Residual, recorded: several instances of one application cold-starting
concurrently can collide the other way — one instance's Streams join meeting another's still-open committer
membership fails that Streams client fatally; the window is the seconds of a bootstrap with missing offsets, and
a supervisor restart recovers (review finding S1). Verified by forcing a generation change between the read and
the commit against a real broker (`BootstrapIntegrationTest#staleBootstrapCommitIsFencedByGroupMembership`);
D36's expiry rule is now pinned end-to-end by `#expiredOffsetsRestartFromEarliestNotTheDeclaredLatest`.

### D49 — A task-width change against prior state is refused at start; names the Streams width validation

**Context**

ASSESSMENT 1.6: the ordering store's changelog is created with one partition per task and Kafka cannot change a
topic's partition count downward (or migrate a changelog's task mapping); the task count follows the declaration's
widest received topic. A legal shrink of the received-channel set therefore started "healthy", delivered nothing
for ~42 s, then died inside Kafka Streams' internal-topic width validation — an unspecified Streams behaviour this
tree previously relied on nowhere and named nowhere (the assessment's constraint 4.8) — with advice to run
StreamsResetter, a remedy that would destroy the ordering store: the state Structural 16 exists to protect.

**Decision**

With prior state, `start()` compares the ordering-store changelog's partition count against the newly induced task
width and refuses any difference (`TASK_WIDTH_CHANGED`) with the accurate condition and remedy: restore the
previous declaration and partition counts, or reset the process's state and group offsets deliberately. The
Streams width validation is hereby named and pinned (`BootstrapIntegrationTest#widthChangingRestartIsRefusedWithTheAccurateDiagnosis`
would fail green if the refusal regressed to the crash-loop). Refusing every width change also keeps the
stranded-holds scan sound — the assessment's constraint on any 1.6 fix: the application never runs at a shrunken
width against prior state, so a held entry committed after one scan's snapshot is always caught by the next
start's fresh scan; the silent Structural 16 window a permissive width fix would re-open never opens.
Width-preserving removals still run, guarded by the scan and the engine refusal.

**Alternatives**

* Letting the shrunken app run by deleting and recreating the changelog at the new width — rejected: destroys
  retained causal past for departed channels (exactly what Structural 16 forbids dropping) and re-opens the
  stranded-scan window as a silent violation.
* Migrating orphaned shards' entries into the surviving shards — rejected: the changelog's partition↔task mapping
  makes the migrated state unreachable again the moment the width changes back; every variant ends in another
  width mismatch.
* Documenting the crash-loop — rejected: a refusal that names the wrong remedy, twice removed, and reports
  healthy while dying is the defect, not a documentation gap.

**Cost**

Width-changing evolutions (growing a topic's partitions, or removing/adding the widest topic) now require a
deliberate reset even when nothing is held. That trade — availability spent on never destroying ordering state —
is recorded as the price of the substrate's rigid changelog widths.

### D50 — Test seams for the bootstrap and facts source: real broker, phase hooks, injected clock, overridable describe

**Context**

ASSESSMENT 3.5: the facts source had no tests at all and the bootstrap none behavioural; both were private and
reachable only through `start()` against a live broker. The seam had to be decided and recorded. Constraint 4.6:
fabricating Kafka admin results pulls toward non-public constructors and `KafkaFutureImpl`; real-broker probes are
the cleaner base.

**Decision**

Four seams, all against the real broker, none touching Kafka internals: the membership committer is
phase-structured (D48) so a test can interleave a competing lifetime between its phases; `AdminFactsSource` takes
its clock by injection (D44), so the dead-confirmation window is driven deterministically; its `describeByIds` is
package-visible and overridable, so a test subclass can lie on exactly the confirming describe and prove the D22
race guard withholds attribution; and the runtime-level checks (stranded scan, width refusal, expiry
re-establishment) are asserted through `Parsley.start` itself throwing — which distinguishes a start-time refusal
from an asynchronous engine failure, so disabling a runtime check cannot stay green. The four suite-blind-spot
mutations the assessment listed (confirming describe deleted; debounce shortened; stranded scan disabled;
priorState ignored) each now fail a named test.

**Alternatives**

* A scripted `Admin` double — rejected: `DescribeTopicsResult` and friends lack public factories
  (constraint 4.6); the override seam gets the same interleaving control with none of the internals coupling.
* Broker-only coverage with no seams — rejected: the debounce and the race guard are timing properties; without
  the clock and describe seams they are untestable deterministically.

**Cost**

`AdminFactsSource` is non-final with one package-visible method; the seams are invisible to the public API.

### D51 — Client interceptors and the timestamp extractor join the unoverridable set; extends D37

**Context**

ASSESSMENT 1.8: `interceptor.classes` (under any prefix) and `default.timestamp.extractor` flowed unfiltered into
the Streams properties. Both have documented uses that break guarantees from outside the record path's owner: a
producer interceptor's documented purpose is mutating records in `onSend` — one that strips or replaces the
`parsley.causes` header makes every emission read cause-free downstream, where Safety 6 then *mandates* immediate
delivery, an effect-before-cause undetectable on either side — and a `LogAndSkipOnInvalidTimestamp`-style
extractor converts a received message into a documented log-and-skip drop whose read position still advances, the
same shape as the continue-style handlers D37 already refuses.

**Decision**

Both names join `FORBIDDEN_SUFFIXES`, refused under any prefix at build time. D37's rule decides membership: the
owned set is the configs whose *documented use* bears on the guarantees. The completeness counterargument —
`extraProperties` can still inject SASL modules, metric reporters, and so on — fails Structural 9's "as
documented" qualifier: those configs' documented use never touches the record path; an interceptor's does.

**Alternatives**

* An allow-list — rejected again on D37's grounds; this is the deny-list's second growth, and a third addition
  should trigger the revisit D37 promised.

**Cost**

Applications cannot install observability interceptors either; they observe through Kafka's own metrics instead.

### D52 — A metadata budget in bytes, enforced on receipt and on emission; corrects D4's attribution

**Context**

ASSESSMENT 2.4: frontier growth has no ceiling short of the substrate's record-size wall (≈37,000 entries at the
default 1 MiB), where the failure is `RecordTooLargeException` → production handler FAIL → permanent stop with no
parsley diagnosis. Nothing in-band can legally shed load: Structural 13 forbids discarding live causes, and
cross-channel compression is unsound — dropping (c₁,p₁) as covered by a retained (c₂,p₂) requires
readers(c₁) ⊆ readers(c₂) across the reachable graph, which partial channel consumption precludes. The assessment
also settled that producer-side limits judge the *uncompressed* size, so compression never moves the ceiling.
(That sender-side impossibility argument is established by the assessment's constraint 4.4, not by D4, whose
record covers the receiver-side ground only — recorded here as the attribution correction the assessment asked
for.)

**Decision**

`ParsleyConfig.metadataBudgetBytes` (default 256 KiB — a quarter of the default wall, ≈9,300 entries, far beyond
any sane topology): the engine fails closed (`METADATA_BUDGET_EXCEEDED`) when a received message's causes header
exceeds it, when the merged frontier's encoded size does (checked where the frontier grows — the codec's size is
affine in the entry count, so the check costs no encode), or when encoding the frontier for emission would. Bytes,
not entries, because the wall is measured in bytes. The merge-site check is the one that makes the promise
whole: the frontier grows by union across messages, so a process that only receives (never sends) — for which
the emission check never runs, and whose every incoming header may individually sit under the budget — still
stops attributably before its persisted frontier balloons.

**Alternatives**

* Shedding oldest causes at the ceiling — forbidden (Structural 13: only no-longer-mattering causes may go).
* Per-channel-count budgets — rejected: the wall is bytes; an entry-count knob mismeasures it.
* No default (opt-in only) — rejected: the wall is a silent default; the diagnosis should be one too.

**Cost**

A frontier legitimately larger than the budget stops the process; the operator raises the budget consciously,
against the growth law now documented in docs/model.md (Operational 5).

### D53 — Frontier size is logged every facts round and surfaced at 80% of budget

**Context**

ASSESSMENT 2.5 / Operational 5: no surface reported frontier size; the first observable symptom of growth was the
terminal stop (or slowing facts rounds).

**Decision**

The engine exposes `frontierSize()` and `frontierBytes()`; the processor logs both at DEBUG every facts round and
warns once when the encoded size crosses 80% of the budget, pointing at the growth law in docs/model.md. The
richer per-process status surface (ASSESSMENT 1.10) will carry the same numbers when it lands.

**Alternatives**

* Kafka metrics integration — deferred: parsley owns no metrics registry yet; logging plus accessors covers
  Operational 5's "observable in operation" without inventing one here.

**Cost**

Operators who scrape metrics rather than logs wait for the 1.10 surface.

### D54 — Position facts are gathered on one background thread; applied on the stream thread

**Context**

ASSESSMENT 1.9: `AdminFactsSource.gather` is synchronized, performs describes and offset queries with 10 s
timeouts plus up to a second of probing per held channel, and ran inside the punctuator — on the stream thread,
against `max.poll.interval.ms`, shared by every task. Its cost scales with the frontier, which an external
producer can inflate (ASSESSMENT 2.3). The assessment's constraint 4.2 established the soundness ground: every
fact is a per-position lower bound, so gathering off-thread and applying late is safe; what is *not* covered is
sharing engine-owned collections across threads, or applying one completed round twice.

**Decision**

One daemon thread per runtime (the facts source serialises rounds anyway) runs `gather`; the punctuator snapshots
the inputs on the stream thread (copied received set, fresh hints map, copied frontier key set), submits, and
applies whichever round has completed — exactly once, via an atomic take — before draining. A completed round is
also taken at the top of `process()`, because a round can carry a stop signal (a recreated channel; a dead
channel with holds) that should not sit for a further interval while records keep being delivered; lower-bound
facts are safe to apply at any point on the stream thread, so this costs one atomic read per record. `onFacts`
still runs only on the stream thread, so the engine stays single-threaded and the
read-report-before-truncation-check order 4.2 flags is untouched. The executor is
injectable; the topology tests use a same-thread executor, keeping their punctuation semantics deterministic.

**Alternatives**

* A thread per process — rejected: the shared source's lock serialises them anyway; one thread costs less and
  changes nothing observable.
* Async application (executor calls `onFacts`) — rejected outright: the engine is single-threaded by design, and
  facts application mutates ordering state atomically with the step.

**Cost**

Facts land up to one interval later than they were gathered — liveness latency the lower-bound argument makes
free of ordering consequences. Startup's seeding round remains synchronous (one-time, off the per-record path).

### D55 — Per-process status: state plus refusal reason, readable programmatically

**Context**

ASSESSMENT 1.10 / Operational 1: the public surface was `start`/`healthy`/`close`; every fail-closed reason died
in a log line, so a supervisor could not distinguish a deliberate refusal (which recurs identically) from a
transient outage — and `healthy()` reported true through the doomed shrink's 42 seconds of rebalance limbo.

**Decision**

`Parsley.status()` returns per-process `ProcessStatus`: the host state (running/rebalancing/stopped), the refusal
reason when the stop was a deliberate `ParsleyFailClosedException` (unwrapped from the failure chain), and the
failure detail either way; `stoppedDeliberately()` is the supervisor's branch. The uncaught-exception handler
records the failure before the client's state machine winds down, and `healthy()` now also requires no recorded
failure — a process whose stream thread has already died can no longer report healthy while rebalancing toward
its shutdown.

**Alternatives**

* A listener/callback API — rejected for v1: polling status covers the operator loop without adding a
  subscription surface Structural 9 would have to defend.

**Cost**

Status is poll-only; failure detail is a string, not a structured cause chain.

### D56 — The seam's header view is application headers only; the reserved refusal names its reason

**Context**

ASSESSMENT 1.13: `Delivery.headers()` included the `parsley.causes` header while `Effects.Emission` refused any
`parsley.`-prefixed header — so the natural pattern of forwarding received headers on an emission threw in the
handler's frame and crash-looped deterministically. Fail-closed and safe, but a sharp edge; and the refusal fired
as `IllegalArgumentException`, leaving the `RESERVED_HEADER_USED` reason dead code (ASSESSMENT 3.7).

**Decision**

`Delivery` filters the reserved prefix out at construction: the causes header is parsley's transport detail, not
application data — the seam passes the delivered message (SPEC Structural 3), and causal metadata is not part of
what the application sent. Forwarding `delivery.headers()` now simply works, with the emission carrying parsley's
own fresh stamp. The emission-side refusal remains for application-attached `parsley.*` headers and now throws
`ParsleyFailClosedException(RESERVED_HEADER_USED)`, wiring the previously dead constant to its real branch. The
read-side asymmetry recorded in D18's shadow — applications could read but not write the reserved header — ends:
the reserved namespace is now invisible in both directions (this also closes ASSESSMENT 1.15's read-side item).

**Alternatives**

* A separate `applicationHeaders()` accessor beside the raw view — rejected: two header methods invite exactly
  the forwarding mistake this removes, for a raw view no application can act on.
* Accept-and-strip on emission — rejected: silently discarding what the application explicitly attached is a lie;
  refusing writes and hiding transport reads is honest in both directions.

**Cost**

An application that decoded `parsley.causes` at the seam loses that view; decoding wire metadata is a consumer's
job (docs/wire-format.md), not the seam's.

### D57 — The ordering store's changelog requests logging and compaction explicitly

**Context**

ASSESSMENT 1.15: D17's "the store is changelogged" and D38's "(compacted) changelog" both rested on documented
Streams *defaults* the code never requested. Were either default ever different, restart would restore an empty
store (Safety 2 and Liveness 5 collapse) or held entries would age out of the changelog.

**Decision**

The ordering store's builder requests `withLoggingEnabled(cleanup.policy=compact)` explicitly. Application
stores keep the defaults: their changelogs carry application state, whose configuration is the application's
concern under the host's rules (Structural 8), not a guarantee parsley stands on.

**Alternatives**

* Recording the reliance under the D19 pattern — rejected: a one-line request is cheaper than a documented
  vulnerability to a defaults change.

**Cost**

None; the explicit request encodes what the defaults already did.

### D58 — Declared topic names must stay clear of the runtime's namespace

**Context**

ASSESSMENT 1.14: nothing refused a declared topic named like a runtime-internal topic; the collision would
corrupt the ordering state's transport, and the namespace grows with every runtime-owned topic.

**Decision**

`start()` refuses, before any broker contact, a declared topic that contains `__parsley.` or collides exactly
with an induced internal name (the ordering-store changelog, or any declared store's changelog).

**Alternatives**

* Relying on the topology build's duplicate-source failure — rejected: it is incidental, name-shaped, and reports
  a Streams concept instead of the collision.

**Cost**

Applications cannot use `__parsley.` in topic names — the point.

### D59 — Partition expansion is handled by restart; mid-run it gets a parsley diagnosis

**Context**

ASSESSMENT 1.7: partition counts are resolved once at start and baked into the topology; a partition added
mid-run kills the application inside Streams or the consumer (`NoOffsetForPartitionException` under
`auto.offset.reset=none`) with no parsley concept or remedy named anywhere.

**Decision**

The boundary stands — a full restart handles expansion correctly (re-resolution plus EARLIEST pre-commit for the
new partitions), and a width-changing expansion refuses with `TASK_WIDTH_CHANGED` and its remedy (D49). Mid-run,
the uncaught-exception handler recognises the two foreign failure shapes and logs the parsley diagnosis naming
the restart remedy; the failure is readable through `status()` (D55). The boundary is now documented here rather
than discovered in a crash loop.

**Alternatives**

* Supporting live expansion — rejected: the topology's width is fixed by Streams at start; anything "live" is a
  disguised restart with more failure modes.

**Cost**

Expansion remains an operator-visible restart; the diagnosis is a log line and a status entry, not a refusal
(the process is already dead when the evidence appears).

### D60 — StateReader keeps point lookups only; enumeration stays out of v1

**Context**

ASSESSMENT 5.1: `StateReader` exposes `get` alone though the stores beneath support range scans. A deliberate
scope decision (D12), not a safety concern — reads of local committed state cannot reorder deliveries.

**Decision**

Deferred, deliberately, until the three questions the assessment names have answers a v1 should not improvise:
iterator visibility against the current invocation's buffered `Effects` writes (undefined today — reads see
applied writes immediately, but an iterator opened mid-handler would see a torn prefix); determinism over
serialized-key order (byte order is not deserialized order, so range semantics depend on the serde); and scan
locality (a "range" is one shard of a partitioned store, which reads as the whole key space to an application).
Multi-store point-lookup patterns already work.

**Alternatives**

* Shipping `range`/`all` now with byte-order semantics — rejected: an API whose iteration order silently depends
  on serde encoding is a trap that public-surface stability would then freeze.

**Cost**

Applications needing enumeration keep a parallel index store by key-prefix convention, or wait.

### D61 — Naming: the wrong-edit hazard is fixed; mechanical enforcement is deferred, recorded as a deviation

**Context**

ASSESSMENT 5.2: nothing in the build enforces naming, and the semantic layer drifts where it costs most — the
public API. The assessment's inventory is accepted as accurate, including the one item with a route to a wrong
edit: `StoreCodec.longValue`/`longOfValue`, adjacent near-inverses.

**Decision**

The near-inverse pair is renamed to the codec's own convention (`encodeLong`/`decodeLong`), removing the
wrong-edit route. Mechanical enforcement (checkstyle or equivalent) is deferred as a recorded deviation: the
uniform invariants worth pinning (record accessors, `LOG`, `*Exception`, `*Test`, SCREAMING_SNAKE constants,
sentence-style test names) are stable under review today, while the drifted names are dominated by public-API
surface whose renames are breaking for any early adopter; renaming into a style gate mid-hardening would spend
the remaining risk budget on churn with no bearing on the guarantees. The deviation's substance: adopting a
linter without the renames would freeze today's drift as sanctioned style — worse than leaving the inventory
open beside this record.

**Alternatives**

* Checkstyle with a naming-only ruleset now — rejected for this pass, as above; it is the natural first step of
  any post-hardening cleanup, with the assessment's inventory as its worklist.

**Cost**

The drift inventory remains live; nothing stops new drift until a gate exists.

### D62 — Emissions are matched to the declared send set by topic name; the emission's Channel supplies the serdes

**Context**

ASSESSMENT 1.15: an implemented open choice was unrecorded. Emissions are validated against the declared send set
by topic name only; the emission's own `Channel` object supplies the serdes; the declared send channel's serdes
are never consulted; and `sends()` silently keeps the first `Channel` per topic while `receives()`/`stores()`
throw on duplicates and store access enforces reference identity. The assessment verified this is not a safety
defect — the emission's serde is the application's own codec either way (what Safety 4/5 require), and a
downstream mismatch fails closed at the reader per D13 — but identity-matched emissions were an implementable
alternative that this code declined without a record.

**Decision**

Name-matching stands, now recorded. The declaration's send set authorises *topics* (Structural 19 speaks of "the
channel the application named", and Assumption 14 maps declarations to topics); the serdes on an emission's
`Channel` are the application's statement of how *this* payload encodes, which Structural 4 makes a compile-time
property of the call site. Two `Channel` objects for one topic with different serdes are therefore legal by
construction wherever plain producers are — the reader's codecs judge the result (D13), exactly as with any
external producer.

**Alternatives**

* Reference-identity matching of the emission's `Channel` against the declared one (as stores enforce) — rejected:
  stores are process-local state where aliasing means corruption; a topic is a shared substrate where multiple
  typed views are ordinary Kafka. Identity matching would refuse working patterns (a shared channel catalogue vs
  a locally constructed channel) to prevent nothing the reader-side check does not already catch.
* Making `sends()` throw on duplicate topics like `receives()` does — declined with the same reasoning: repeated
  sends-declarations of one topic are harmless, and the first-Channel-kept value is never consulted for emissions.

**Cost**

A serde mismatch between two views of one topic surfaces at the downstream reader, not at the emission site.

### D63 — close() releases every resource, each step isolated and bounded

**Context**

ASSESSMENT 1.12 / Operational 3: `close()` ran streams closes, facts-source closes and the admin close with no
isolation — one throw leaked everything after it, and `start()`'s failure path calls the same method. Hardening
this surfaced a second shape live: a streams application wedged in shutdown (a stream thread died failing closed
while its topics were being deleted) blocks an unbounded `KafkaStreams.close()` forever — an unbounded close is
itself a failure to release.

**Decision**

Every release step runs in its own try/catch (streams applications, the facts executor, the facts sources, the
admin client), and the streams close is bounded at 30 seconds with a logged warning on timeout. Nothing a single
resource does can prevent the others from being released.

**Alternatives**

* Rethrowing the first failure after releasing the rest — rejected: `close()` is called from failure paths where
  a second exception would mask the first; the log carries the release failures.

**Cost**

A wedged streams application may leave its own threads behind after the timeout; the process exit reaps them.

### D64 — Real coordinates: `io.github.tobyjamesclements:parsley`, packages `io.github.tobyjamesclements.parsley.*` (corrects D27)

**Context**

D27 chose `candidate.parsley:parsley` because AGENTS.md restriction 1 forbade consulting the pre-existing
implementation, and coordinates that might collide with it could not be checked. Its recorded cost was exact: "a
rename ripples through packages if this ever publishes for real." That is now the case — this tree is being
prepared to replace the implementation behind the published artefact, so it must carry that artefact's identity
rather than a placeholder.

**Decision**

Maven `io.github.tobyjamesclements:parsley`, packages `io.github.tobyjamesclements.parsley.core` (host-independent
protocol), `io.github.tobyjamesclements.parsley.api` (public surface), `io.github.tobyjamesclements.parsley.kafka`
(Streams adapter). `Automatic-Module-Name` is `io.github.tobyjamesclements.parsley`, so a modular consumer names
the library the same way whether it reads the manifest or the packages.

Note the groupId is `io.github.tobyjamesclements`, not `io.github.tobyjamesclements.parsley` as D27's shape would
suggest: that is the coordinate the existing artefact already publishes under, and preserving it is the whole point
of the change.

D27 stands as the record of why the placeholder was correct at the time; it is superseded, not withdrawn.

**Alternatives**

* Keep `candidate.parsley` and publish under it — rejected: it abandons the coordinate consumers already resolve,
  which would make the replacement a new artefact rather than a new version of the existing one.
* Flatten to a single package to match the existing implementation's layout — rejected: the core/api/kafka split is
  load-bearing (SPEC Structural 9 rests on `core` naming no host type, and `CorePurityTest` enforces it by scanning
  that directory). Matching a layout is not worth dissolving a boundary a test depends on.

**Cost**

Every type in the published surface changes its fully-qualified name, so no consumer of the previous implementation
recompiles against this one. That break is inherent in replacing the implementation and is not made worse by the
choice of coordinates.

### D65 — The mutation gate's skip flag is bound to a declared property

**Context**

The grafted workflows pass `-DskipPitest=true` in four places (`broker-it`, `publish`, and both Claude workflows'
allowed-tools lists). The POM those workflows came from declares no such property and gives pitest no `<skip>`, so
the flag was inert: every one of those invocations ran the full mutation analysis it believed it had turned off.

**Decision**

Declare `<skipPitest>false</skipPitest>` and bind pitest's `<skip>` to it, so the flag does what its four callers
already assume. The gate stays on by default; only an explicit `-DskipPitest=true` lifts it.

**Alternatives**

* Drop the flag from the workflows instead — rejected: the callers are right that a publish step repackaging an
  already-verified tree, and a broker suite whose subject is the broker, should not re-run mutation analysis. The
  POM was the side that was wrong.

**Cost**

A gate that can now be skipped can be skipped by mistake. `ci.yml` — the workflow `publish.yml` keys off — passes no
such flag, so the path that gates publication still runs it.

### D66 — The mutation gate covers the core and the API, not the Kafka adapter

**Context**

The grafted POM brings a pitest gate at `verify`: 75% mutation score, 80% line coverage, thresholds inherited
unchanged from the repository this tree is replacing. That repository aimed the gate at its whole package and
excluded only `*IT` — tests that live behind a profile and never run in a normal `verify`. This tree has no `*IT`
classes; its equivalent slow tests are the `*IntegrationTest` classes, and they run in the ordinary suite.

Transliterating the exclusion pattern gave the wrong answer in both directions. With `*IntegrationTest` excluded and
the gate aimed at every package, the score was 54% against a 75% threshold — but test strength was 85% and 237 of
651 mutations had no coverage at all. The gate was not reporting a weak suite; it was reporting the kafka adapter
measured with the only tests that cover it switched off. Aiming the gate at every package with nothing excluded
inverts the problem: each mutation in the adapter re-runs a test that starts an embedded broker, and the run was
abandoned unfinished after more than ten minutes on eight threads — against a CI that runs `pitest.threads=1`.

**Decision**

Aim the gate at `core.*` and `api.*` and exclude `*IntegrationTest`. Measured: 375 mutations, 82% killed, 89% line
coverage, test strength 91%, 1m49s. Both inherited thresholds pass without being lowered.

The adapter is left out on purpose, and the boundary is a real one rather than a convenience: `core` is where the
protocol lives and is already required to name no host type (Structural 9, enforced by `CorePurityTest`), so it is
exactly the code whose behaviour a mutation can change without a broker noticing. The adapter's evidence is the
integration suite against a real embedded broker, and the rows in `EVIDENCE.md` that cite it.

**Alternatives**

* Lower the thresholds until the whole-package score passes — rejected: it would encode a number produced by a
  misconfiguration as the standard, and the honest core+api figure clears the inherited bar anyway.
* Keep the adapter in and accept the runtime — rejected: not measured to completion, so its score is unknown; a gate
  nobody can afford to run is not a gate. Reopening this needs the integration tests to stop being the adapter's
  only coverage, not a longer timeout.
* Drop the gate — rejected: it earns its keep on the core, which is the part a reader is asked to trust.

**Cost**

A mutation in `kafka` that the integration suite happens not to catch is caught by nothing here. That is a known
hole, named rather than hidden, and it is the hole `EVIDENCE.md` exists to keep honest.

### D67 — No mutation gate; the three gaps it found are recorded here instead (supersedes D65, D66)

**Context**

D66 put a pitest gate on `core.*` and `api.*` and justified it by the number: 82% mutation score, 89% line
coverage, both inherited thresholds cleared unlowered. That justification does not survive reading the survivors
rather than the score.

Of 66 surviving or uncovered mutations, roughly forty are accessors, `toString`, `equals`/`hashCode`, `return this`
builder chains and one-line delegation (`Parsley.close()` → `runtime.close()`). Five are mutations of the
`sabotage.has(...)` branches themselves — test-only instrumentation that ships in `main` but which the public API
offers no way to reach (Structural 9), so the gate was partly scoring its own scaffolding. Several of the most
alarming-looking survivors are behaviourally equivalent and unkillable by construction: D-410 and D-502 mutate
`position > current` to `position >= current` inside idempotent max-merges, where the equal case rewrites the same
value; L482 negates `head.position == position`, routing the equal case to a `removeIf` on that same position.

A score dominated by boilerplate, partly measuring test-only code, and floored by mutants that cannot be killed is
not a quality signal. Defending it over time means chasing equivalent mutants or growing an exclusion list.

The suite already has mutation testing, and better aimed: `Sabotage`'s modes are semantic violations each tied to a
named SPEC criterion, each with a meta-test proving the suite catches it, and a randomised sweep with a measured
margin. That is the mechanism this project verifies itself with, and it is the one ASSESSMENT.md used.

**Decision**

Remove the pitest plugin, its properties, and the now-dead `-DskipPitest=true` flags the workflows carried. D65's
skip-flag binding goes with it; the flag it fixed no longer has a plugin to skip.

Three gaps the run surfaced are recorded here, because they are the whole return on it and must outlive the tool:

1. **`markDelivered` L494 — `mergeDeliveredPast(channel, position)` can be deleted and the full suite stays green.**
   Verified by hand, not inferred: the call removed, `mvn test` returns 418 tests, zero failures. Its own comment
   cites Structural 16 and "Safety 1 across the lifetime", and EVIDENCE row 16 claims the delivered-past clamp is
   pinned by `TargetedScenarioTest#channelJoiningDoesNotDeliverCausesBehindDeliveredEffects`. That test covers the
   *other* call site — the `delivered.causes.byChannel().forEach(this::mergeDeliveredPast)` on the following line —
   not the clamp's own position. Either the call is redundant and the comment overclaims, or row 16 overclaims and
   a lifetime-safety property has no test. EVIDENCE.md's own standard makes both worth resolving.
2. **`extractCauses` L262 — no test constructs a message carrying two `causes` headers.** The duplicate-header path
   into `failUndecodable` has no coverage at all. Malformed-wire handling, adjacent to Safety 7.
3. **`onReceive` L209 — the `message.position() <= fedBefore` boundary is unpinned.** Weakened to `<`, feeding the
   same position twice stops failing closed and nothing notices. The comment directly above describes exactly that
   case in the wild — a recreated topic's records arriving under the old channel's identity — and says fail loudly,
   never drop silently.

**Alternatives**

* Keep the plugin without the `<executions>` binding, runnable on demand — rejected: an unrun tool in a POM is a
  claim of rigour nobody is checking, and anyone who wants this can add it back for an afternoon. Deleting it is
  honest about what the project actually relies on.
* Keep the gate because the repository being replaced had one — rejected: it had one aimed at a codebase without a
  sabotage framework. Carrying a mechanism because it was inherited, rather than because it earns its place, is how
  a build accumulates ceremony.
* Lower the thresholds to leave headroom — rejected: it makes the number even less meaningful than it already is.

**Cost**

Nothing now watches for the *class* of defect an outside-view syntactic tool finds — the invariant nobody thought
to sabotage, which is exactly how all three gaps above surfaced. `Sabotage` can only encode violations already
imagined. Anyone reopening this should run pitest as a finder, read the survivor list, and discard the score.

### D68 — `kafka-metadata:test` dropped; the rest of the broker test stack is load-bearing

**Context**

`dependency:analyze` reports six declared-but-unused dependencies. Five of those reports are false: the tool sees
only compile-time symbol references, and a logging binding, a JUnit aggregator POM and a broker started
reflectively by a test kit are none of them. Taking the list at face value would have removed the embedded broker.

**Decision**

Remove `org.apache.kafka:kafka-metadata:test` only. Each candidate was tested by deletion against the broker
integration suite rather than argued from the analyzer's output:

* `kafka-clients:test` — required. Without it `TestKitNodes$Builder.build` fails on `org.apache.kafka.test.TestUtils`.
* `kafka-server-common:test` — required. Without it the cluster fails on `org.apache.kafka.server.fault.MockFaultHandler`.
* `kafka_2.13` (no classifier) — required: it carries the broker and, transitively, the whole server stack. The
  `:test` classifier of the same coordinates brings no transitives of its own, classifier artifacts being leaves.
* `kafka_2.13:test` — required: `kafka.testkit.KafkaClusterTestKit` and `TestKitNodes` live there.
* `kafka-metadata:test` — **not** required. All 17 broker integration tests pass without it; the non-classifier
  `kafka-metadata` still arrives transitively through `kafka_2.13`, which is what the runtime actually needs.
* `slf4j-simple` — kept. A binding is never referenced by symbol, and surefire's
  `org.slf4j.simpleLogger.defaultLogLevel` only means anything while it is present. Removing it would silence every
  fail-closed diagnostic the suite prints.
* `junit-jupiter` — kept. An aggregator POM is always "unused" by this analysis; the api and params artifacts the
  analyzer wants declared arrive through it, and declaring them directly would be the less conventional POM.

**Alternatives**

* Declare `junit-jupiter-api` and `junit-jupiter-params` explicitly to silence the used-undeclared warning —
  rejected: it trades the idiomatic aggregator for a longer POM that pins two artifacts instead of one.
* Leave `kafka-metadata:test` in place as insurance — rejected: an unused test-classifier artifact is a version
  that has to be kept in step with the broker for no benefit. A Kafka upgrade that changes what the test kit needs
  will say so by failing, which is the same signal, arriving when it is actionable.

**Cost**

If a future Kafka version moves a class the test kit needs into `kafka-metadata`'s test jar, the suite fails with a
`NoClassDefFoundError` naming it. That is a clear failure, not a silent one.

### D69 — Kafka 3.9.2 (supersedes D19's version pin)

**Context**

D19 pinned Kafka clients and Streams at 3.9.1. Kafka 3.9.2 fixes CVE-2026-35554: a buffer
pool race in `kafka-clients` causing silent message corruption and cross-topic misrouting,
affecting every version from 2.8.0 up to but excluding 3.9.2.

That defect is squarely in this library's path. `kafka-clients` is a compile-scope dependency
inherited by every consumer, and a message silently delivered to the wrong topic defeats the
delivery guarantee from underneath, in a way no check in this repository could observe. A
corrupted or misrouted record is not a record whose causes were mis-decided; it is a record
that was never the one sent.

**Decision**

Move to 3.9.2. The suite is green at 418 tests, and the test-classifier artifacts the
embedded broker needs all resolve at the new version.

**What this does not fix**

The open item this closes was recorded as a concern about the commons-compress CVEs, which
drove the previous repository's Testcontainers bump. This bump does not touch them, and the
tree does not need it to. Comparing the dependency tree either side of the change, every
transitive version is identical:

* `commons-compress` 1.26.2, unchanged, test scope
* `commons-io` 2.14.0, unchanged, test scope
* `snappy-java` 1.1.10.5, unchanged
* `zstd-jni` 1.5.6-4, unchanged

commons-compress 1.26.2 already carries the fixes for CVE-2024-25710 and CVE-2024-26308, both
resolved in 1.26.0, which were the advisories behind that Testcontainers bump. It arrives here
through the embedded broker's test artifacts rather than through Testcontainers, at test scope,
so it reaches no consumer. The consumer-facing dependency set is `kafka-streams`,
`kafka-clients`, `rocksdbjni` and `slf4j-api`, and nothing else.

**Alternatives**

* Move to 4.x, as the previous implementation had done, rather than take the patch release.
  Rejected here as a separate question: 4.x is a major upgrade whose behaviour this tree has
  never been run against, and taking a security patch should not be coupled to it. The
  argument for 4.x stands on its own and is untouched by this entry.
* Stay on 3.9.1 and treat CVE-2026-35554 as not applicable. Rejected: the affected component
  ships to consumers, and the failure mode is silent.

**Cost**

The 3.9.x line is a maintenance branch. Whatever argument existed for moving to 4.x still
applies, and this entry neither makes nor forecloses it.
