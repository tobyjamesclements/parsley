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

*Superseded in part by D98: the value grammar this record froze (version byte `0x01`, flat
entries) was replaced pre-release by the grouped grammar, before any released message carried
it. The header key, the reserved prefix, strict fail-closed decoding and canonical bytes all
stand.*

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

*Superseded in part by D98: the merge-site check's no-encode premise — size affine in the
entry count — no longer holds under the grouped grammar; the engine maintains the encoded
width incrementally instead. The budget itself, its three enforcement points, and the
bytes-not-entries choice all stand.*

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
free of ordering consequences. Startup's seeding round remains synchronous while the source is free (one-time,
off the per-record path); when another round holds the source, the seed waits a bounded five seconds and starts
unseeded — facts are lower bounds, so the cost is evidence deferred to the first background round, and a slow
broker no longer stacks every initialising task's seed against the poll interval (audit finding M1).

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

### D70 — Kafka 4.3.1 (supersedes D69 and D19's pin)

**Context**

D69 took the 3.9.2 patch release for CVE-2026-35554 and left the move to 4.x open as a
separate question. This entry settles it. 4.3.1 is the current 4.x release and carries that
fix, 4.2.0 being its earliest 4.x fix version.

**Decision**

Move to 4.3.1. The suite is green at 418 tests against a real embedded 4.3.1 broker. Three
things had to change, none of them in the protocol or its core.

**The embedded broker moved.** `kafka.testkit.KafkaClusterTestKit` and `TestKitNodes` are
gone in 4.x, replaced by `org.apache.kafka.common.test` in the `kafka-test-common-runtime`
artifact. That artifact brings the broker transitively, so the five declarations the 3.9.x
test kit needed collapse to one: `kafka_2.13`, its test classifier, and the test classifiers
of `kafka-server-common` and `kafka-clients` are all removed. Each removal was tested by
deletion against the integration suite rather than assumed.

**The JUnit platform had to align.** `kafka-test-common-runtime` brings
`junit-platform-launcher` 1.13.1, and Jupiter 5.11.4 brings `junit-platform-engine` and
`junit-platform-commons` 1.11.4. A launcher newer than its engine fails the forked JVM before
any test runs, on `OutputDirectoryProvider`. Jupiter moves to 5.13.1, whose platform is
exactly 1.13.1.

**Avro had to stop dictating Jackson.** `avro` 1.12.0 depends on `jackson-core` 2.17.2, and
being a direct dependency it won on nearest-first over the 2.21.2 the broker's Jackson stack
expects. log4j2 parses its configuration with Jackson, so the broker died in static
initialisation with `YAMLParser._updateToken` missing, surfacing as an unrelated-looking
`Could not initialize class kafka.utils.Log4jControllerRegistration$`. `jackson-core` is now
excluded from `avro`, so the broker's version governs and keeps governing as Kafka moves.

**Effect on the transitive set**

* `snappy-java` 1.1.10.5 to 1.1.10.7
* `zstd-jni` 1.5.6-4 to 1.5.6-10
* `commons-io` 2.14.0 to 2.16.1
* `commons-compress` 1.26.2, unchanged, still test scope
* `rocksdbjni` 7.9.2 to 10.1.3, which is consumer-facing

The consumer-facing set stays four artifacts: `kafka-streams`, `kafka-clients`, `rocksdbjni`,
`slf4j-api`.

**Alternatives**

* Stay on 3.9.2. Rejected: 3.9.x is a maintenance branch, and the tree is now demonstrated
  green on 4.x against a real broker of that version, which was the only real unknown.
* Keep `kafka_2.13` declared alongside `kafka-test-common-runtime` as insurance. Rejected: it
  is unused, and an unused artifact is a version to keep in step for no benefit. Its absence
  fails loudly if the test kit ever needs it again.

**Cost**

The broker floor rises with the client. `EVIDENCE.md` row 1 already records that no executable
check distinguishes broker versions below the floor, and that remains true.

### D71 — Avro dropped from the test dependencies (amends D70)

**Context**

`avro` 1.12.0 supported exactly one test, and carries CVE-2025-33042, a code-injection flaw
fixed in 1.12.1. The exposure here was already nil: the dependency is test scope, so it
reaches no consumer, and the vulnerable path is specific-record generation from untrusted
schemas, where the test used a hardcoded schema and generic records.

The dependency's real cost was elsewhere. Avro pins `jackson-core` 2.17.2 and, being a direct
dependency, won on nearest-first over the 2.21.2 the embedded broker's Jackson stack expects.
That is what broke the move to Kafka 4.x, presenting as a failure to initialise
`kafka.utils.Log4jControllerRegistration$`, and it is why D70 added a `jackson-core` exclusion.
One test dependency was setting the Jackson version for the whole build.

**Decision**

Remove `avro`. `AvroWireFormatTest` becomes `FramedPayloadWireFormatTest`, keeping every
assertion and encoding its payload by hand in the Confluent Schema Registry layout: a zero
magic byte, a four-byte big-endian schema id, then an opaque body.

The `jackson-core` exclusion D70 added goes with it, since Avro was its only cause. The whole
Jackson stack resolves at 2.21.2 unaided.

**What this costs, plainly**

Safety 5 names Avro. No test now drives a real Avro codec, so that leg of the criterion rests
on the structure rather than on a demonstration: the topology carries bytes and applies
application serdes only after the delivery decision, so no code path can distinguish Avro
bytes from any others. The framing case still exercises the shape that would catch a
regression, since a leading zero magic byte is what defeats naive wrapping and
length-prefixing, and the test asserts that byte is present. A regression touching only
Avro-encoded bodies would not be caught. `EVIDENCE.md` row 5 says so.

**Alternatives**

* Bump to 1.12.1, keeping a real codec and the realism it gives. Rejected: it keeps a
  dependency that earns one test, keeps the Jackson exclusion, and keeps a CVE stream for a
  library this project does not otherwise use.
* Leave 1.12.0 in place, since the vulnerable path is unexercised. Rejected: true, and it
  still leaves scanners flagging the build and Avro governing Jackson.

**Cost**

Recorded above. The dependency count for the test suite falls by one, and nothing outside the
one test changed.

### D72 — Names applied at vendoring (implements ASSESSMENT §5.2's decide-now)

**Context**

ASSESSMENT §5.2 found the semantic layer drifting where it costs most — the public API —
and closed with the asymmetry that makes this a vendoring decision: public renames are
breaking later, so decide now or record the deviation. PR #90's `NAMING.md` carries the
audit-derived recommendations; this entry records what was decided and applied, so the
resolution is traceable in-tree rather than only in an unmerged draft.

**Decision**

`StoreDef` → `Store`, and the rule that decides the suffix question: a declaration that
doubles as the runtime handle is a bare noun (`Channel`, `Store` — the parameter at every
use site was already `store`), while the one purely declarative type keeps `Definition`.
`ProcessDefinition` keeps its name both because the declared/running split is real only
for processes (`Parsley.start` consumes the definition; the running process reports as
`ProcessStatus`) and because bare `Process` would shadow the auto-imported
`java.lang.Process`. `ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT` is
deliberately kept: D55 establishes supervisors key on `refusalReason`, and renaming it
breaks the integrations the fail-closed contract exists to serve.

Internal renames applied in the same change, per `NAMING.md` entries 1–7: `pendingRound`,
one `incarnation` term through the revival machinery, `roundLock`, `earliestOffsetFutures`,
`channelOfEntryKey`, and the `gatherRound`/`completeRound` merge — with one correction the
review of the change itself surfaced: entry 1's proposed `affirmedGoneSince` overclaims
for an id whose name was never learned, where the window is anchored by an UNAVAILABLE
verdict rather than any affirmed answer (the reason that path waits the longer window).
The applied name is `deadWindowSince`: anchored on what the field does — opens the
dead-confirmation window — which is true on both paths.

No behaviour changes. Wire format and store key bytes are untouched (the codec renames are
method names only), so `EVIDENCE.md` is unchanged; the one coupling the compiler cannot
see — `ProcessorRevivalTest`'s reflective field lookups — fails the suite if the names
drift, which is the pin these renames need.

### D73 — The api/ surface validates at declaration; the send seam resolves the declared channel by name (supersedes D62's serde-supply and duplicate-sends choices; resolves ASSESSMENT §1.14 and §1.15's send-set bullet)

**Context**

ASSESSMENT §1.14 recorded topic names validated nowhere beyond non-blankness while the
runtime's internal namespace grew, and §1.15's "[MUST — Structural 20] send-set matching"
bullet recorded emissions matched by topic name with the emission instance supplying the
serdes — an implemented open choice with an identity-matching alternative, unrecorded. A
focused pass found the surface enforcing well at the effect seams while accepting
declaration mistakes that surfaced late or silently: a null `startingAt` position compared
unequal to EARLIEST at commit time and silently meant LATEST; null serdes, handlers and
effect targets died as NPEs on the stream thread at first use; a malformed
`applicationIdPrefix` failed deep inside Streams internal-topic creation; and two
processes could silently compose one changelog topic name, each restoring the other's
records.

An earlier revision of this entry adopted reference identity at the send seam ("emissions
must carry the very instance passed to `sends(...)`"). Review showed that rule fails
closed on the first record — permanently, replaying identically across restarts — for two
patterns that build cleanly and were legal before it: a `Channel.of` factory method called
at both `sends()` and `send()` time, and a self-loop declared
`receives(channel.startingAt(LATEST))` whose handler re-emits via `delivery.channel()`
(`startingAt` returns a new instance). No declaration-time check can catch either, because
the emission instance does not exist until the handler runs. SPEC Structural 19 also words
the obligation by name — an emission "naming a channel outside the declared send set" must
fail — which identity-matching narrows beyond the spec.

**Decision**

Declaration mistakes fail at the declaration site with `IllegalArgumentException` — the
one exception type for null or malformed components across `api/` and `core/` alike
(`HeaderKV` and `ChannelId` previously threw `NullPointerException` while `Causes` and
every `api/` site threw `IllegalArgumentException` for the identical mistake). `Channel.of`
and `Store.of` refuse null serdes; `startingAt` refuses null; `receives` refuses a null
channel or handler; effect targets, header elements, state-write keys, property keys and
values, and status components are refused at construction with messages naming the
mistake; `sends(...)` and `stores(...)` validate their whole argument list before
committing any of it, so a refused call leaves the builder unchanged.

Names that feed Kafka topic names — channel topics, store names, process names,
`applicationIdPrefix` — share one spelling of Kafka's rule (`KafkaNames`: a precompiled
pattern, the 249-character bound, `'.'`/`'..'` refused; public, so the kafka layer
consumes the same bound, applications can pre-validate names, and the spelling is pinned
sample-for-sample against kafka-clients' own `Topic.isValid` so a client upgrade that
changes the broker's rule fails the build), and none of the four may *contain* the
reserved `__parsley.` namespace anywhere — containment, not prefix, because an embedded
occurrence composes an application id, consumer group or changelog name inside parsley's
own namespace, whichever component carries it. Changelog names are composed at exactly
one site (`ProcessTopology.changelogName`, also the serde topic the processor hands
serializers, so schema-registry subjects cannot drift), bounded there to Kafka's limit,
and refused at start when two processes compose the same changelog topic.
`Parsley.start` refuses a null config, a null definitions array and null elements under
the same taxonomy.

The send seam resolves the declared channel **by name** and serializes with the declared
channel's serdes — the way the store seam already writes with its declared store. The
emission instance contributes only the topic name, so a look-alike has no serdes to
smuggle and reference identity has nothing left to protect; an emission naming a topic
outside the declared send set still fails the step (`EMISSION_TO_UNDECLARED_CHANNEL`,
SPEC Structural 19). This keeps D62's name-matching core but supersedes two of its
choices: the emission's own `Channel` no longer supplies the serdes (the declared
channel's are consulted instead — the option D62 never weighed, having compared
name-matching only against identity-matching), and `sends(...)` now refuses one topic
declared through two instances, the alternative D62 declined while the first-kept
instance "was never consulted for emissions" — a premise this entry's serde rule
invalidates. The store seams keep the identity rule and gain their own reason,
`STATE_ACCESS_TO_UNDECLARED_STORE`, in place of a bare `IllegalStateException` that
`ParsleyFailClosedException.findIn` could not surface through `status()`. The asymmetry
between the seams is deliberate: a store *read* returns a value the caller casts to the
passed instance's types, so resolving a look-alike store by name would smuggle a
differently-typed codec into the application's own frame; an emission is write-only and
has no such path back. A read refusal is raised inside the handler's own frame, where an
application catch could swallow it, so the reader also latches it and the processor
rethrows once the handler returns — the step fails either way. And the processor plans
before it applies: every effect target is resolved and every payload serialized (declared
serdes; a serializer failure is `APPLICATION_PAYLOAD_UNSERIALIZABLE`) across the whole
returned `Effects` before the first write reaches RocksDB or the first record is
forwarded, so no refusal — undeclared target, unserializable payload, reserved header,
exceeded metadata budget — can leave a half-applied step relying on the EOS abort alone.

**Alternatives**

- *Reference identity at the send seam* (this entry's earlier revision). Rejected: breaks
  the factory and self-loop patterns permanently at first production record, and narrows
  SPEC Structural 19's name-worded obligation.
- *Canonicalising one `Channel` instance per topic per definition at `build()`*. Rejected:
  the factory pattern's emission instance is constructed inside the handler and is
  invisible to `build()`, so the runtime seam still needs its own rule; and refusing
  cross-surface instance mismatches at `build()` would refuse the `startingAt` self-loop —
  the legitimate declaration the identity rule already broke. Its one unambiguous piece
  survives: `sends(...)` refuses the same topic through two different instances, at the
  site where the ambiguity is visible.
- *Keeping the emission instance's serdes* (0.2.0-SNAPSHOT behaviour). Rejected: leaves
  open the serde-smuggling path §1.15 flagged, for no gain over declared-serde resolution.
- *`NullPointerException` for null components* (`java.util` convention, `HeaderKV`'s old
  behaviour). Rejected: message-bearing refusals are the point of this entry, the majority
  of the surface already threw `IllegalArgumentException`, and one rule beats two.
- *Keeping the bare `IllegalStateException` at the store seam*. Rejected: `findIn` returns
  `null` for it, so `status()` reported a guarantee-preserving stop with an empty
  `refusalReason` and no `Reason` constant existed for the condition at all.

**Cost**

This is a source- and behaviour-breaking change against 0.2.0-SNAPSHOT — nothing else in
the tree said so before this entry. `Channel.of`, `Store.of`, `ProcessDefinition.named`
and `ParsleyConfig.builder` now refuse names they accepted: longer than 249 characters,
`'.'` or `'..'`, or containing `__parsley.` anywhere (process names and prefixes gain the
length bound and dot refusals). `startingAt(null)`, null serdes, null handlers, null
effect targets, null header elements, null state-write keys, null property keys and
values, and null status components now throw where they previously misbehaved later — or,
for `startingAt`, silently meant LATEST. `sends(...)` refuses a topic declared through two
instances where it silently kept the first. `HeaderKV` and `ChannelId` throw
`IllegalArgumentException` where they threw `NullPointerException`. Emissions serialize
with the declared channel's serdes, so an application that deliberately emitted through a
second instance carrying different serdes now gets the declared codec's bytes; a
type-level mismatch surfaces as `APPLICATION_PAYLOAD_UNSERIALIZABLE` during planning,
before any write applies — though a declared serde typed loosely enough to accept any
object (a `Serde<Object>`, say) serializes a mismatched payload as-is, which type erasure
leaves undetectable. Process names and prefixes containing `__parsley.` are refused where
0.2.0-SNAPSHOT accepted them. Store-seam violations report
`STATE_ACCESS_TO_UNDECLARED_STORE` instead of a
bare `IllegalStateException`; supervisors keying on `refusalReason` (D55) see a constant
that did not exist. Finally, the construction-site refusals throw inside the handler's own
frame: an application wrapping its effect-building in `catch (RuntimeException)` can
swallow its own declaration bug and commit an empty step — the shape ASSESSMENT §1.13
records for the reserved-header refusal, accepted here for the same reason (the refusal
lands where the bug is, D56 already chose construction-time for headers, and an
application catching around its own handler owns what it swallows; causal safety is
unaffected because a swallowed emission was never expressed).

What would catch a violation: `ApiValidationTest` carries one test per refusal, each run
against the pre-fix tree and red there;
`TopologyWiringTest#lookAlikeEmissionSerializesWithTheDeclaredSerdes` fails if the send
seam consults the emission instance's serdes again; `#emissionThroughAFactoryBuiltChannelInstanceIsSent`
and `#selfLoopReEmissionViaTheDeliveredChannelInstanceIsSent` fail if either broken
pattern is refused again; `#stateWriteToAnUndeclaredStoreFailsClosedBeforeAnyWriteApplies`
pins the store-seam reason and the validate-before-apply ordering;
`#typeMismatchedLookAlikeEmissionFailsClosedBeforeAnyWriteApplies` fails if a mismatched
emission loses its reason or fires after a write applies;
`#swallowedUndeclaredStoreReadStillFailsTheStep` fails if the reader's latch is removed;
and `ApiValidationTest#kafkaNamesAgreesWithKafkaClientsOwnRule` fails if parsley's
spelling of the topic-name rule drifts from kafka-clients' own.

### D74 — Re-established read positions are checked against durable coverage; corrects D36's "never lossy"

**Context**

A spec-compliance audit of the kafka layer found D36's expiry restart unsound in one shape. Missing offsets are
re-established at the current log start; where retention advanced past the previous execution's covered position
while the process was stopped, that log start lies beyond positions this process never read. Committing it
fabricates a read-position report the host never made (Host obligation 2's report semantics are what make group
offsets trustworthy — Streams commits only what it fed), and the engine's own Safety 8 check (D9's defense in
depth) can never fire: `onFacts` applies the committed-offset report before the log-start comparison, so the very
report the bootstrap fabricated advances the coverage the comparison reads. That apply order is deliberate and
correct for genuine host reports — a report legitimately covers never-arriving positions, and truncation below a
covered position is retention (D54 preserved the order for exactly that reason) — which is why the defect is the
fabrication, not the order. D36's recorded cost, "slower, never lossy", was refuted: effects could deliver past
causes that were real messages discarded unread, with no local failure anywhere.

**Decision**

`commitInitialPositions` refuses (`POSITIONS_DISCARDED_UNREAD`, at start) any re-established position beyond the
ordering state's covered position plus one. Coverage comes from the same compacted changelog view the stranded
scan already reads — the changelog is now read once per start and shared — interpreted by the public, pure
`OrderingStateInspector.coveredPositions`: the `fedUpTo` records alone. A delivered-past entry without a
`fedUpTo` record marks a channel that joined the received set without ever being read here; its fresh baseline
is legitimate (D31 governs its dedupe), so it is deliberately not treated as coverage.

**Alternatives**

* Checking log starts against pre-apply coverage inside `onFacts` — rejected: a genuine host report may
  legitimately exceed the engine's coverage (aborted runs, control records), and truncation below such a report
  is retention; the pre-apply comparison would spuriously refuse compliant hosts. The comparison is sound only
  against coverage no report can inflate, which is exactly what the start-time durable view is.
* Re-establishing at covered+1 rather than earliest — rejected: where no positions were discarded this changes
  D36's re-feed-and-dedupe path for no gain, and where they were, resuming at covered+1 still resumes below the
  earliest retained position — the very thing Safety 8 forbids. The gap case must refuse, not resume.

**Cost**

One inspector pass per start. D36's recorded cost — "slower, never lossy" — is corrected by this record, not
edited in place: slower, never lossy, and refusing outright where retention outran the stopped process —
availability spent on Safety 8. Pinned by
`BootstrapIntegrationTest#expiredOffsetsBeyondRetentionRefuseRatherThanAbsorbTheGap` (red on the pre-fix tree:
the start succeeded and silently absorbed the gap).

Three shapes of the refusal's conservatism, found by review and accepted:

* The comparison is spelled `offset - 1 > covered` rather than `offset > covered + 1`: coverage can be the
  engine's fed-to-end sentinel (`Long.MAX_VALUE`, a dead-settled channel), which the addition would wrap to a
  refusal of every offset; the engine's own truncation check excludes the sentinel the same way.
* The gap between coverage and the log start may in truth have held only transaction markers or aborted
  batches — a fully-drained transactional topic whose retention then expires everything leaves exactly a
  one-marker gap — but once discarded nothing can show that, and Safety 8 forbids assuming it. Such an expiry
  restart refuses and needs a deliberate reset; with a plain producer the identical history starts clean.
* A channel that left the declaration keeps its coverage record (D25 never drops it). If its offsets expire
  and retention crosses that coverage while it is away, re-adding it now refuses rather than resuming at the
  log start — the positions in the gap were discarded without this process ever covering them, which is the
  condition Safety 8 names, whether or not the process was subscribed while they aged out. D25's
  expiry-rejoin flow stands only while retention has not crossed the retained coverage.

One residual stays open, recorded rather than closed: a partition whose prior execution committed no step at
all (bootstrap crash after the changelog was created but before the first task commit) leaves no coverage
record, and its re-established position is indistinguishable from a first start — no durable evidence exists
either way, and refusing would break every legitimate channel-join (D31's exemption is the same judgement).

### D75 — A nameless topic id is never confirmed dead; corrects D44's four-fold window

**Context**

The audit refuted D44's residual claim for ids with no learned name. D44's denial protection — a by-name
`TopicAuthorizationException` classifying as DENIED and resetting the window — is structurally unreachable for
exactly the ids that need it: a name is only learned from a successful describe, which a DENY-Describe ACL
forever prevents (the broker masks denial-by-id as unknown-topic), and name bindings are memory-only, so every
frontier cause on a non-declared topic loses its name at each restart. The four-fold time-only window then
confirmed death of a live topic after ~4 × 3 × factsInterval of routine ACL denial, pruning a live cause —
violating Structural 13's "MUST NOT discard any other cause" and under-expressing it on every subsequent send
(Structural 15), a downstream Safety 1 inversion with no diagnostic. D44's "bounded settle latency, never
safety" was false on this path, and its claim that D40's residual was "closed by corroboration" held only for
ids with known names.

**Decision**

Death confirmation always requires affirmative corroboration: the dead-confirmation window opens and extends
only on a NAME_GONE answer. An id whose name was never learned reports nothing, however long describe-by-id
stays silent; each round keeps trying to learn its name.

**Alternatives**

* Keeping the four-fold window with the residual recorded — rejected: the residual is reachable by routine
  operations and violates a MUST; recording it does not sanction it.
* Persisting name bindings for frontier ids — rejected: ordering-store format churn that closes only the
  restart leg; the never-learned leg (denied from first sight) needs the no-time-alone rule regardless, and
  with that rule in place persistence adds nothing safety-bearing.

**Cost**

A genuinely dead topic whose name this process never learned lingers in the frontier and its expression
forever; so does a foreign-injected id. The metadata budget (D52) bounds the growth and stops attributably.
Structural 13's required means of discarding dead-channel causes remains — name-corroborated death and
affirmative recreation — and is forgone only where no sound local evidence can exist. Pinned by
`AdminFactsSourceDebounceTest#anIdWithNoKnownNameIsNeverConfirmedDeadOnTimeAlone` (red on the pre-fix tree at
four windows).

### D76 — Lost ordering state under surviving Streams offsets refuses at start

**Context**

The audit found that `priorState`, keyed solely on the ordering changelog existing, silently misclassifies one
shape: the changelog absent (operator deletion — including the recorded remedies' "reset the process's state
and group offsets deliberately" followed halfway, state deleted but offsets kept — or disaster) while the
group's committed offsets survive. The start then ran as a genuinely first start: the scans were skipped, the
fast path adopted the surviving offsets, Streams recreated the changelog empty, and the process resumed mid-log
with an empty engine — every emission under-expressing the causes of everything delivered before the loss
(Structural 15, downstream Safety 1), held messages gone (Liveness 5), the delivered-past clamp erased
(Structural 16). Every committed step writes ordering state and read positions atomically (Host obligation 3),
so offsets stamped by a Streams execution prove the changelog existed; its absence is a detected Host
obligation 5 breach, and the Host-obligations preamble requires failing closed on detection.

**Decision**

The bootstrap stamps every offset it commits with its own marker (`parsley.bootstrap`, in the offset
metadata). When no prior state is found, any committed group offset carrying anything other than that stamp
refuses the start (`ORDERING_STATE_LOST`): a non-marker stamp is a prior Kafka Streams execution's (Streams
overwrites offset metadata with its own encoded stamp on every commit — D33 records the overwrite, here
load-bearing in reverse), and bare metadata is external tooling's; either way the offsets were not left by a
crashed first-start bootstrap, and the ordering state that must have accompanied a prior execution is gone.
Keying on our own stamp rather than on Streams' stamp being non-empty makes the refusal hold by construction
under toolchain drift: a future Streams that committed empty metadata would still refuse, not silently resume.

Two review corrections to this record's earlier revision are folded in. The scan covers every offset in the
group, not only the currently-declared partitions — a declaration change alongside the state loss must not
hide a formerly-received partition's evidence. And because the admin listing silently omits any partition
whose offset has a pending transactional commit (partition-level `UNSTABLE_OFFSET_COMMIT` is skipped, not
failed, by the admin client), the slow path re-runs the refusal against the group member's own `committed()`
fetch, which the consumer always issues transaction-stable and retries until pending commits resolve.

**Alternatives**

* Refusing on any offsets-without-changelog — rejected: refuses the crashed-bootstrap recovery, a legitimate
  first-start path with no state to lose.
* A persisted first-start marker elsewhere — rejected: any marker co-located with the state shares the
  state's fate.
* Keying on "non-empty metadata means Streams" without a bootstrap stamp (this record's earlier revision) —
  superseded: it fails open if a Streams version ever commits empty metadata, and it cannot tell external
  tooling's bare offsets from a crashed bootstrap's.

**Cost**

Offsets pre-seeded into the group by external tooling before a first start are now refused rather than
adopted — deliberate: initial positions are declared through the API (D9), and the refusal's remedy (delete
the group's offsets) is stated in its message. Pinned behaviourally:
`BootstrapIntegrationTest#lostOrderingChangelogWithSurvivingOffsetsRefusesToStart` fails if the refusal
regresses to the silent empty resume, and `#bootstrapCommittedOffsetsWithoutAChangelogStillStart` fails if it
overreaches into bootstrap crash recovery. Residual: a formerly-received partition whose offset is pending a
transactional commit at the pre-check instant, on a start whose declaration no longer names it, is invisible
to both checks — reaching it needs the changelog loss, the declaration change and the crash mid-commit to
coincide; recorded rather than papered over.

### D77 — A report/feed contradiction has its own reason; OUT_OF_ORDER_FEED is feed order alone

**Context**

The audit found the engine's covered-position branch (a feed at a position at or below `fedUpTo`, above the
session floor) diagnosed as `OUT_OF_ORDER_FEED` with a message asserting the host "must feed each channel in
increasing position order" — false on both counts in the branch's most reachable case. In-execution feed order
is checked separately (D45); this branch fires when a read-position report contradicts a later feed, which is
either a false report (Host obligation 2 breach) or routine supersession: a session-timed-out zombie's
background facts round reads the group's committed offsets, sees its successor's progress, and advances
coverage past records still buffered in the zombie's own consumer. The stop is correct either way — the two are
locally indistinguishable, and a detected apparent breach must fail closed — but the diagnosis blamed a
compliant host (Operational 6) and presented a stop that does not recur on restart as a deliberate refusal
(Operational 1, D55's supervisor contract).

**Decision**

The branch throws `COVERED_POSITION_FED`, its message naming the contradiction, both causes, and that a
superseded execution's step cannot commit (Host obligation 6), a restart recovers, and the refusal then does
not recur. `OUT_OF_ORDER_FEED` keeps the in-execution order breach and the dead-channel re-feed. A new
constant, not a rename: supervisors key on `refusalReason` (D55, D72), and additions are compatible where
renames break.

**Alternatives**

* Suppressing the stop when supersession is plausible — rejected: locally indistinguishable from a false
  report, whose detected-breach duty stands; and the zombie's own state is rolled back regardless.
* Confirming supersession via broker group queries before throwing — rejected: an admin round-trip on the feed
  path to improve only a diagnosis.

**Cost**

Supervisors that treat every fail-closed stop as permanent restart one recoverable case needlessly; the reason
and message now say which case they are in. Pinned by
`ProcessEngineTest#feedAtAReportCoveredPositionFailsClosedAsCoveredPositionFed`.

### D78 — Compacted received topics are an Assumption 10 reliance; the probe residual is recorded

**Context**

docs/delivery.md names compaction among the sources of never-yielding positions, but no record covered the
reliance (Structural 20). On a compacted received topic the cleaner discards committed mid-log records
irrespective of consumer lag, so Assumption 10 — "retention covers consumer lag, so that a cause is never aged
out before its effect is delivered" — cannot hold by construction. The audit traced the sharpest consequence
through the trailing-run probe (D35): a record fetched into the main consumer's buffer and compacted away
before the probe polls yields a probe report claiming the position never yields; the buffered record then
arrives and the engine stops on the contradiction. In a narrower window (the record still servable after the
probe missed it), a post-restart re-feed lands at or below the session floor and is dropped as a replay though
never delivered.

**Decision**

Recorded as an accepted residual of relying on Assumption 10, per D26's pattern: the failure mode is a loud
stop — now honestly diagnosed as `COVERED_POSITION_FED` (D77) rather than a host feed-order accusation — plus
the narrow silent-drop window, which D10's replay dedupe cannot distinguish from a legitimate rewind. No
mechanism changes.

**Alternatives**

* Skipping the probe on compacted topics — rejected: forgoes D35's liveness fix exactly where trailing
  transaction markers make it most needed, turning a routine restart back into a permanent stall.
* Delivery-gating records on a post-fetch identity/log re-check — rejected on the grounds D44 already
  recorded: per-record round-trips to close a window bounded by cleaner timing.

**Cost**

The residual window stands, bounded by the cleaner's timing on a topic shape the spec's assumptions do not
cover; named rather than papered over.

### D79 — Read-path hardenings: stable bootstrap reads, true-end changelog scan, offsets inside the confirmed window

**Context**

Three read-path edges surfaced by the audit, each a window rather than a demonstrated failure. First, the
bootstrap's offset reads (`listConsumerGroupOffsets` pre-check; the committer's `committed()`) read the last
stable offset without requiring stability, so a pending transactional commit from a live lifetime could be
read as absent-or-old — every write remains generation-fenced (D48), but the reads could observe state a live
lifetime was replacing. Second, the changelog scan bounded its read by the last stable offset: a task moved
between stream threads mid-run leaves the predecessor's producer able to die with a transaction open below
records the successor committed, and a bootstrap racing the broker's transaction abort (bounded by the
producer transaction timeout) would silently miss the committed tail — including held entries the stranded
scan exists to see. Third, the facts round fetched the group's committed offsets after the confirming
describe, leaving the one name-keyed query outside the identity-confirmed window that D22 built for log
starts.

**Decision**

The pre-check lists offsets with `requireStable(true)`. Review of the pinned admin client corrected this
record's earlier reading of what that buys: the admin handler treats a partition-level
`UNSTABLE_OFFSET_COMMIT` as a partition to skip, not an error to surface, so an unstable partition simply
vanishes from the listing rather than failing it — which is why the D76 refusal is re-run on the slow path
against the group member's own `committed()` fetch. That fetch needs no configuration at all: the consumer
issues every `committed()` as a transaction-stable OffsetFetch and retries while a pending commit is deciding
(verified in kafka-clients 4.3.1, `ConsumerCoordinator#sendOffsetFetchRequest`; an earlier revision of this
record set `isolation.level=read_committed` on the committer for this, an inert config since the member never
fetches records — removed, with the real mechanism documented at the seam).

`readOrderingChangelog` targets the read_uncommitted end offsets — requested explicitly, since the choice is
load-bearing — and tolerates empty polls up to the stall deadline (30s, comfortably above the 10s default
transaction timeout), so an orphaned open transaction resolves before the scan concludes rather than
truncating it. A partition that reaches its snapshot end is paused: without that, a live writer on one
partition would keep resetting the shared stall deadline while another partition sat pinned below its end,
turning the promised loud stall into an indefinite hang. The committed-offsets fetch moves before the
confirming describe, putting both name-keyed queries inside the same confirmed window.

**Alternatives**

* Accepting the LSO bound with the window recorded — rejected: the fix is a different bound in the same loop.
* A dedicated hanging-transaction integration test — declined: deterministically orphaning a transactional
  producer inside the suite costs more flake than the assertion earns; recorded honestly instead.

**Cost**

A producer transaction timeout configured beyond the stall deadline fails the start loudly instead of
silently truncating the scan. The reorder is behaviourally invisible outside the recreation race it closes.

### D80 — The membership protocol joins the unoverridable set; extends D37/D51, and the promised revisit

**Context**

`group.protocol` (and its companion `group.remote.assignor`) selects the group membership, assignment and
fencing semantics under KIP-848. D48's bootstrap fence is argued and broker-verified on classic-protocol
generation fencing, and the committer's join-conflict handling catches the classic protocol's failure shape.
On the pinned toolchain, setting `group.protocol=consumer` happens to fail loudly — the new protocol refuses
explicitly-set `session.timeout.ms`, which the committer always sets — but that refusal is an accident of
client validation, surfacing as a generic startup failure, and a future toolchain that accepts the protocol
silently would put an unverified fencing story under the safety argument. By D37/D51's membership rule — the
owned set is the configs whose documented use bears on the guarantees — the protocol selector belongs in it.

**Decision**

`group.protocol` and `group.remote.assignor` join `FORBIDDEN_SUFFIXES`, refused under any prefix at build
time. This is the deny-list's third growth, the trigger D51 named for revisiting the allow-list. Revisited and
declined again: each addition has come from an audit naming a specific key whose documented use touches the
guarantees, the list's growth rate remains three entries across the project's hardening history, and an
allow-list would still make every harmless broker and tuning knob a compatibility decision — the cost D37
recorded, unchanged. The revisit stands re-armed: a fourth addition should reopen it.

**Alternatives**

* Allow-listing known-safe keys — rejected again on D37's grounds, now with the growth-rate evidence.
* Relying on the pinned toolchain's accidental refusal — rejected: an accident is not a guarantee, and its
  failure shape names nothing.

**Cost**

Applications cannot opt into the new consumer rebalance protocol, or Streams' early-access protocol, through
parsley — by design until a record argues the fencing story afresh.

### D81 — Seam and runtime diagnoses name their conditions; extends D73's taxonomy

**Context**

Audit sweep of stops that reached operators without their condition named (Operational 1/6). A handler
returning null `Effects` — a seam-contract breach that recurs identically on restart — threw a bare
`IllegalStateException`, so `status()` showed an empty `refusalReason` and `stoppedDeliberately()` false. A
stored state value the declared serde can no longer decode, and a state-read key the declared serde cannot
serialize, stopped the process with the codec's raw exception, unnamed, while every adjacent path (delivery
payloads, effect payloads) carries a reason — and being thrown inside the handler's frame, an application
catch could swallow the store read's failure where the undeclared-store refusal is latched. At the runtime,
`recordFailure` attributed every `NoOffsetForPartitionException` to a mid-run partition-shape change —
mislabeling mid-run offset removal — and an `OffsetOutOfRangeException` (retention passing surviving committed
offsets, the loud half of Safety 8's guard) got no parsley diagnosis at all.

**Decision**

Null effects throw `HANDLER_RETURNED_NULL_EFFECTS`. State-read codec failures throw
`APPLICATION_PAYLOAD_UNSERIALIZABLE` (key) and `APPLICATION_PAYLOAD_UNDECODABLE` (value), latched through the
reader exactly as the undeclared-store refusal is, so an application catch cannot commit the step.
`recordFailure` names the out-of-range condition (Safety 8, with the deliberate-reset remedy) and splits the
no-offset diagnosis into its two real causes.

**Alternatives**

* A distinct reason for stored-state decode failures — rejected: D13 already establishes payload-codec
  failure as `APPLICATION_PAYLOAD_UNDECODABLE`; the store read is the same condition at a different seam.

**Cost**

None of substance. Pinned by `TopologyWiringTest#nullEffectsFromAHandlerFailClosedWithTheirOwnReason` and
`#undecodableStoredStateValueFailsClosedEvenWhenSwallowed`.

### D82 — Hand-built consumers are pinned against mutating the cluster

**Context**

Audit finding (kafka-layer audit, C1/N1). The consumer default leaves `allow.auto.create.topics` true, so
a bare metadata request against a broker with `auto.create.topics.enable=true` (the broker default) silently
creates the topic it asks about. Kafka Streams pins the config false for every consumer it builds
(`NON_CONFIGURABLE_CONSUMER_DEFAULT_CONFIGS`, verified in kafka-streams 4.3.1) precisely so a client never
mutates the cluster — and the three consumers this layer builds by hand carried no such pin. Each one's
metadata requests touch a topic whose absence is load-bearing: the bootstrap's changelog reader asks about
the ordering changelog whose record content is the prior-state evidence, so a deletion racing the start
could be resurrected as an empty impostor that passes every prior-state refusal and resumes mid-log with an
empty engine — the ORDERING_STATE_LOST catastrophe D76 refuses, reached around its guard; the bootstrap
member subscribes to received topics resolved moments earlier; and the probe asks about topics
mid-deletion, where an auto-created impostor under a new id turns the designed dead-channel settle (D44)
into a spurious CHANNEL_IDENTITY_CHANGED stop manufactured by the client's own side effect. The changelog
reader also ran on the default `auto.offset.reset=latest` — the one consumer in the layer not pinned to
`none` — so a log start advancing mid-scan would silently reset it to the end and truncate the restored
view the refusals read.

**Decision**

Every hand-built consumer pins `allow.auto.create.topics=false`, and the changelog reader additionally pins
`auto.offset.reset=none`. The property maps are composed in package-visible builders
(`ParsleyRuntime.changelogReaderProperties`, `GroupMembershipCommitter.memberProperties`,
`AdminFactsSource.probeProperties`) so the pins are pinned by `ClusterMutationPinningTest` rather than
asserted in comments. With the pin, the deletion race fails the start loudly (the reader's metadata wait
times out and the scan refuses) instead of resuming silently.

**Alternatives**

* Refusing `allow.auto.create.topics` in `ParsleyConfig` instead — insufficient: the default, not an
  override, is the hazard; the layer's own consumers must pin it regardless of configuration.
* An integration test deleting the changelog mid-start — rejected for now: the race window sits between two
  admin calls inside `start()` and cannot be held open deterministically from outside; the property pin plus
  the unit test carries the guard.

**Cost**

None of substance. A start racing a changelog deletion now fails with the reader's timeout rather than a
crisper diagnosis; the failure is loud, transient in shape, and a restart reaches the absent-changelog path.

### D83 — The reserved zero topic ID is undecodable metadata; the facts round tolerates unanswerable ids

**Context**

Audit finding (kafka-layer audit, M2). Nothing rejected an all-zero topic ID arriving in an otherwise
well-formed `parsley.causes` header — the substrate reserves `Uuid.ZERO_UUID` and never assigns it
(`Uuid.randomUuid` excludes reserved ids; `ParsleyRuntime.resolveTopics` refuses it at start under
SUBSTRATE_MISCONFIGURED), so no genuine cause can name it, but a foreign producer can frame one in 33 valid
bytes. Once decoded it merged into the persisted frontier, and every subsequent facts round died at the
opening describe: the admin client answers a zero id locally with `InvalidTopicException`
(`KafkaAdminClient.topicIdIsUnrepresentable`, verified in 4.3.1 sources), which `describeByIds` rethrew where
it tolerates only unknown-topic answers. Facts stopped forever for the task — no settling, no dead or
recreated verdicts, no truncation evidence — surfacing only as a repeating warn, surviving restarts through
the durable frontier, and spreading to every downstream process via re-expression on emissions.

**Decision**

Two independent guards. `CausesCodec.decode` refuses an entry naming the zero topic ID as undecodable
metadata — wire-format constraint 5, a reader-side tightening rather than a grammar change, because no
conforming writer has ever produced such an entry (writers only express channels the substrate named, and
the substrate never names this one). So the id can no longer enter a frontier at all, and the message
carrying it fails closed with UNDECODABLE_METADATA like every other untrustworthy header.
`AdminFactsSource.describeByIds` additionally tolerates `InvalidTopicException` exactly like an
unknown-topic answer, so ordering state persisted before this refusal existed — which can still carry the
id — degrades to "no facts for that channel" instead of aborting every round.

**Alternatives**

* Refusing in `ChannelId`'s constructor — rejected: the constructor also serves decode paths that must
  report *undecodable metadata* with position context, and an `IllegalArgumentException` there would
  surface as a malformed-header catch-all rather than the named condition.
* Tolerating in `describeByIds` alone — rejected: the frontier would still carry and re-express the
  ghost id forever, costing budget bytes on every emission and poisoning downstream frontiers.

**Cost**

A message whose forged header names the zero id now stops the process (fail closed) instead of delivering
with the ghost merged. That is the project's stated preference: undecodable metadata is a reason to stop.
Pinned by `CausesCodecTest#rejectsZeroTopicId` and
`IdentityIntegrationTest#zeroTopicIdInTheFrontierDoesNotAbortTheFactsRound` (real admin client, real
local InvalidTopicException answer).

### D84 — Prior state is keyed on ordering records, and prior-state describes are corroborated; extends D76

**Context**

Audit findings (kafka-layer audit, M3/N4/N5), three weaknesses in one determination. First, `priorState` was
keyed on the ordering changelog *topic* existing, but D76's refusal guards state, not topics: a changelog
emptied of its records — `kafka-delete-records`, or a cleanup-policy excursion under a runbook reset followed
halfway — passed every prior-state check vacuously, never consulted the bootstrap stamp (its guard is
conditioned on no prior state), and resumed mid-log with an empty engine: the precise ORDERING_STATE_LOST
fail-open D76 closes for the deleted-topic shape, reachable through record deletion. Every committed step's
transaction wrote at least the store's version entry, which compaction retains, so "exists but recordless"
is as detectable a loss shape as "absent". Second, absence itself rested on a single describe answer: one
transient unknown-topic response — served from one broker's possibly lagging metadata view, the exact answer
shape D44/D75 refuse to trust for deletion — flipped `priorState` to false and misdiagnosed a healthy start
as state loss, with a remedy that deletes offsets. Third, the flag was fixed before the offsets were listed,
and a pause between the two statements (SPEC Fault model 2) spans any concurrent sibling's first commit, so
the refusal could assert "the changelog does not exist" about a changelog that now did.

**Decision**

Three coupled changes. `priorState` is `!orderingState.isEmpty()`: the changelog is read whenever the topic
exists, and only committed records constitute prior state; the width refusal still keys on the topic, whose
partition count outlives its records. `describeChangelog` concludes absence only from three consistent
unknown answers spaced half a second apart — a genuine first start pays the extra describes once. And
`refuseLostOrderingState` looks again before refusing: if the changelog now exists with records, the refusal
is a transient "retry this start" naming the concurrent-lifetime condition, never a state-loss diagnosis
whose remedy would delete offsets a healthy sibling just wrote; when it does refuse, the message names the
shape it found ("does not exist" against "exists but holds no ordering records").

**Alternatives**

* Refusing "exists but empty" unconditionally, without the stamp check — rejected: a first-start bootstrap
  that crashed after Streams created the (still-empty) changelog but before any commit is a legitimate
  recovery, distinguished exactly by every offset carrying the bootstrap stamp.
* A confirmation window for the describe, as the facts source keeps for deletion — rejected: start() is a
  synchronous path with no rounds to observe across; bounded re-describes are the same evidence standard
  scaled to a start-time budget.

**Cost**

A genuine first start performs two extra describes (~1s). The lost-state refusal path re-reads a changelog
it already read; the path is terminal. Pinned by
`BootstrapIntegrationTest#emptiedChangelogWithSurvivingOffsetsRefusesToStart` (would have resumed silently
before this record) alongside D76's three existing pins, which all still hold.

### D85 — Verdict windows require observed continuity; recreation is debounced; contradicted verdicts are rescinded (extends D44/D75)

**Context**

Audit findings (kafka-layer audit, M4/M5/M6), three unsoundnesses in the dead/recreated verdict machinery.
First, D44's window promised death confirmed by *continuous* corroboration, but the implementation anchored a
timestamp and confirmed on elapsed time at the next name-gone answer: during a blind gap — no round asking
about the id at all, which is routine exactly when answers are least trustworthy, since one process's round
grinding against 10s admin timeouts starves every other process's rounds on the runtime's shared single-thread
facts executor, and a task sitting out a rebalance asks nothing — the anchor silently persisted, so two
isolated stale sightings 3 seconds apart could confirm a live topic dead. A spuriously dead received channel
settles to fed-to-end and releases held messages ahead of causes still coming; a spuriously dead frontier
channel is pruned, under-expressing a live cause on every emission (SPEC Structural 13) — both fail open.
Second, the RECREATED verdict convicted from a single by-name answer, immediately and stickily, on D44's
argument that "a stale view can serve an old binding but cannot invent a new one" — but serving an old binding
is precisely the false positive: when the process's own binding is fresher than the answering broker's
metadata (topic deleted and recreated shortly before the process resolved the new id), one lagging broker
self-consistently answers unknown-by-id and old-id-by-name, convicting the live topic into a permanent
CHANNEL_IDENTITY_CHANGED stop whose remedy tells the operator to redo the reset they just performed. Third,
the recheck loop computed SAME_ID for confirmed-dead ids — the name resolving to the very id the verdict
condemned, affirmative proof of a spurious confirmation since the substrate never reuses a topic id — and
discarded it, holding the verdict against its own contradicting evidence forever.

**Decision**

Confirmation windows carry both an anchor and a latest-observation bound: an observation gap reaching the
window length restarts the window, so maturing always takes an unbroken run of at least three affirmative
observations, and no pair of isolated sightings can confirm anything. The RECREATED verdict goes through the
same windowed corroboration as NAME_GONE, in the first-classification path and in the dead-to-recreated
upgrade alike; any contrary observation (same-id, denied, unavailable, live-by-id) restarts it. And the
recheck acts on SAME_ID: the verdict — dead or recreated — is rescinded with a logged warning, the id
returns to unconfirmed, and a genuine condition reconfirms through a fresh window. To keep confirmed
verdicts recheckable, pinned ids retain their name binding through `forget`; unpinned ids (departed frontier
entries) still drop it and remain non-rescindable, which costs expression size, never safety.

**Alternatives**

* A shorter continuity bound (half the window) — rejected: it would demand more than one observation per
  facts interval to mature at all under the default window of three intervals, making healthy confirmation
  flaky; the window-length bound already forces at least three unbroken observations.
* Failing closed on the SAME_ID contradiction instead of rescinding — rejected: the contradiction proves the
  *verdict* wrong, not the world; an engine that already consumed the spurious verdict fails closed on its
  next feed regardless (OUT_OF_ORDER_FEED on a settled channel), which is the loud half, while rescission
  stops the spurious verdict reaching engines that have not.
* Corroborating recreation by describing the new id — rejected: the new id resolves fine on the lagging
  broker too; freshness of the *binding* is what cannot be asked of one broker, and only sustained
  observation answers it.

**Cost**

A genuine mid-run recreation or deletion is confirmed roughly one window later than before (default: three
facts intervals, floor three seconds). The engine's own guards are unchanged and still fire immediately on
affirmative evidence in the feed path. Pinned by the `AdminFactsSourceDebounceTest` additions and the
reworked `AdminFactsSourceDegradationTest#verdictsRideTheRoundThroughAPartitionOutage`;
`IdentityIntegrationTest#midRunRecreationOfAReceivedTopicStopsTheProcess` still passes on the real broker,
one window later.

### D86 — A partially-covering stable listing is retried before the group join; extends D79

**Context**

Audit finding (kafka-layer audit, M1). D79's requireStable pre-check asks the broker for stable offsets, and
the admin client silently *skips* any partition answering partition-level UNSTABLE_OFFSET_COMMIT — the
behaviour D79 itself records. Against a live sibling instance of the same process, offsets are committed
transactionally every EOS commit interval (100ms by default), so at any listing instant some received
partition is plausibly mid-commit: that partition vanishes from the listing, the containsAll fast path fails,
and the start falls into the bootstrap group join — which, against a live Kafka Streams group, can only cycle
on the protocol conflict until the join deadline (twice the session timeout, ~90 seconds by default) and then
refuse the whole start. Starting a second instance beside a loaded first — routine horizontal scaling, which
`EndToEndIntegrationTest`'s migration test itself relies on — was thus refused probabilistically, with a
90-second hang and a diagnosis pointing nowhere near the cause.

**Decision**

A listing that covers some received partitions but not all is treated as the unstable-skip shape and
re-listed for up to five seconds: pending transactional commits resolve within the transaction timeout
(10 seconds under Streams EOS defaults, typically milliseconds), so a healthy sibling's listing completes
within a retry or two. A listing that stays partial falls through to the join exactly as before — the join
remains the authority on genuinely missing offsets, and the fencing argument (D48) is untouched. A first
start lists nothing for the received set and skips the wait entirely, so the retry taxes only the shapes
that were already heading for a 90-second failure.

**Alternatives**

* Dropping requireStable from the pre-check — rejected: D79 added it so the listing never *acts on* an
  offset a live lifetime is about to replace; existence-checking on an unstable snapshot would reopen that.
* Retrying on any incomplete listing, empty included — rejected: it would tax every genuine first start
  five seconds for nothing; the partial shape is the one that evidences a live group.
* Distinguishing skip-from-missing via the member's committed() fetch before joining — rejected: reading
  committed offsets without membership is exactly what the pre-check does; a stable *fetch* needs the join
  whose cost this record avoids.

**Cost**

A start whose group genuinely has offsets for only some received partitions — a partition added to a
received topic while the group's offsets survive, say — waits five extra seconds before the join it would
have entered anyway. The retry-decision predicate is pinned by `BootstrapPreCheckTest`; the loop is
exercised by every bootstrap integration test through the unchanged fast and join paths.

### D87 — Seam and configuration minors from the kafka-layer audit, in one sweep

**Context**

Audit findings (kafka-layer audit, N2/N3/N6/N7/N8/N9), six contained defects. (N2) `deliver()` reset the
seam-violation latch *after* the delivered payload's deserializers had run — application code that may hold a
reader captured on an earlier delivery — so a refusal latched there was erased and the step committed; the
latch was also a plain field, invisible across threads to application code that (incorrectly but possibly)
holds the reader on another thread. (N9) The read seam handed application deserializers the raw header set,
reserved transport header included, while the write seam serializes before the stamp goes on — contradicting
D56's "invisible in both directions" one frame before `Delivery`'s own filter. (N3) `bootstrap.servers` was
pinnable only in its plain spelling: Streams re-pins it for producers after applying prefixed overrides but
not for consumers, so a `main.consumer.`/`restore.consumer.`/`global.consumer.` spelling pointed a consumer
at a different cluster than the one start() resolved identities against. (N6) A positive sub-millisecond
`factsInterval` passed `build()` and crashed the stream thread at task init (Streams punctuation is
millisecond-grained), unattributed, after the bootstrap had committed. (N7) Session-timeout inheritance
parsed stricter than Kafka's own config parser (no trim; int at one site, long at the other), refusing values
every other client in the process accepts, as a bare NumberFormatException naming nothing. (N8) A held
message's persisted blob — payload, headers and causal metadata together — can exceed the ordering
changelog's `max.message.bytes`, which the metadata budget does not bound; the crash-loop surfaced with only
the substrate's diagnosis.

**Decision**

The latch is checked-and-rethrown at every seam boundary — frame entry, post-deserialization, post-handler,
post-apply — and never blanket-reset mid-frame; the field is volatile. Deserializers receive the application
header view, matching `Delivery` and the write seam. `bootstrap.servers` joins `FORBIDDEN_SUFFIXES`.
`factsInterval` requires at least one millisecond at declaration. Session-timeout resolution is one shared
helper parsing as Kafka does (numbers as numbers, strings trimmed), refusing attributably, with the int-range
check where the value meets the consumer config. `recordFailure` names the record-too-large condition with
the changelog-sizing explanation and its remedy. The held-blob size itself remains unbounded by parsley:
bounding it would refuse holds the broker would accept (the limit is the topic's, raisable by the operator),
so the decision here is diagnosis-only, recorded as such.

**Alternatives**

* Setting `max.message.bytes` on the ordering changelog at creation — rejected: any value parsley picks
  silently overrides broker policy, and the right bound depends on payload sizes parsley cannot know;
  the diagnosis names the two knobs the operator already owns.
* Keeping the latch reset and adding one more recheck before it — rejected: every reset mid-frame is a
  window; check-and-rethrow at boundaries leaves no code between a latch and the next boundary.

**Cost**

None of substance. Pinned by `TopologyWiringTest#deserializerLatchedRefusalFailsTheStepEvenWhenSwallowed`,
`#serializerLatchedRefusalDuringPlanningFailsTheStep` (the post-apply recheck's first executable pin),
`#reservedTransportHeaderIsInvisibleToApplicationDeserializers`, `SessionTimeoutInheritanceTest`,
`ApiValidationTest#subMillisecondFactsIntervalIsRefusedAtDeclaration` and the `bootstrap.servers` additions
to `#guaranteeBearingConfigurationIsUnoverridable`.

### D88 — The audit fixes, reviewed adversarially; corrections to D82, D84, D85 and D87

**Context**

The D82–D87 changes were themselves put through the same adversarial review that produced them: four
independent lenses over the diff, every finding verified against the pinned 4.3.1 sources. Fifteen findings
survived, four of them substantive. (1) D85's continuity rule measured raw spacing between per-round answer
timestamps, but a round's own queries sit inside that spacing — a leaderless partition burning the shared
offset deadline, probe polls on idle channels — so any steady-state round tail at or beyond the window
restarted the window every round and made a genuinely dead or recreated topic permanently unconfirmable: the
held messages whose release needs the verdict are themselves what put the probes on the round's tail, a
self-sustaining silent stall. D85's recorded cost ("confirmed roughly one window later") was wrong under
these conditions. (2) The dead-to-recreated upgrade window lacked the contrary-observation restarts D85's
own Decision paragraph records, so flapping answers at sub-window spacing could mature an upgrade the
first-classification path would have kept restarting — converting a settled dead verdict into a permanent
identity-changed stop. (3) With auto-create pinned off, an unknown-topic metadata answer to the changelog
reader is an immediate empty partition list — not the timeout D82's record claimed — so the reader could
conclude "no records" from one stale broker view: priorState flipped false, and a healthy concurrent
sibling's Streams-stamped offsets then drew an ORDERING_STATE_LOST whose remedy destroys that sibling's
offsets, the exact single-answer trust D84 removed from the describe path, reopened through the read path.
(4) D84's check was whole-topic while the loss is per task: the version entry lives in each task's own
changelog partition, so purging one partition of a multi-partition changelog left the merged view non-empty
and resumed the purged task mid-log with an empty engine.

**Decision**

Window continuity is judged on blind time: the restart interval runs from the previous answer to the moment
the current round began asking — time no round was watching — while time inside a round's own queries is
watched time and extends the window. The upgrade path takes the same contrary-observation restarts as first
classification. The changelog reader corroborates its partition list against the describe the read was keyed
on, refusing a mismatch as a retryable transient; `RetryableStartException` carries every such transient
uncaught through the bootstrap's wrapping catch, so a retry-heals condition is never dressed as a terminal
diagnosis. And the lost-state refusal is per changelog partition: a non-bootstrap-stamped offset on
partition p requires records in changelog partition p, with the whole-topic shapes falling out as every
partition failing.

Recorded corrections without code change. D84's premise is more precisely "a task's *first* committed step
writes the version entry" (later steps need not rewrite it; compaction retains it — the conclusion stands).
D85's "affirmative proof" for SAME_ID rescission overstates: the rescinding answer can itself be stale, so
rescission can flap a genuine dead verdict back to unconfirmed — kept deliberately, because rescission's
failure direction is a delayed settle (liveness) where holding the verdict's is settling a live channel
(safety), and a genuine death reconfirms through the window. Three smaller closures ride along: the reserved
zero topic id is refused when a *restored frontier* names it (UNKNOWN_ORDERING_STATE_FORMAT) — state
persisted before D83's receipt refusal would otherwise re-express the unanswerable ghost forever, D83's
describe tolerance notwithstanding; a reader refusal that propagates out of a payload deserializer or
planning serializer unswallowed keeps its own reason instead of being relabeled as a payload-codec failure;
and the session-timeout helper accepts exactly Kafka's INT parse (an Integer, or a trimmed string holding
one) so the bootstrap can never succeed on a value StreamsConfig then rejects post-bootstrap.

**Alternatives**

* Budgeting the probe tail instead of re-spelling continuity — rejected as the primary fix: it shrinks the
  common tail but leaves the rule wrong (any slow query still erases the window); the blind-time spelling
  makes in-round latency irrelevant by construction. A probe budget remains open as a cadence improvement.
* Debouncing SAME_ID rescission — rejected: delaying rescission extends the fail-open half of a spurious
  verdict to protect against a liveness-only flap.
* Keeping the whole-topic lost-state check with a partition-count side condition — rejected: the
  per-partition rule subsumes it, needs no separate prior-state flag in the refusal, and scans every group
  offset unchanged.

**Cost**

Confirmation can now take two observations when rounds are slower than the window — the pre-D85 semantics
for a continuously-asking source, which is what a slow round is. The per-partition refusal also refuses
externally-committed offsets naming partitions no changelog partition backs, where the whole-topic check
under prior state ignored them; that direction is fail-closed and named. Pinned by
`AdminFactsSourceDebounceTest#inRoundLatencyDoesNotBreakConfirmationContinuity` (red on the D85 spelling),
`#aDeadVerdictUpgradeWindowRestartsOnContraryAnswers` (red on the D85 code),
`BootstrapIntegrationTest#emptiedChangelogPartitionWithSurvivingOffsetsRefusesToStart` (red before the
per-partition rule), `ProcessEngineTest#restoredFrontierNamingTheZeroTopicIdFailsClosed`,
`TopologyWiringTest#unswallowedReaderRefusalInADeserializerKeepsItsReason`, and
`SessionTimeoutInheritanceTest#longTypedValueIsRefusedLikeKafkasOwnParser`.

### D89 — The eviction horizon is pinned by scripted-round evidence; name learning extracted as a seam

**Context**

Issue #95 (gap B): D44's eviction horizon — tracking state evicted only when no task has asked about an
id for eight confirmation windows, floored at five minutes, with declared ids pinned — was implemented
but unpinned: no test failed if the sweep never ran, if it evicted pinned ids, if it dropped the ask
timestamp without the verdict, or if the floor was lost. The scripted-round harness also could not build
a dead verdict on a non-pinned id at all: the name learning that corroboration requires (D75) lives
inside `describeByIds`, the very seam a scripted double overrides.

**Decision**

`AdminFactsSourceEvictionTest` pins the four regression directions through `gather()` results alone —
verdict and learned-name amnesia past the horizon with death re-earned through a fresh window, pinned
survival, re-evaluation of a reappearing topic (whose verdict is sticky by construction inside the
horizon, eviction being its only exit), and retention one second inside the horizon, pinning the
five-minute floor. Because the sweep stamps the current round's ask set before it runs, an id asked in
the current round can never evict; the tests drive later rounds through an unrelated sweeper channel.
The one production change is a pure extraction: package-private `recordLearnedName` pulls the
`topicNamesById` write out of `describeByIds` so a scripted describe honours the same learning contract
as the real one. Each test was verified red under the exact mutation it exists to catch — sweep deleted;
pinned guard dropped; `confirmedDead` retained through the sweep; floor weakened to 8 × window — and
green on the revert.

**Alternatives**

* Reflection over the private tracking maps — rejected: the suite observes behaviour through the seams,
  never fields, and reflective pins survive refactors they should fail.
* Pinning the ids under test via the constructor's known-names map — rejected: pinning is exactly the
  property under test; the non-pinned learning path must be exercised as itself, which is what forced
  the extraction seam.

**Cost**

The sweep's `confirmedRecreated.remove` line is not separately pinned — a mutation dropping only that
removal would survive these tests; a recreated-verdict eviction test needs the same scaffolding again
and can join this class when it earns its keep. The horizon boundary is pinned at one second either
side, not at the exact boundary millisecond.

### D90 — The 80%-of-budget warning is pinned through a latch seam plus one stderr-capture wiring test

**Context**

Issue #95 (gap A): D53 promised a single warning at 80% of the metadata budget, but nothing pinned it:
the threshold could drift and the once-per-process latch could vanish with the suite green — the shape
EVIDENCE.md's standard calls worse than no test. The suite has no log-capture infrastructure,
slf4j-simple exposes no appender API, and mock frameworks are banned.

**Decision**

The threshold and the latch move into `ParsleyProcessor.BudgetAlarm`, a package-private seam
`observeFrontier` delegates to — pure extraction, no behaviour change; the latch deliberately survives
task revival, since D53's "once" is per process, not per incarnation. `BudgetWarningTest` pins the
boundary at exactly 80% in both directions and the latch across repeated consultations through the
seam, and pins the wiring the seam cannot see — that the wall-clock punctuator still consults the
alarm — by driving `TopologyTestDriver` punctuations over a 61-byte frontier inside a 64-byte budget's
[80%, 100%) band and counting the warn line on a swapped `System.err`, where slf4j-simple writes:
exactly one warning across repeated punctuations. All three regression directions were
mutation-verified red before landing.

**Alternatives**

* Log-capture/appender infrastructure — rejected: slf4j-simple has none, and inventing a logging seam
  for one warning outweighs one contained stderr swap behind a specific marker string.
* Seam-only pinning — rejected: it leaves `observeFrontier` free to stop consulting the alarm with the
  suite green.

**Cost**

The wiring test depends on the test logging backend writing warn lines to `System.err` (slf4j-simple's
default under the surefire configuration); a backend swap moves that one test, not the seam tests.
EVIDENCE.md carries no Operational section today, so this entry is the record of what catches the
warning breaking.

### D91 — Mid-run supersession is pinned structurally over ProcessEngine, not through the sim harness

**Context**

Issue #95 (gap C): D77's most reachable COVERED_POSITION_FED case — a superseded execution's facts
round observing its successor's committed progress — was pinned only by a hand-written PositionFacts
(`ProcessEngineTest#feedAtAReportCoveredPositionFailsClosedAsCoveredPositionFed`), which cannot catch
the facts round failing to adopt committed coverage, and pinned nothing about the refusal message's
recovery promise. The sim harness cannot stage the condition: SimProcess holds one engine per lifetime,
MemoryOrderingStore has no fork, ingestFacts derives committed positions from the process's own
progress so a successor-ahead report is structurally unreachable, and Scenario's RefusalLedger would
flag a COVERED_POSITION_FED refusal as a violation.

**Decision**

`SupersessionTest` stages two lifetimes of one logical process directly over ProcessEngine: lifetime
one commits positions 0..2; a successor engine restored from that committed image over an independent
store (forked purely through the OrderingStore surface — an empty-prefix scan copies the just-committed
image, mirroring a second instance restoring from the shared changelog) commits 3..5; the superseded
engine's facts round is built from the successor engine's actual `fedUpTo`, the way
`ParsleyProcessor#probeHints` derives read positions. One test pins the refusal (reason, supersession
named in the message, drop and delivery both loud); the second pins the promise that a restart restored
from the successor's committed state drops the covered replay silently (D10) and delivers the next
position, so the refusal does not recur.

**Alternatives**

* Extending the sim harness with engine forking, successor-ahead facts and a justifiable-refusal ledger
  entry — rejected: three orthogonal harness changes to reach one branch that two direct-engine tests
  reach exactly, and the harness's own-progress facts derivation is a deliberate Host-obligation model,
  not a gap to widen.
* Leaving the fabricated-report pin as sole coverage — rejected: it invents the successor's numbers, so
  a regression in onFacts adopting committed coverage is invisible to it.

**Cost**

The staging duplicates a small restore-and-fork idiom inside one test class; no production or harness
surface changed. The sabotage sweep still has no mode for the silent-drop direction of this refusal
(REDELIVER_REFEEDS bypasses the whole covered-position block, the delivery direction only); a
TREAT_COVERED_FEED_AS_REPLAY mode would be warranted if the sweep is to cover this class, and its
meta-test would have to run directly over ProcessEngine as SupersessionTest does — recorded here, not
taken now.

### D92 — ParsleyRuntime's diagnosis, changelog-read and identity-floor behaviours get unit seams; pins issue #95's gaps E–G

**Context**

Three load-bearing runtime behaviours ran with no unit evidence. `recordFailure`'s diagnosis taxonomy
(D81) and its refusal-retaining merge (D55's supervisor contract) executed only inside the
uncaught-exception handler, so a reordered branch, a shrunk cause-chain bound, or a last-writer-wins
merge would have reached operators unnoticed. `readOrderingChangelog`'s stall deadline,
finished-partition pause and READ_UNCOMMITTED end-offset snapshot (D79) executed only against a live
broker, and D79/D82 already record why the hang shape cannot be held open deterministically from
outside. The zero-topic-ID refusal (SPEC Substrate 1, Assumption 2; relied on by D83) was the one hard
broker-floor tripwire, and EVIDENCE.md honestly recorded it had no executable check.

**Decision**

Three behaviour-preserving package-private extractions, following the D82/D86 precedent
(`changelogReaderProperties`, `preCheckLooksUnstable`): `classifyFailure` — an enum-returning
cause-chain walk with the same 64-link bound and branch order — plus `preferFailClosedDiagnosis`, the
merge remapping; `readToEnds` — the polling loop, taking the consumer, the end-offset snapshot and the
stall deadline, production still passing the 30s constant — plus `changelogEndOffsetIsolation`; and
`requireTopicId`, the per-description refusal `resolveTopics` runs. Pinned by
`RecordFailureDiagnosticsTest`, `ChangelogReadStallTest` (over a hand-rolled `Consumer` double; empty
polls nap, a poll budget turns an endless loop into an assertion failure) and `TopicIdentityFloorTest`;
each test was verified red under the exact regression it catches (eleven recorded trials: every
classification branch deleted, the depth bound shrunk, the merge flipped to last-writer-wins, the stall
throw removed, the pause removed, the isolation flipped, the zero-id check deleted, the reason
renamed). The classification's outward-in precedence — an outer link named before a deeper one is
reached, type checks before the message probe within a link — is now pinned as observed behaviour
rather than left implicit.

**Alternatives**

* Integration tests through `start()` against the embedded broker — rejected: the pause/stall interplay
  needs a partition held below its end while a sibling is live-written, un-holdable deterministically
  from outside, and a healthy 4.3.1 broker can produce neither a zero topic ID nor a 66-link cause
  chain.
* Reflection over the private methods — rejected: the suite's convention is narrow package-private
  seams the production caller actually uses.

**Cost**

None of substance. Five package-private names join `ParsleyRuntime`'s surface; each is a direct
factoring of code that existed, called from the same sites with the same values.

### D93 — The engine's error sites are pinned at reason-and-message level; the per-message budget gate is discriminated by raw length

**Context**

The error-reachability sweep found five ProcessEngine error sites with weak or no pinning. The
undeclared-channel refusal and both shapes of the markDelivered head guard had no test at all: deleting
either guard left the suite green while a message from outside the declared received set was absorbed
silently (causes merged, body held) or a non-head markDelivered removed a mid-buffer hold with no
exception. The emission budget check in `causesHeaderForEmission` was unreachable through any growth
path — receipt and merge both police the budget as the frontier grows — so deleting it was invisible;
its one route is a frontier restored by the constructor, which deliberately carries no budget check,
under a budget smaller than the one the state was committed with. The per-message header-length gate
shares `METADATA_BUDGET_EXCEEDED` with the merged-frontier growth gate, and the existing fresh-channel
test stayed green when the gate alone was deleted. And the dead-channel re-feed's reason and diagnosis
(the `OUT_OF_ORDER_FEED` half D77 left behind; D21's sentinel) were asserted nowhere.

**Decision**

All five pinned in `ProcessEngineTest`, no production change. The undeclared-channel and markDelivered
pins assert exception type, message fragment, and that nothing was absorbed or removed (frontier,
holds, coverage, true head). The emission check is reached the only way it can be: an eleven-channel
frontier committed under the default budget, restored under a 64-byte one — construction and an empty
facts round pass, and `causesHeaderForEmission` stops with the "expressing the causal frontier"
diagnosis. The per-message gate is discriminated by construction: a canonically-encoded header whose
channels already sit in a within-budget frontier can never itself exceed the budget (the encoded size
is affine and monotone in the entry count), so the discriminating message carries the canonical
encoding of exactly the frontier's channels at their frontier positions, padded past the budget — the
growth gate provably cannot fire, and the gate's judgement of raw length *before* any decode is what
produces the budget diagnosis. Deleting the gate alone degrades that refusal to `UNDECODABLE_METADATA`
and turns only the new test red while `#metadataBeyondTheBudgetFailsClosedOnReceipt` stays green — the
recorded differential. The dead-channel re-feed now pins `OUT_OF_ORDER_FEED` and "recorded as no longer
existing" in the existing restart test. Eight mutation trials recorded: both guards deleted, both
budget throws deleted, the reason swapped, three messages garbled — each red, each revert green.

**Alternatives**

* A budget check on the constructor's frontier restore, stopping at restore instead of emission —
  rejected here: a behaviour change beyond a pinning pass. An operator who lowers the budget below an
  already-committed frontier today gets the emission-time stop with its own diagnosis, which is D52's
  documented third enforcement point; restore-time enforcement would need its own decision.
* Pinning the per-message gate with an over-budget *valid* header over fresh channels — rejected: that
  is exactly the existing test's shape, which the growth gate masks with the same Reason.

**Cost**

The discriminating header is not decodable (padding), so the test leans on the gate's
judge-length-before-decode order; a deliberate reversal of that order would need to rewrite the test
alongside the decision that reversed it.

### D94 — Codec refusal diagnoses are pinned message-level; the position floor is pinned at the value objects

**Context**

The codec cluster's refusals were pinned only at the exception-type level. `CausesCodec.decode`'s two
catch clauses — the BufferUnderflowException wrap ("truncated causes header") and the
IllegalArgumentException wrap ("malformed causes header: ...") — had no test at all: deleting either
let a raw runtime exception escape into the receive path with the suite green, unwinding D8's
classified stop for exactly the inputs D3 calls hostile. The negative-count and per-entry
negative-position guards share their signature with sibling checks (the count/length mismatch below;
`Causes.of`'s backstop through the IAE wrapper), so a deleted guard still refused — with the wrong
diagnosis — and every type-level test stayed green, the shape EVIDENCE.md's standard calls worse than
an empty cell. `StoreCodec.decodeHeld`'s length diagnoses had the same shape against its catch-all
wrap, degrading a named stop (`docs/failing-closed.md`; D81) to a bare "corrupt held blob". And the
value-object floor — `ReceivedMessage`'s negative-position refusal and `Causes.of`'s negative/null
refusal, the very backstop the codec leans on — was itself unpinned.

**Decision**

Pure test additions; no production change, every site being reachable through crafted bytes or direct
construction. `CausesCodecTest` gains two classification pins (a sub-5-byte header, and an entry whose
negative partition only ChannelId's constructor refuses, must both come back as
UndecodableMetadataException, never raw) and two message-level pins ("negative cause count -1";
"negative position -5 on <channel>" from the per-entry check, not the backstop's wrapped text).
`StoreCodecCorruptionTest` gains message-level pins for the header-value-length and readSizedBytes
diagnoses, exact against validBlob()'s documented layout. `PositionRefusalTest` pins the value-object
floor and records `Causes.of` as the backstop behind the codec's per-entry check. Eight mutations
verified red and reverted green; in the shared-signature trials the type-level siblings stayed green
under the deleted guard, which is precisely the gap the message pins close.

**Alternatives**

* Type-level assertions only — rejected: every deleted-guard mutation survives them through the sibling
  or backstop refusal.
* Full message equality on the store-blob diagnoses — rejected: the discriminating fragment (field
  name, length, bytes remaining) is the load-bearing part; wholesale equality couples tests to
  incidental prefix wording without catching more.

**Cost**

The truncation pin depends on a decode-order fact its Javadoc records: the count/length check refuses
longer truncations arithmetically before an entry read can underflow, so the underflow catch's only
live entrances are the version byte and the count int. Adjacent non-assigned diagnoses remain
type-pinned only: CausesCodec's count/length mismatch text, and StoreCodec's header-count and
cause-count texts — each would need its own message pin to catch a garbled diagnosis.

### D95 — The processor's refusal sites, the bootstrap join wait and the facts round's abort paths get direct pins

**Context**

The error-reachability sweep found eight error sites in this cluster with no unit evidence. In
`ParsleyProcessor`, the write-key serializer returning null (`planWrite`'s D81 refusal), the read-key
serializer that throws (the D87 latch site — its returns-null sibling was pinned, the throwing shape
was not) and the undeclared-topic guard in `process` were each deletable with the suite green. In
`GroupMembershipCommitter`, nothing pinned the sub-millisecond session-timeout refusal, the join
deadline — the site whose deletion turns a diagnosed bootstrap failure into an infinite hang
(Operational 2) — its protocol-conflict diagnosis and cause, or the interrupted-backoff refusal. In
`AdminFactsSource`, the rethrow that aborts a round on a describe failure outside D83's
unknown/invalid tolerance was unreachable by the scripted-round tests, which override `describeByIds`
wholesale and replace the very classification under test; the interrupt rethrow out of the
earliest-offset wait was equally unpinned.

**Decision**

Two behaviour-preserving extractions, following the D89/D92 precedent. `GroupMembershipCommitter.join`'s
wait loop becomes the package-private static `awaitAssignment(Consumer, Duration)` — the identical
statement sequence, the consumer arriving as its interface — so `BootstrapMemberJoinTest` pins the
deadline, its protocol-conflict explanation and cause, and the interrupt refusal over a hand-rolled
never-assigned consumer double (empty polls nap; a poll budget turns an endless loop into an assertion
failure; the interrupt is staged by self-interrupting ahead of the backoff's sleep).
`AdminFactsSource.describeByIds`' admin call becomes `describeByIdFutures`, mirroring
`earliestOffsetFutures`, so `AdminFactsSourceRoundAbortTest` fails a `KafkaFutureImpl` through the real
tolerate-or-abort classification: the abort resets the confirmation streak — a name-gone run
interrupted by the outage reopens rather than matures, an unbroken post-outage run still confirms —
and a second test interrupts a gather thread blocked on an incomplete offset future and asserts the
round aborts interrupted instead of completing under the per-partition warning. `ParsleyProcessor`
needed no seam: `TopologyWiringTest` stages all three sites through the driver, the undeclared-topic
guard via the width arm (a zero-width `TopicInfo` against the driver's task 0 exercises the same
`partition < width` predicate as a task numbered at or above a real topic's width — the only reachable
spelling, since the driver refuses records on unconnected topics). Every test was verified red under
the exact mutation it exists to catch — nine recorded trials — and green on the revert.

**Alternatives**

* A MockAdminClient or mock-framework describe — rejected: the suite's convention is hand-rolled
  doubles behind narrow package-private seams, and the future-level seam keeps the classification
  itself real where a scripted `describeByIds` replaces it.
* Pinning the join deadline against the embedded broker with a held group — rejected: the
  contested-group shape needs a live Streams member ground against for the full deadline; the consumer
  double stages the same loop deterministically in milliseconds, and `BootstrapIntegrationTest` keeps
  the real-broker join covered.
* Reflection over the private wait loop instead of the extraction — rejected: the suite's convention is
  seams the production caller actually uses.

**Cost**

Two package-private names join the surface, each a direct factoring of code that existed, called from
the same sites with the same values. The width-arm staging leans on `TopicInfo` not validating a
positive width; if it ever does, that test needs a task numbered above zero instead. The conflict-path
join tests spend real time in the loop's genuine 500ms backoff. EVIDENCE.md still carries no
Operational section, so this entry is the record of what catches the join deadline and its diagnoses
breaking.

### D96 — Start-path refusal decisions are pinned through scripted functional seams; extends D92 into the determination and recheck loops

**Context**

Four start-path decisions ran with no unit evidence, three of them safety-bearing. describeChangelog's
corroboration loop (D84) could regress to concluding absence from a generic transient — a timeout
resuming a stateful process as a first start — with nothing red. The changelog reader's width
corroboration (D88) could be deleted and a lagging broker's empty metadata answer scanned vacuously.
refuseLostOrderingState's pre-refusal second look (D84) could be deleted or inverted, misdiagnosing a
healthy concurrent sibling as ORDERING_STATE_LOST, whose printed remedy deletes the offsets that
sibling just committed. And the D86 pre-check retry was pinned only through its shape predicate, not
its relist wiring or interrupted arm.

**Decision**

Four behaviour-preserving package-private extractions, following D92's precedent, each a direct
factoring called from the same site with the same values: describeChangelogCorroborated over a
hand-rolled ChangelogDescribe (the retry loop verbatim); requireCorroboratedWidth (the comparison and
retryable refusal verbatim); refuseLostOrderingState made static over a ChangelogRecheck returning
Optional<ChangelogView> — production supplies describeChangelog(...).map(readOrderingChangelog(...)),
preserving the lazy per-entry re-read and the presence-to-shape mapping; and awaitStablePreCheck over a
StableOffsetListing. Pinned by PriorStateDeterminationTest, LostOrderingStateRecheckTest, and additions
to ChangelogReadStallTest and BootstrapPreCheckTest, plus one embedded-broker case for the
declared-topics resolution refusal
(BootstrapIntegrationTest#aDeclaredTopicThatDoesNotExistRefusesToStartNamingTheResolutionFailure).
Fourteen recorded mutation trials, each red under its exact regression and green on revert: the
generic-failure arm falling through to absent, single-answer absence, both swallowed interrupts, the
width guard deleted and inverted, the recheck throw deleted and inverted, the shape and provenance
strings garbled, the bootstrap-stamp and records-present skips deleted, the relist loop inverted, and
the resolution wrap swapped for a bare rethrow.

**Alternatives**

* Driving the concurrent-lifetime recheck through the embedded broker — rejected: it needs a sibling's
  commit to land in the window between the first changelog read and the offset listing, an interleaving
  no external test can hold open deterministically.
* Pinning the three remaining catch-wrap diagnosis shells ("prior ordering state could not be read",
  "initial read positions could not be established", "committed read positions could not be listed") —
  declined for now: each needs a consumer-factory, committer-factory or Admin seam solely to reach a
  wrap whose interior refusals are already pinned, and a wrap-helper unit test that feeds an exception
  to a rethrow proves nothing. The Admin-injection seam through listStableOffsets is the refactor to
  take if the third is ever judged worth a test.

**Cost**

Four package-private names and three one-method functional interfaces join ParsleyRuntime's surface.
The corroborated-absence test sleeps through the loop's real half-second backoffs (~1s once per suite
run). The seams pin the decisions, not their call sites: deleting a seam's invocation in production is
caught only by the integration paths that reach it, the same residual D92 carries.

### D97 — Review corrections to the D89–D96 pins: two trials that did not discriminate, seam waits parameterized, wiring gaps closed, two claims narrowed (corrects D89, D92, D95, D96)

**Context**

An adversarial nine-angle review of the D89–D96 pinning work found three pins whose recorded
mutation trials did not hold at the commit they were recorded for, plus wiring the extractions had
left uncovered. (1) `classifyFailure`'s depth pin probed depth 65 where the first excluded depth is
64, so D92's trial covered only the shrink direction — growing the bound to 65 passed, silently
desynchronising it from `ParsleyFailClosedException.findIn`'s 64 (confirmed empirically before
fixing). (2) `AdminFactsSourceRoundAbortTest`'s streak-reset leg landed its post-outage sighting at
exactly one confirmation window, where `observeWindow`'s blind-gap rule restarts the window
regardless of the abort's clears — deleting `deadWindows.clear()`/`recreatedWindows.clear()` left
the suite green, so D95's recorded red trial for that mutation did not hold (confirmed empirically).
(3) The production `ChangelogRecheck` lambda in `commitInitialPositions` was unpinned: replacing it
with the stale first view passed the entire suite, the deleted-topic integration tests asserting
`reason()` only (confirmed empirically). Additionally: `readToEnds` rendered sub-second stall
deadlines as "0s"; `describeChangelogCorroborated`, `awaitStablePreCheck` and `awaitAssignment`
hardcoded their sleeps so their scripted tests paid ~3s of real waits and the pre-check's give-up
arm was untestable; `awaitStablePreCheck` took both an eager first listing and the relist seam, so
production spelled `listStableOffsets` twice with the first listing's wiring uncovered; and
`recordFailure`'s enum-to-remedy switch and merge wiring were unpinned behind the D92 extraction.

**Decision**

The depth probe moved to depth 64 (`depthSixtyThreeIsClassifiedAndDepthSixtyFourIsNot`); both bound
directions now red. The streak-reset test lands its first post-outage sighting strictly inside the
window and asserts no confirmation where an un-reset anchor would mature; the clears-deleted
mutation is now red. The two deleted-topic integration tests assert the missing-topic loss shape,
which the stale-view recheck mutation flips — red now, green before. Sub-second stall deadlines
render in milliseconds (the production 30s message is byte-identical). All three loop seams take
their waits as parameters — production passes the exact prior constants — and the pre-check's
give-up arm is pinned (a listing partial past the budget is adopted for the join, per D86) through
the now-single `StableOffsetListing` seam. `recordFailure` and the runtime's Admin-bearing
constructor were relaxed to package-private so the merge wiring (a refusal survives surrounding
transients) and the per-diagnosis remedy lines (stderr capture, D90's technique) are pinned
directly. The `recordLearnedName` seam is retired (corrects D89): the eviction double now scripts
`describeByIdFutures` — the D95 seam — so the real `describeByIds` performs both classification and
learning; all four eviction trials were re-run red/green after the rework. Two claims were narrowed
rather than forced: the isolation pin covers the spelling of `changelogEndOffsetIsolation` only
(READ_UNCOMMITTED is also the `ListOffsetsOptions` default, and nothing pins the `listOffsets` call
site), and the width-corroboration pin covers the decision, not its call site — both residuals
recorded here, both closable by the Admin-level seam D96 already names. Shared test scaffolding was
consolidated (`RefusingConsumer`, `ScriptedAdminFacts`, `TestChains` with findIn's 64-link bound,
`StartPathFixtures`, `EngineTestFactory.plain`), with the four riskiest pins re-verified by
mutation afterwards.

**Alternatives**

* Treating the two non-discriminating trials as harmless because the behaviours still hold —
  rejected: EVIDENCE.md's standard calls a test that stays green when the behaviour breaks worse
  than no test, and both entries claimed trials that were not discriminating at the recorded commit.
* Pinning the isolation and width-corroboration call sites now — rejected as disproportionate for
  this pass: each needs an Admin/consumer-level injection seam; D96's Alternatives already name that
  refactor as the one to take.

**Cost**

`ParsleyRuntime`'s constructor and `recordFailure` are package-private for the wiring pins — the
test constructs the runtime with a null Admin the failure path never touches. The streak-reset
test's final confirmation exercises `observeWindow`'s `>=` boundary at equality; a deliberate move
to a strict bound would need that assertion revisited alongside the decision.

### D98 — Causes wire format: entries grouped by topic id, varint structural fields, fixed-width positions (supersedes D3's value grammar and D52's affine-size check)

*Superseded in part by D101: the version byte this record assigned (`0x02`, with `0x01`
retired) was renumbered to `0x01` pre-release, before any released message carried either.
The grammar itself stands unchanged.*

**Context**

The causal frontier rides on every message, and its steady-state size approaches the sum of
partition counts over the transitive upstream closure (docs/model.md). D3's flat grammar spent
the dominant term of that law twice over: the 16-byte topic ID was repeated once per partition,
and the structural fields held fixed 4-byte widths for values almost always under 128 — a
frontier naming 32 partitions of one topic wrote that topic's UUID 32 times, 901 bytes where
307 carry the same information. The bytes bite exactly where compression cannot reach: the
growth gate in `mergeFrontier` runs before anything compresses, and the producer-side record
ceiling is judged on uncompressed size (ASSESSMENT 2.4), so batch compression recovers the
redundancy on disk and network but never at either wall. AGENTS.md prefers no change to the
wire format; the argument past that default is that the constant factor is 2.7–2.9× on topics
with tens of named partitions, never worse than the flat grammar for any shape with partition
ids below 2²¹ (past that a wider partition varint costs a single-partition group one byte over
the flat 28, and two bytes at ids of 2²⁸ and above), and it decides whether transports with
hard uncompressed header limits (issue #96) are viable at all. Decisive for the shape of the
change: Parsley is pre-release, and no message carrying the flat grammar exists in any log —
so this lands as a replacement, not a migration. Issue #97 records the full proposal, its
evaluation, and the size table.

**Decision**

The grouped grammar is the wire format, version byte `0x02`, defined normatively in
docs/wire-format.md: topic IDs ascend once each in unsigned order carrying a partition count,
partitions ascend within their group, and the three structural fields — topic count, partition
count, partition id — are minimal unsigned base-128 varints spelling exactly 0 to 2³¹ − 1.
Version byte `0x01` is retired, not reused: readers refuse it as unknown, and no byte ever
names two grammars. Positions stay fixed eight bytes, deliberately: a varint position's width
grows with the log, so encoded size would drift with wall-clock throughput toward a budget
refusal no topology diff explains; fixed positions keep size a stable function of the
topology, which is what belongs behind a budget. Canonical strictness is preserved: the
grouped order equals the channel order (`ChannelId`'s comparison is topic-major unsigned with
a numeric partition tie-break, coinciding with the 20-byte unsigned order for the non-negative
partitions the grammar admits), and the decoder refuses padded varints, over-long varints, and
a fifth byte carrying anything beyond the low three bits — Java's shift discards or misplaces
the surplus, so `85 80 80 80 10` would otherwise silently decode to the same value as `05`,
an aliasing the padding rule alone cannot see.

Size ceases to be affine in the entry count, so D52's merge-site premise — "the codec's size
is affine in the entry count, so the check costs no encode" — is superseded: the engine
maintains the encoded width incrementally at the frontier's three mutation sites (merge,
prune, restore; positions update in place without changing size), with per-topic partition
counts supplying the varint-width deltas, and `frontierBytes()` reads the counter in O(1).
The counter's agreement with `CausesCodec.encode`'s actual bytes is pinned. The receipt
gate's raw-length check is exact again by construction: a canonical header's length *is* its
frontier's encoded size, and size is monotone under subsetting, so one in-budget message can
never inject a beyond-budget frontier.

**Alternatives**

* Keeping the flat grammar — rejected: the repetition sits on the growth law's dominant term,
  and the walls it presses against are the ones compression cannot move.
* Grouping without varints — rejected: single-partition-topic estates regress ~14% (20 bytes
  of group framing to save 16 of repetition); with varints the per-topic cost is 17 + 9p
  against the flat 28p for partition ids and counts below 128, smaller for every p ≥ 1, so
  the varints are what remove the regression.
* Varints on positions too — rejected: a partition at 1,000 msg/s crosses into five varint
  bytes within days, so encoded size would grow with elapsed time and a deployment could hit
  the fail-closed budget with no topology change and nothing to point at in a diff. Checking
  the budget against a fixed-width worst case while the wire shrinks was considered and
  rejected with it: the real bytes still drift where hard external limits live (#96's
  headers), and the 80% warning would fire at different calendar times for identical
  topologies.
* Truncated topic ids (8 bytes) — rejected: the frontier's undeclared topics are resolvable
  only because it carries full UUIDs (`AdminFactsSource` describes by id to prune entries
  below logStart and on dead or recreated topics); truncation makes the bulk of a real
  frontier permanently unprunable against a budget that fail-closes. A reverse-index registry
  restoring the mapping is sound only under the single-cluster assumption (issue #98) and
  lands within 8% of varints at best — not worth a new internal topic, bootstrap path, and
  refusal class.
* Delta-encoding partitions within a group — rejected: it only narrows ids at or above 128
  (about a byte per partition there) and adds another canonical-form rule to specify and pin.
* A dual-read migration — readers accepting both grammars ahead of a staged writer flip —
  built first in this change's history, then removed as vestigial: it exists for a deployed
  fleet whose logs retain old-grammar messages, and none exists. Pre-release, carrying a dead
  grammar's decoder, golden vector and malformation battery forever is cost without a payer.
  The branch history preserves the dual-read implementation should a released format ever
  need the pattern; the reader-first ordering it encoded (every reader accepts the new
  grammar before any writer emits it, external writers included) remains the rule for any
  post-release format change.
* Renumbering the grouped grammar to version byte `0x01` — rejected: version bytes are not
  scarce, and reusing a byte that once named a different grammar makes the record ambiguous
  for no gain.

**Cost**

Byte-calibrated tests are shape-sensitive and were re-derived, not scaled: the growth-gate
pin's budget moved from 80 to 70 (three single-partition topics encode to exactly 80 bytes,
which a strict `>` gate admits), the status-surface refusal's header grew from five
partitions to six (five encode to exactly the 64-byte budget), and the 80%-warning wiring
now rides at 54 of 64 bytes — 84%, with 2.8 bytes of margin above the warning floor where
the flat grammar had 9.8. The flat grammar's negative-count refusal class is gone — a
canonical varint cannot spell a negative — its D81 message-level pin replaced by the varint
refusals. D93's recorded discrimination argument leaned on the affine half of "affine and
monotone in the entry count"; it survives on monotonicity alone, restated in its test's
javadoc. `encodedSize(entries)` left the public surface (pre-release; nothing external can
have adopted it), and any doc-driven implementation of the never-released flat grammar is
orphaned. The maintained counter is state the old arithmetic did not need: three mutation
sites must stay in step, the price of an O(1) budget check over a shape-dependent size, with
the counter-versus-encoder agreement pinned so drift cannot stay green.

**Specification gap**

None. Structural 5 requires a documented, stable, distinguishable representation and
Operational 4 and 5 a bound and observability — all version-neutral, and the spec is right
not to fix an encoding. Structural 11 is satisfied: a group header is factoring, not a new
dependency form, and the grammar has no field a process identity could hide in.

### D99 — Issue #96's session companion lands as its own package, `session`, over the public surface

**Context**

Issue #96 proposes extending the causal frontier past the last consumer: a client carries
its frontier as a server-minted token, writes stamp the validated token as causes, and a
read tier refuses to serve data whose recorded past does not cover the token — session
consistency for a participant outside the protocol. Its build list asks for a companion
token class over the existing public surface plus malformation tests mirroring
`CausesCodecTest`, and it insists the companion must not accrete into `core`. The
specification says nothing about edges beyond the last process, so both the shape of the
companion and where it lives were open. Evaluation against this tree confirmed the issue's
claims with two corrections: `CausesCodec.encodedSize` left the public surface with D98
(sizing outside `core` is `encode(...).length`), and D98's grouped grammar retires the
issue's 28-bytes-per-entry arithmetic — its cookie-budget pessimism, that three
32-partition upstream topics already overflow a 4 KB cookie, is now false by about a
factor of three.

**Decision**

A fourth package, `…/parsley/session`, holding one type: `CausalPast` — parse
(`decode`, delegating to the frozen codec and exactly as strict), merge (pointwise maxima,
immutable values), encode (byte-identical to `CausesCodec.encode`, so a token is a valid
`parsley.causes` header value), and the coverage check. Coverage reuses
`Deliverability.decide` with the token's own channel set as the received set, which
inverts the delivery gate's disposition on purpose: the gate skips a cause on a channel
the process never receives (Liveness 4 requires it), while a read tier must fail closed
over a channel its recorded past cannot verify, because the wrong answer there is a silent
read-your-writes violation that every test of the gate's disposition would call correct.
`CausalPastTest` pins the inversion by asserting both dispositions against the same
inputs; `CausalPastMalformationTest` re-pins every malformation class `CausesCodecTest`
holds, through `CausalPast.decode`, so the parser cannot later be rewritten around the
codec into a salvaging one; `SessionPurityTest` scans the package for host and adapter
references the way `CorePurityTest` scans `core`. Nothing in `core`, `api` or `kafka`
changes, and the engine's private `deliveredPast` stays private: the seam's own-coordinate
substitute under-reports only in the conservative direction, and issue #96 defers any
`Delivery.causalPast()` accessor until measured false blocks justify it.

**Alternatives**

* In `api` — rejected: D64's split is load-bearing because each package has one charter,
  and `api` declares processes. A session participant declares no process; wedging the
  companion in dilutes the surface AGENTS.md tells applications to start from.
* In `core` — rejected: the issue's own scope note, and `core` is the protocol. A type the
  engine never reads does not belong beside the one the engine is.
* A separate Maven module or repository — rejected for now: this repo is deliberately one
  module, and a second artifact means release machinery for a class of a few hundred
  lines. Residency in its own package keeps later extraction mechanical.
* Exposing the engine's delivered past (the issue's first draft) — rejected by the issue
  itself: `frontierSnapshot()` over-approximates (receipt precedes delivery, so it names
  effects still held back), and a `deliveredPast` accessor widens the engine's surface for
  a companion that has a correct, narrower substitute at the seam.
* Returning core's `Deliverability.Verdict` from the coverage check — rejected:
  `isDeliverable()` is the gate's vocabulary, and a read tier asking "may I serve" while
  reading "may I deliver" invites exactly the disposition confusion Boundary 2 warns
  about. `Coverage` carries the same `Blocker` diagnostics under the right question.
* A lenient token parser (accept and re-canonicalise what the codec refuses) — rejected: a
  token decoded from damaged bytes into a weaker frontier weakens the session guarantee
  silently, the same hazard Safety 7 exists to stop on the wire.

**Cost**

The artifact now ships a type the engine never reads, and a fourth package charter to keep
honest — `SessionPurityTest` and this record are the fence. Coverage's fail-closed
disposition makes the availability cost real and permanent where topology is transitive: a
read model recording only own coordinates can never cover a token naming a channel it does
not receive, so the pattern binds read models to receiving written channels, or to
application-level coordinate propagation, until measurement justifies a seam change
(issue #96's step 4). The seam names channels by topic name while `ChannelId` needs the
topic id, so edge participants owe a one-time name-to-id resolution the library does not
provide. And the companion's presence will read as endorsement of the whole issue-#96
programme; Q1 (per-row versus per-model pasts), Q3 (behaviour when behind) and the token
protection work (AEAD, bounding, TTL) remain the application's, undecided and unshipped.

**Specification gap**

None. The specification ends at the process seam and is right to: session guarantees for
non-participants are an application concern, and the spec's own criteria (Liveness 4,
Safety 7) are what force the companion's two deliberate departures — the inverted skip and
the inherited strictness — to be recorded rather than improvised.

### D100 — Review hardenings to the session companion: compile-checked coverage exhaustiveness, identity merges, and shared fences (extends D99)

**Context**

An adversarial review of the D99 companion (nine independent finder angles, each surviving
finding re-verified) found no live bug but a cluster of hardenings, all in the same key:
places where a future, individually reasonable change would silently weaken the companion's
fail-closed posture or let its twin fences drift. Chief among them: `coverageOf` mapped the
sealed `Deliverability.Verdict` with an `instanceof Held` ternary, so a hypothetical third
permitted verdict kind would have meant *covered* — fail-open in the one class whose
documented invariant is that errors are only ever conservative. Alongside it: the
malformation battery and the purity fence were both verbatim copies of their core-side
originals, drift-prone in exactly the mirror properties they exist to hold; the
gate-contrast test's `SettledView` violated the interface's empty-for-unsettled contract
and so pinned `decide`'s incidental evaluation order; `merge(CausalPast)` rebuilt two maps
to return a value equal to an input in the session steady state; and the fence never
guarded the "core's public surface alone" half of the package charter.

**Decision**

Fail-closed made compile-checked: `coverageOf` maps the verdict through a default-less
pattern switch over the sealed interface, so a new permitted verdict kind fails compilation
at this seam rather than silently serving (verified by compile test against a widened
replica). The battery gets one spelling: `CausesMalformationVectors`, a public test-package
catalogue of every malformation class as (family, label, bytes, diagnosis-fragment)
vectors, swept by both decoders — `CausesCodecTest` through `CausesCodec.decode`,
`CausalPastMalformationTest` through `CausalPast.decode`, each per family for its own red
plus a whole-catalogue sweep as the drift fence, so a class added to the catalogue is
enforced on both decoders with no further test change. The purity fence likewise:
`PurityScan` holds the one forbidden-facility list — hardened with `java.util.concurrent`,
`SecureRandom` and `UUID.randomUUID`, all verified absent from the scanned sources today —
and the session scan appends the adapter *and* `api` packages, closing the charter gap.
`merge(CausalPast)` returns the covering side itself when one side already covers the
other, deciding coverage through `coverageOf` so the covers rule keeps one spelling; the
identity return is pinned by `assertSame`, and lets a read tier skip re-encoding an
unchanged token. The contrast test's `SettledView` now honours empty-for-unsettled, so it
stays red on a genuine disposition change and stops dying with an unlabeled
`NullPointerException` on a verdict-identical reordering inside `decide`. `Coverage`'s
null-element `NullPointerException` is documented. The verification docs and AGENTS.md now
name the session companion in the unit layer.

**Alternatives**

* Leaving the `instanceof` ternary — rejected: the review's compile test showed the sealed
  switch turns the fail-open case into a build failure for free, and this codebase's whole
  posture is that the conservative direction must be structural, not remembered.
* Replacing `coverageOf`'s reuse of `Deliverability.decide` with direct iteration — 
  rejected, twice over: D99 records the reuse as deliberate (one safety rule, one
  spelling), and the review refuted the per-entry overhead on materiality against
  budget-bounded frontiers.
* Consolidating the session battery into a single sweep method — rejected: the gate's test
  count never shrinks, and per-family methods keep each refusal class its own red.
* A zero-allocation hand-rolled pre-scan for the merge short-circuit — rejected: the win is
  the identity return, not the constant factor, and deciding "covers" anywhere except
  `coverageOf` would give the rule a second spelling.
* Shipping a public topic-name-to-id resolver, and the collection-gate meet as library
  algebra — deferred, recorded here as the two review findings deliberately not built:
  both widen surfaces (`kafka` and the companion respectively) beyond a hardening round,
  and the meet belongs with whatever future change ships clock machinery at all.

**Cost**

`CausesCodecTest`'s narrative tests keep their own hand-built vectors beside the
catalogue's — deliberate, since D94's exact-message pins stay verbatim, but it means a new
malformation class pinned narratively without a catalogue entry still escapes the mirror;
the catalogue's javadoc names it as the place new classes land. The steady-state merge now
runs up to two coverage scans before a genuine merge pays for three map builds. And the
hardened facility list constrains `core` too: a future core need for `java.util.concurrent`
would have to argue with the shared fence rather than quietly extend a private list.

**Specification gap**

None. Everything here hardens application-layer companions and test structure; the
specification's criteria are untouched.

### D101 — The grouped grammar releases as version byte `0x01` (supersedes D98's renumbering rejection)

**Context**

D98 assigned the grouped grammar version byte `0x02` and retired `0x01`, which had named the
flat grammar for part of the pre-release history, rejecting a renumbering as record ambiguity
for no gain. Approaching the 0.2.0 release — the first release of this wire format, since
0.1.0's protocol travelled in different headers entirely (`vc`, `vc-sender`, `vc-seq`) with
its own internal versioning — the owner weighed the other side of that trade: a released
format whose history starts at version 2 carries a permanent retirement note in a document
that is otherwise standalone, and every future reader of the frozen grammar meets a dangling
"what happened to 1?" before the grammar itself. No released message carries either byte
under `parsley.causes`, so the release is the last moment the numbering can be chosen rather
than inherited. The one complication is that pre-release snapshot builds were published to
Central's snapshot repository continuously, so messages under both pre-release bytes — the
flat grammar under `0x01`, and this grouped grammar under `0x02` — may exist in logs the
project cannot see.

**Decision**

The grouped grammar releases as version byte `0x01`, and the released wire document carries
no retirement note: version 1 is the only version, and any other byte is undecodable. The
snapshot-era exposure is accepted, examined rather than assumed away:

* A snapshot-era grouped message (leading byte `0x02`) is refused as an unknown version —
  fail closed, with an exact diagnosis.
* A snapshot-era flat message (leading byte `0x01`) now enters the grouped parse instead of
  being refused at the version byte. It still fails closed, by grammar rather than by
  version: the flat layout put a fixed 4-byte big-endian entry count after its version byte,
  so the count's leading `0x00` byte reads as a zero topic count and the remaining bytes are
  refused as trailing; the empty flat frontier (`01 00 00 00 00`) falls to the same refusal.
  The leading-zero premise is the deployment's doing, not the grammar's — the flat-era
  budget was caller-supplied, and only a deployment configured for frontiers near 470 MB
  could spell a count at 2²⁴ — so both flat shapes are pinned as refusals in the shared
  battery (family `snapshot-flat`, swept by both decoders and named by
  `CausesCodecTest#rejectsSnapshotEraFlatEncodingsAsTrailingBytes` and
  `CausalPastMalformationTest#rejectsSnapshotEraFlatEncodings`) rather than left to prose:
  an empty-frontier fast path or tolerated trailing padding would otherwise decode them as
  the empty frontier, discarding real causes. What is lost is only the diagnosis —
  "trailing bytes" where "unknown version" would have named the real cause.
* The reverse direction fails closed structurally. A flat-era snapshot reader meeting a
  released message accepts the version byte, then reads the varint topic count and the
  first three topic-id bytes as a 4-byte big-endian entry count — at least 2²⁴ for any
  non-empty frontier, against a message orders of magnitude smaller — and refuses as
  truncated; the released empty frontier (`01 00`) underflows the count read itself. No
  rolling-upgrade ordering constraint follows, though snapshot builds carry no
  compatibility promise either way.

The refusal pins move with the numbering rather than shrinking: the unknown-version battery
(`CausesMalformationVectors`, swept by both decoders) now holds byte zero, the snapshot-era
`0x02`, the first unassigned byte, a far one and two high-bit bytes, and
`CausesCodecTest#rejectsUnknownVersion` pins the same set against a widened accept, a
salvaging default or a sign- or mask-shaped compare.

**Alternatives**

* Keeping `0x02` with the retirement note (D98's choice) — rejected by the owner at the
  release decision: the note is a permanent cost in the one document meant to stand alone,
  paid to preserve a distinction — one byte, one grammar, across the pre-release period —
  that protects only logs of unreleased snapshot builds, which carry no compatibility
  promise. D98's principle that no byte ever names two grammars is kept for the released
  history, which is the history the document governs; version bytes stay unscarce from here.
* Renumbering while keeping the retirement note for the snapshot-era bytes — rejected: it
  spends the change without collecting its benefit. The history lives here instead, which is
  what this record is for.
* A grammar-level sentinel distinguishing renumbered v1 from flat v1 — rejected: it is a
  grammar change purchased to improve a diagnosis for unreleased builds, against AGENTS.md's
  preference for no change and D98's own analysis that the flat shapes already fail closed.

**Cost**

Snapshot-era flat messages refuse with a structural diagnosis rather than a version one, and
an operator meeting that refusal finds the explanation here rather than in the wire document.
Any doc-driven decoder of the pre-renumbering grouped grammar is orphaned by one byte, as
D98's flat-grammar decoders were orphaned entirely — the same pre-release license, used the
same way. The wire document's stability section shrinks by its historical sentence; the
"never two grammars under one byte" fence is now enforced for released history by this
record and the refusal pins rather than by never reusing a pre-release byte.

**Specification gap**

None. Structural 5 requires a documented, stable, distinguishable representation; the
representation is unchanged and its version byte is release-frozen from here. The
specification is version-neutral, as D98 recorded.

### D102 — Hold-back memory is bounded by the heads, a flush costs the holds it writes, and the emission header is encoded once per frontier change

**Context**

D17 persists held bodies at step end, and D5's decision reads only the head of each channel's
buffer; the engine nonetheless kept more in memory, and did more work per record, than either
needed — and the cost fell exactly where a hold-back buffer earns its keep, behind a lagging
cause. Measured against the built classes with a two-channel engine, a 200-entry frontier
per header (ten topics of twenty partitions, a modest topology) and a cause that never
settles:

* Every `Hold` retained its decoded `Causes` for as long as it was held — about 93 bytes per
  frontier entry per held message on the heap, 18.7 KB at 200 entries and 96 KB at 1,000 —
  so 40,000 held messages retained 750 MB and 400,000 exhausted a 6 GB heap. The restore path
  decoded every held blob into memory the same way, so a restart paid it again before
  delivering anything. Only the head of each buffer is ever offered to
  `Deliverability.decide`; the decoded form of everything behind it was dead weight.
* `flushHolds()` walked every buffer looking for unpersisted holds on every call, and
  `ParsleyProcessor.process` calls it after every record: 246 µs per call at 10,000 held,
  4.4 ms at 100,000. Per-record cost grew linearly with hold-back depth, so throughput
  collapsed as the buffer deepened — the moment it most needs to drain.
* `causesHeaderForEmission()` copied the frontier into a fresh `Causes` and re-encoded it per
  emission: 35 µs at 200 entries, 288 µs at 2,000, paid for each message a step sends
  although the frontier changes at most once per delivery.

None of this touches the specification — the decision unit, the wire format and every refusal
are unchanged — but Operational 2 asks that work outside delivering be bounded, and a
per-record walk of the buffer is the opposite.

**Decision**

Three rules in `ProcessEngine`, each pinned.

1. **Decoded only where read.** A hold's decoded form — causes, key, value, headers — is in
   memory in exactly two cases: the hold is not yet persisted, or it is the head of its
   channel's buffer. A flush drops the decoded form of every hold it writes except a head; a
   restore decodes each blob to refuse corruption at start, as before, and keeps only the
   skeleton (channel, position, timestamp); a hold is decoded from the store when it reaches
   the head (`load`), and delivery reads it from there. `causes == null` is the one spelling
   of "not in memory", and the four fields load and drop together. Memory is O(held)
   skeletons plus O(channels) decoded messages. A hold whose store entry is missing when
   loaded refuses as `UNKNOWN_ORDERING_STATE_FORMAT` rather than delivering nothing: the
   buffer and the store contradict each other. Pinned by
   `ProcessEngineTest#onlyTheHeadOfEachBufferKeepsItsDecodedFormOnceFlushed` (counts decoded
   holds across flush, restart, first decision and head turnover through the package-private
   `decodedHoldCount()`), `#aHoldReloadedFromTheStoreReachesLogicWithItsCausesIntact` (key,
   value, headers and causes byte for byte through the reload path, and the reloaded causes
   still entering the delivered past that clamps a joining channel — D31) and
   `#aHeldMessageMissingFromTheStoreRefusesRatherThanDeliveringNothing`.
2. **A flush writes what arrived since the last one.** Holds enter an `unpersisted` queue on
   receipt; `flushHolds` drains that queue and never walks the buffers. A hold delivered
   before its first flush is marked `removed` and skipped, so it is never written. Pinned by
   `#aHoldDeliveredBeforeItsFirstFlushIsNeverWrittenToTheStore` and by one of the suite's two
   wall-clock bounds (the other, `ProbeIdleChannelCostIntegrationTest`, times a real broker's
   probe under D107), `#flushingAfterEveryReceiptStaysCheapWhileTheBufferDeepens`: 50,000
   receipt-and-flush cycles under five seconds, where the walk spent about twenty seconds in
   that shape alone. The margin is an order of magnitude each way, which is why a timing
   bound is acceptable at these two sites and
   nowhere else in the suite.
3. **Encode once per frontier change.** The encoded frontier is cached and dropped at the
   frontier's mutation sites — merge on receipt, merge on delivery, prune on facts — and
   `CausesCodec.encode` gained a package-private overload over the engine's own sorted map so
   no `Causes` copy is made. Every emission receives its own copy of the bytes. The
   `OVEREXPRESS` sabotage keeps its uncached path. Pinned by
   `#theEmissionHeaderIsReusedUntilTheFrontierChangesAndHandedOutAsACopy`, which walks each
   mutation site and re-checks the cached width against `frontierBytes()` (D98's counter).

Measured after: a flush with nothing new costs 0.3 µs at 100,000 held (was 4.4 ms); the
engine's own retention for 400,000 held messages behind a head sits inside the harness's
25 MB measurement noise, where 40,000 retained 750 MB before; an emission at 200 entries
costs 4.8 µs (was 35), at 2,000 entries 47 µs (was 288). The per-record cost that remains
is the header decode and merge, linear in the frontier (about 40 µs at 200 entries, 320 µs at
1,000), which is the price of the frontier itself rather than of the buffer.

**Alternatives**

* Keeping the buffer itself in the store, with only a per-channel head and count in memory —
  O(channels) memory regardless of depth. Rejected for now: it needs a scan that can stop at
  the first key, and `OrderingStore.scanPrefix` visits every entry, so it is a contract
  change every host implements; the head-only rule takes most of the gain (a skeleton is
  tens of bytes against tens of kilobytes) without touching the seam. It is the next step if
  hold-back depths in the millions turn out to be a real deployment shape.
* Dropping the head's decoded form too, reloading it on every decision. Rejected: the
  processor decides every blocked head after every record, so each record would pay one
  store read and one decode per blocked channel — a per-record cost proportional to the
  frontier, bought back from the buffer.
* Skipping the blob decode at restore and validating lazily on reaching the head. Rejected:
  it moves a corrupt-state stop from start to mid-run, against the posture of D76 and D84
  that state which cannot be trusted refuses before delivering; the decode's cost at restore
  is I/O-bound and unchanged, only the retention goes.
* A dirty flag per hold with the buffer walk kept. Rejected: it is the walk that costs, not
  the flag.
* Handing out the cached header array itself rather than a copy. Rejected: a Kafka
  `RecordHeader` keeps the reference, and bytes handed to the host should not be the engine's
  cache.
* Avoiding the `Causes` copy without caching the encoding. Rejected as half the gain: a step
  delivering one message and emitting several would still encode several times.

**Cost**

A hold that becomes head after a flush costs one store read and one blob decode, once,
where it was in memory before — the price of not holding every decoded frontier.
`markDelivered` on a persisted hold likewise loads its causes before deleting the entry. The
`Hold` skeleton grows by a channel reference and a flag. The timing bound in the suite is a
deliberate exception to its otherwise clock-free evidence, and its Javadoc says so.

**Specification gap**

None. These are economies inside the ordering state Structural 8 already keeps where
application state cannot alter it.

### D103 — Each task publishes its delivery state, and `status()` carries it (fulfils D53's promise)

**Context**

Operational 1 asks that the public API say, per process, whether it has stopped delivering
and why; Operational 5 asks that the size of each process's causal metadata be observable in
operation. D55 gave `status()` the lifecycle state and the refusal reason. D53 logged the
frontier size each facts round and promised that "the status surface will carry the same
numbers when it lands"; D55 landed without them, and ASSESSMENT §2.5 stayed half resolved.
The larger gap was the case Operational 1 is really about: a process that has not stopped
but is not delivering either. A held message is the protocol doing its job, and the
diagnosis — which cause, on which channel, at which position, against which settled
position — has existed inside the engine as `Deliverability.Held` since D30, reachable only
by a debugger. An operator whose process "looks stuck" had logs at DEBUG level and no
question they could ask the running process.

**Decision**

A new public record, `TaskStatus`, and a fifth component on `ProcessStatus`, `tasks`. Each
task of a process — partition *i* of every received topic — publishes, on its own stream
thread, once per facts interval and at initialisation: the frontier's channel count and
encoded width (D53's numbers), the total held, and for every channel with held messages its
topic, partition, count, head position and the head's outstanding blockers, each blocker
named by topic and partition with the position required and the position settled, exactly as
`Deliverability.decide` reports them; plus how long ago broker facts were last applied, so a
starved facts source is visible as a growing age. The engine gained two read-only
inspections for this, `headPosition` and `headVerdict`, the latter returning the same verdict
`nextDeliverable` acts on. A task retires its entry when it closes, so a task reassigned
elsewhere never lingers. `ParsleyRuntime.status()` merges the live entries in partition
order. The four-component `ProcessStatus` constructor remains, reporting no task detail, so
nothing that built one breaks; supervisors keying on `refusalReason` (D77) are unaffected.

Pinned by `TopologyWiringTest#taskStatusNamesWhatIsHeldAndWhichCauseItWaitsFor` (a held
effect appears with its head position, its blocker's topic, partition, required and settled
positions, and the frontier width equal to the encoded header; the entry empties when facts
release the hold and disappears when the task closes),
`ApiValidationTest#nullStatusComponentsAreRefusedAtConstruction` (the new records refuse
nulls and negative counts; the four-component form reports no tasks), and on the real host by
`EndToEndIntegrationTest#heldMessageSurvivesAStateDirWipeByChangelogRestore`, which now waits
for `status()` to name the held message and the cause it waits for before wiping state.

**Alternatives**

* Kafka Streams metrics (gauges through `StreamsMetrics`) instead of, or as well as, the
  status surface. Rejected for now: the public `StreamsMetrics` API offers sensors for
  latency, rate and totals but no gauge, and a gauge needs the internal
  `StreamsMetricsImpl`; the status surface is pull-based and host-neutral, and an
  application can export it to any registry. A metrics bridge is a reasonable later addition
  once the numbers have a public shape, which this record gives them.
* Publishing on every record rather than every facts interval. Rejected: naming the
  blockers costs one decision per held channel, which per record would double the decision
  work of a drain; per interval it is invisible, and a diagnosis one second old is a
  diagnosis.
* Computing the snapshot on the caller's thread from the engine. Rejected: the engine is
  not thread-safe and is owned by its stream thread (Structural 1's locality is per task);
  crossing threads to read it would need a lock on the hot path.
* Naming blockers by `ChannelId` rather than topic name. Rejected: an operator reads topic
  names; every blocker the decision reports is on a received channel, whose name the task
  resolved at start, so the name is always available and is what a runbook can act on.
* Keeping `ProcessStatus` as it was and adding `Parsley.diagnostics()`. Rejected: D53
  promised the numbers on the status surface, and one call is what an operator reaches for;
  the compatibility constructor keeps the addition source-compatible for anyone constructing
  the record.

**Cost**

A public record added to the API surface, and one more component on `ProcessStatus`; the
snapshot is a few allocations per task per facts interval. `docs/failing-closed.md` and
`docs/runtime.md` describe the surface. `EVIDENCE.md` gains an Operational section with this
record's tests under its rows 1 and 5.

**Specification gap**

None. Operational 1 and 5 are met more fully; nothing in the spec fixes the shape of the
surface, which is why it is recorded here.

### D104 — Retention crossing a held message fails the holder closed; the log-start check runs against the settled position (corrects D26's Assumption 10 line; the retention dual of D46)

**Context**

The engine's mid-run truncation check (D9's defence in depth for Safety 8) compared a
channel's earliest retained position against `fedUpTo`, the fed-or-never coverage. A held
message has already advanced `fedUpTo` past itself, so retention that discards a message
this process received but has not delivered passed the check: log start equals the held
position plus one, which is exactly `fedUpTo + 1`. Nothing else noticed. Every upstream
process that had delivered that message pruned it from its frontier on its next facts round,
as Structural 13 permits — its position was below the log start — so their later sends
expressed nothing about it, and the holder delivered those sends past the held message once
its other causes settled: an effect before its cause, a Safety 1 inversion, with no refusal
anywhere. The review that found it built the interleaving as a scenario and the oracle's
pair check flagged the inversion once the held cause finally delivered.

D46 closed the same hole for deletion, and its argument transfers verbatim: once senders may
legally have pruned a message this process still owes, no local rule can order later
arrivals against it, and the only sound choice is to stop. D26's line for Assumption 10 —
that the Safety 8 machinery turns a retention breach into a fail-closed stop rather than a
misdelivery — was true for the fed case and false for the held one. The random sweep was
structurally blind: truncation events targeted committed read positions with no bias toward
channels anything was held from, the Safety 8 obligation judged coverage from the read
position, and `SimProcess#settledCauses` excused a truncated cause at delivery time even
when the delivering process itself still held it.

**Decision**

The log-start check runs against the settled position (D5): while messages are held on a
channel, the earliest retained position crossing the head of the hold-back buffer refuses
with `POSITIONS_DISCARDED_UNREAD`, naming the held position and the reason its place in
causal order cannot be preserved; with nothing held the check is the fed-based one it always
was, since the two coincide there. Retention up to exactly the held message discards nothing
owed and is not refused. The refusal recurs on restart while the condition holds, as D46's
does, and its remedy is the same deliberate reset.

The harness now reaches the shape. `truncateEvent` is biased toward channels a running
process holds from, as `killEvent` already was, and has a shape that truncates to exactly one
past a reader's oldest held message; a quiescence obligation fails any running process still
holding a message below its channel's earliest retained position without a recorded refusal;
and `settledCauses` no longer excuses a cause the delivering process itself holds, so the
delivery-time Safety 1 check sees the inversion the pair check could only see after the held
cause delivered.

Pinned by `TargetedScenarioTest#retentionDiscardingAHeldMessageFailsTheHolderClosed` (the
holder refuses at its facts round, names the held position, has delivered nothing past it,
and the oracle is clean), `#retentionUpToExactlyTheHeldMessageIsRetention` (the boundary:
no spurious refusal), and
`SabotageMetaTest#deliveringPastARetentionDiscardedHoldInvertsCausalOrderAndTheOracleSeesIt`
(with `IGNORE_TRUNCATION` disarming the check, the same rig delivers the effect past its held
cause and both oracle checks flag it — the proof the pins are load-bearing).

**Alternatives**

* Keeping the fed-based check and relying on Assumption 10. Rejected: an assumption whose
  breach the implementation can detect must fail closed (SPEC Host obligations preamble,
  Safety 9's pattern), D26 claimed it already did, and the alternative is a silent causal
  inversion — the one outcome AGENTS.md's first rule forbids.
* Holding every later arrival behind the discarded message forever instead of stopping.
  Rejected, as D46 rejected it: the process cannot tell which later arrivals depend on the
  discarded message, so it would be holding on a guess, and a growing buffer with no
  diagnosis is worse than a named stop.
* Refusing at the sender's prune instead of the holder's facts round. Rejected: the sender
  cannot know who holds; Structural 13 obliges it to prune; and the holder is the process
  whose delivery order is at stake.
* Checking at start only (D74's coverage check). Insufficient: the hazard arises mid-run
  and the start-time check reads `fedUpTo` rows, which have the same blind spot.

**Cost**

A deployment whose retention does not cover hold-back time now stops where it used to
misdeliver. That is the correct trade, and the diagnosis says what to raise. The facts-round
window D46 has — an effect arriving between the sender's prune and the holder's next round —
remains, with the same bound (one facts interval plus gather latency) and the same recorded
status. `docs/failing-closed.md` and `docs/model.md` describe the condition; `EVIDENCE.md`
rows Safety 1, Safety 8 and Structural 13 name the pins.

**Specification gap**

Yes, and worth recording in the spec's own terms. Safety 8 binds only the "read position",
which under Host obligation 2 a held message has already advanced past; Safety 9 gives
deletion an explicit duty while retention has none; Structural 13's "exactly when" a cause
can no longer matter is true only under Assumptions 10 and 17; and the Liveness premises do
not name retention covering hold-back. A Safety 8 clause of the form "a received but
undelivered message whose position falls below the channel's earliest retained position is
such a resumption, and an implementation MUST fail closed rather than deliver past it" would
say what this record implements.

### D105 — The reserved maximum position is undecodable metadata and untrusted state (wire-format constraint 7; extends D83)

**Context**

D21 uses `Long.MAX_VALUE` in `fedUpTo` as the in-band marker of a received channel whose
topic is confirmed deleted, and D74 already guards that sentinel in the bootstrap's
arithmetic. Nothing kept a header position out of the same slot. A message whose header
named `(c, Long.MAX_VALUE)` decoded cleanly — positions were only required to be
non-negative — and, delivered while `c` lay outside the received set (vacuous under
Liveness 4), put the value into the delivered past. When `c` later joined, D31's clamp copied
it into `fedUpTo(c)`, and every feed on the live channel was then refused as a feed "on a
channel recorded as no longer existing", recurring on every restart: an Assumption 13 breach
diagnosed as a topic deletion, the misnaming Operational 6 forbids. Any other unassignable
position (say 2⁶²) took the same path and silently dropped every future message on the joined
channel as covered. The review pinned the shape with a test that failed on the tree.

**Decision**

A position of `Long.MAX_VALUE` is undecodable: `CausesCodec.decode` refuses it as "beyond any
position a channel can assign", `Causes.of` refuses it at the value-object floor so no encoder
can spell it, and the shared malformation battery gains the vector (family `max-position`),
which both decoders sweep. `docs/wire-format.md` records it as constraint 7, a reader-side
tightening in the manner of constraint 5: no conforming writer ever produced such a pair. For
state persisted before the refusal existed, a restored frontier or delivered-past row at the
sentinel refuses as `UNKNOWN_ORDERING_STATE_FORMAT`, on D88's rule that state which cannot be
trusted stops the process before it re-expresses anything. Pinned by
`EngineBoundaryTest#aHeaderPositionAtLongMaxValueIsRefusedBeforeItCanReachTheFedToEndSentinel`
and by the catalogue sweeps in `CausesCodecTest` and `CausalPastMalformationTest`.

**Alternatives**

* Taking the sentinel out of band (a set of dead channels beside `fedUpTo`). Rejected for
  now: it changes the persisted meaning of a `fedUpTo` row that existing stores carry, so it
  needs a store-format version and a migration for a value the substrate cannot produce.
* Bounding positions at receipt against the channel's end offset, which would also catch
  positions below the maximum that no log has reached. Recorded as the stronger follow-up:
  it needs end-offset facts the facts round does not yet carry, and it is a liveness matter
  (a forged position holds downstream until the log grows past it) where this record's
  shape was a misdiagnosis.
* Clamping the value to the highest assigned position. Rejected on D23's grounds: a clamp
  launders input; a refusal names it.

**Cost**

One more vector in the battery and one more restore-time scan condition. A message carrying
the position is refused where before it was absorbed; only an untruthful sender can carry it.
The restore-time check also refuses a negative row, which receipt has always refused and so can
only be corruption; it has to, because D102's emission path encodes the engine's frontier map
directly and no longer passes through `Causes.of`, so restore is the one point between the
store and the wire where a stored position is validated (`EngineBoundaryTest#aNegativeRestoredFrontierRowIsRefusedBeforeItCanBeReExpressed`).

**Specification gap**

None. Structural 12 forbids a sender expressing an unassigned position; this closes the
receiver side for the one value the implementation itself reserves.

### D106 — Review pins over the core: D67's gaps closed, the emission header pinned byte for byte, the simulator's timestamps decorrelated from positions

**Context**

A line-by-line review of the core under this tree's own evidence standard found pins the
suite lacked and one call it did not need. D67 had recorded three gaps from the removed
mutation gate; gap 2 (two `parsley.causes` headers) was closed since, the other two were
open. The engine's emission header was pinned by length only: a position-only raise changes
bytes and no size, so a stale cached header — the exact failure D102's cache could
introduce — was visible to no unit test, only to the random sweep through the oracle. And the
simulator aliased every message's timestamp to its position, so an engine reading one for the
other, anywhere from the store encoding to the decision Structural 7 forbids from reading
timestamps at all, would have passed every run.

**Decision**

* D67 gap 1: the own-position `mergeDeliveredPast(channel, position)` in `markDelivered` is
  deleted. For a received channel `fedUpTo` was advanced at receipt, is never pruned and is
  retained across leave and rejoin (D25), and the join clamp is a maximum over `fedUpTo` and
  the delivered past, so the delivered position could never raise it: the call was redundant,
  which is why D67 found the suite green without it. The delivered past now records only the
  causes of delivered messages, which is the entry the D31 pins cover; the Javadoc says so.
* D67 gap 3: `EngineBoundaryTest#equalPositionRefeedOfAHeldNotDeliveredMessageFailsClosedAsOutOfOrderFeed`
  pins the `position <= fedBefore` boundary at reason level — weakened to `<`, the equal
  re-feed falls to the covered-position branch and blames a report that never existed
  (D77's boundary, Operational 6), which the type-only assertion could not see.
* `EngineBoundaryTest#emissionHeaderIsByteExactAfterEveryFrontierMutationKind` compares the
  emission header byte for byte with a fresh encoding after a new channel, a position-only
  raise, a delivery merge, a prune, a restore and a raise on a restored frontier; D102's
  cache is what it guards. The same class pins holds spanning several flushes (D17's rule
  that a same-step delivery never touches the store, and restore order and fidelity across
  flush boundaries), the `nextRead = 0` coverage floor, and the inclusive budget gates D98's
  cost note describes.
* `Instance` in the simulator carries a timestamp derived from its uid, never equal to its
  position; the fidelity assertion compares against it. `EngineTestFactory.plain` keeps the
  alias for unit tests that never read the timestamp.

**Alternatives**

* Keeping the redundant merge as defence in depth. Rejected: a write that can change no
  outcome is a claim the suite cannot check, and D67 recorded exactly that as the hazard.
* Pinning the header through the sweep alone. Rejected: 209 of 300 seeds went red under the
  stale-header mutation, but only through the oracle, after the fact, with no unit test
  naming the property.

**Cost**

One fewer delivered-past row per delivering channel in existing changelogs, which restore
ignores. The simulator's timestamps are now arbitrary-looking numbers in journals.

**Specification gap**

None.

### D107 — A facts round's tail is watched time; the probe is batched over the channels held heads wait on; the seed round does not probe (implements D88's spelling, whose pin did not cover the round tail; corrects D35's cost claim)

**Context**

D88 spelled the confirmation window's continuity on blind time: the interval that restarts a
dead or recreated verdict's window runs from the previous round's last observation to the
next round's first question, and time a round spends inside its own queries is watched. The
implementation stamped "last observation" at classification, right after the by-name
describes, before the earliest-offset wait, the committed-offsets fetch, the confirming
describe and the trailing-run probes. Everything after that stamp was charged to the next
round as blind time. The probe cost a second per idle channel — four polls of 250 ms, each
blocking its full timeout when nothing arrives, one channel at a time — and the processor
hinted every received channel of a task whenever it held anything, so a task holding across
three received channels had a three-second tail on every round: at the window's three-second
floor, every round restarted the window and no death or recreation was ever confirmed while
the task held. That is exactly the state in which the verdict matters: a received topic
deleted with nothing held from it while a held message on another channel waits on it. The
review reproduced it against scripted rounds, and measured the per-channel probe cost against
the embedded broker (three idle channels, 3.07 s; eight, 8.03 s; unaffected by
`fetch.max.wait.ms`). D35's "up to one short poll per blocked channel" understated the cost
by four, and its per-channel serialisation multiplied it by the task count on the runtime's
one facts thread.

**Decision**

Three changes in the facts path.

1. **Round-end stamp.** Every window a round observed is stamped again when the round ends,
   after the probes; maturity is still judged at classification, so a single long round
   cannot mature a window it opened. Pinned by `FactsRoundTailContinuityTest`: a
   three-second tail confirms on the second back-to-back round where it used to confirm
   never, a two-second tail on the third; and the remaining limit is pinned rather than
   assumed away — sibling rounds on the shared thread that together exceed the window still
   restart it (`#siblingRoundsLongerThanTheWindowStillRestartIt`), which the coalesced
   per-process round below is the follow-up for.
2. **Batched probe.** Every hinted partition is assigned to the probe consumer at once, each
   sought to the position above its hint, and one bounded poll loop resolves each partition
   by the offset of its first record or by its position advancing past an aborted run. A
   round's probe costs one loop however many channels are hinted. `max.poll.records=1` goes,
   since the batch wants one record per partition per poll; `max.partition.fetch.bytes` is
   bounded at 16 KiB so a batched fetch over many partitions stays small, the broker still
   returning a first batch larger than that (KIP-74). Pinned against the embedded broker by
   `ProbeIdleChannelCostIntegrationTest#idleHintedChannelsAreProbedTogetherWithinOnePollLoop`
   (three and eight idle channels each resolve in under two seconds).
3. **Hints name only what a held head waits on.** The processor hints the blocker channels
   of its held heads — from the same verdict the status surface reports (D103) — and never a
   channel that itself holds messages, whose settled position is its head whatever the broker
   says above it, nor an idle channel nothing waits on. The seed round at task initialisation
   carries no hints at all, so initialisation never probes on the stream thread; facts are
   lower bounds, and the first background round probes one interval later. Pinned by
   `TopologyWiringTest#probeHintsNameOnlyTheChannelsAHeldHeadWaitsOn`.

**Alternatives**

* A probe budget per round (D88's open item). Subsumed: batching makes the probe's cost one
  loop, and blocker-only hints remove the channels a budget would have had to ration.
* Confirming verdicts on the classification stamp alone, with the probes moved before it.
  Rejected: the offset wait and the confirming describe would still sit after it, and the
  D22 identity window fixes their order.
* Lowering `fetch.max.wait.ms` on the probe consumer. Measured to change nothing: a poll with
  no records blocks for its own timeout, not the fetch's.
* Coalescing rounds to one per process per interval, shared by its tasks with per-task
  incarnation checks, and a facts thread per process. Recorded as the follow-up that closes
  the sibling-round limit above and D54's per-task launch: every fact is a per-channel lower
  bound and an engine consumes only its own channels' facts, so a shared round is sound; it
  is a larger change to the launch and apply paths than this record takes.

**Cost**

A probe now reads at most 16 KiB per partition per fetch. A held message whose cause sits
above a trailing aborted run settles one facts interval later after a restart than before,
since the seed does not probe. The window's continuity now depends on the round tail only
through sibling rounds, which the pinned limit states.

**Specification gap**

None. Liveness 3 and Host obligation 2 are met with the latency the interval promises rather
than the latency the task count imposed.

### D108 — A concurrent cold start waits for other instances' bootstrap members and replaces a refused stream thread (closes D48's residual S1)

**Context**

D48 commits initial positions through a group membership, so a stale bootstrap is
generation-fenced, and recorded as residual S1 that several instances cold-starting together
could fail one instance's Kafka Streams client: each joins the group as a bootstrap member
under the consumer protocol, and a Streams join that reaches the coordinator while another
instance's member is still present is refused as a protocol conflict, which the consumer
treats as fatal — the stream thread dies, the runtime shuts the client down, and `status()`
reports it stopped with no refusal reason, after `start()` returned a handle. D48 sized the
window as "the seconds of a bootstrap with missing offsets" and left recovery to a
supervisor. The review reproduced it against the embedded broker in three of eight
two-instance cold starts: the window is the milliseconds between one member's leave and the
other's Streams join, and every fresh deployment of more than one instance opens it.

**Decision**

Two mechanisms, keeping D48's fence untouched. The bootstrap member carries a recognisable
client id (`parsley-bootstrap-` and a random suffix), and before starting Kafka Streams the
runtime waits, bounded by the committer's session timeout, until the group description shows
no such member from any instance: members leave within milliseconds of committing, so the
wait is usually nothing, and an ungraceful exit holds its membership for at most the timeout,
after which the join proceeds. For the join that still meets a member — the check and the
join are not atomic — the uncaught-exception handler recognises the protocol conflict and
replaces the stream thread rather than shutting the client down: the refused thread held no
task and did nothing that needs undoing, and its replacement joins after the member has
left. Everything else the handler sees still shuts the client down, and so does the collision itself
once it can no longer be another instance's member: a refused join is replaced only within twice
the bootstrap member's session timeout of the start, after which a member speaking another
protocol under this application id is persistent and the client stops with
`SUBSTRATE_MISCONFIGURED`, naming the conflict, rather than replacing its thread forever. Pinned by
`ConcurrentColdStartIntegrationTest#twoInstancesColdStartingTogetherBothComeUpHealthy` —
eight two-instance cold starts, every instance healthy, where three of eight died before —
and its sibling `#startReturnsWithoutWaitingForTheProcessToRun`, which pins what `start()`
actually returns into (D109); `StreamsJoinCollisionTest` pins both mechanisms deterministically
over their seams, since the broker test's collision window is probabilistic.

**Alternatives**

* A supervisor restart, as D48 left it. Rejected: the stop was undiagnosed, and the
  deployment shapes that open the window — a rolling deployment, an autoscaler — are the
  routine ones.
* Retrying `KafkaStreams.start` from the runtime after the fatal join. Rejected: the client
  is already shut down by then; replacing the thread keeps the client and its state
  directory.
* A different bootstrap protocol so the joins do not conflict. Rejected: the committer is a
  plain consumer because that is what carries the generation fence (D48), and Streams does
  not admit a foreign member under its own protocol.
* Waiting for the group to be empty rather than free of bootstrap members. Rejected: a live
  sibling's Streams members are the ordinary steady state of a scale-out.

**Cost**

One group describe per process per start, and a wait of at most the committer session
timeout when another instance's member exits ungracefully. A replaced thread is one WARN line.

**Specification gap**

None. Fault model 2's arbitrary pauses were always met by the fence; this record is about not
dying at the join.

### D109 — Substrate-detected stops that recur identically carry their reason; `start()` says what it returns into; `close()` is bounded on every leg

**Context**

Three operational findings of the review, each small. `recordFailure` classified the
consumer's `OffsetOutOfRangeException` under `auto.offset.reset=none` — the consumer-level
half of Safety 8 (D9) — and the changelog's `RecordTooLargeException` for a log line, but
stored the bare client exception, so `status()` reported them with no `refusalReason`: a
supervisor keyed on D55's `stoppedDeliberately()` read a stop that recurs identically on every
restart as a transient and restarted forever. `Parsley.start`'s Javadoc promised to return
"once each is running or has been refused", but the host's `start()` returns at the start of
its rebalance, and refusals raised inside task initialisation surface only through
`status()`. And D63 bounded the streams close at thirty seconds while `admin.close()` waited
`Long.MAX_VALUE`, the facts source's close blocked without bound on its round lock, and the
probe consumer closed with the client default.

**Decision**

`recordFailure` wraps the two conditions that recur identically as refusals — the offset
range as `POSITIONS_DISCARDED_UNREAD`, the record limit as `SUBSTRATE_MISCONFIGURED`, each
with its remedy — before merging, so D55's preference keeps them over later transients and
`status()` names them; a partition-shape change and a missing committed position stay
transient, since a restart resolves them (D59). Pinned by `SubstrateDetectedStopStatusTest`.
The Javadoc on `Parsley.start` and `ParsleyRuntime.start` now says the call returns once
each application has been started and that initialisation-time refusals surface through
`status()`; `ConcurrentColdStartIntegrationTest#startReturnsWithoutWaitingForTheProcessToRun` pins
that the state a caller finds immediately after is a live one, rebalancing or running. `admin.close` takes the same thirty-second
bound as the streams close, the probe consumer closes with five seconds, and the facts
source's close waits five seconds for its lock and otherwise marks itself closed and
abandons the round, whose own interrupt handling closes the probe.

**Alternatives**

* Blocking `start()` until every task is running or refused, through a state listener with a
  bounded wait. Recorded as a reasonable later addition; a rebalance can legitimately take
  long, and the bootstrap already refuses every condition it can see before the host starts,
  so the truthful contract was the smaller change.
* New reasons for the shape change and the missing position. Rejected: a reason is the
  signal that a restart will not help, and for those two it will.
* Closing the streams applications in parallel under one shared deadline. Not taken here;
  the sequential worst case is the process count times thirty seconds, which D63's bound
  already implies.

**Cost**

`failureDetail` for the two wrapped conditions now reads the refusal's diagnosis with the
client exception beneath it. `EVIDENCE.md`'s Operational rows 1 and 3 name the pins.

**Specification gap**

None.

### D110 — The ordering store is cached, and the bootstrap view keeps a held message's presence rather than its body

**Context**

Two measurements from the review's performance lens. First, the ordering store was built
with logging and compaction (D57) but without the host's write cache, so every `put` was one
RocksDB write and one changelog record. A received record merges its whole frontier — one
put per channel whose position advanced — and every delivery merges the same channels into
the delivered past; in a flowing pipeline, where upstream positions advance on every message,
that is about two writes per frontier channel per record: 402 puts at a 200-entry frontier,
some 15 KB of changelog per 1 KB record, a store-side ceiling near a thousand records a
second per task. Second, at every start the runtime read the ordering changelog end to end
into a map holding every live value, held blobs included, to answer which channels hold
something and how far each was covered — about 12 KB per held message on the heap at a
200-entry frontier, so a hold-back backlog of a hundred thousand messages cost over a
gigabyte before any task existed, and a large backlog failed `start()` with an
`OutOfMemoryError` exactly when the process most needed to restart.

**Decision**

The ordering store builder adds `withCachingEnabled()`. The cache keeps the latest value per
key and writes it through at commit, so writes per commit interval are bounded by the keys
touched rather than by records times channels; reads and range scans go through the cache;
a delete is written through as a tombstone; and under exactly-once the flush precedes the
transaction commit, so what a step persists is unchanged (D17). Held blobs pass through the
cache and evict under `statestore.cache.max.bytes` as any value does. Pinned by
`OrderingStoreCachingTest`, which walks the built store's wrapper chain for the caching and
changelogging layers. The bootstrap read keeps, for a held message's key, an empty presence
marker in place of its body — a tombstone still clears it — since the view's readers test
presence and never content; pinned by
`ChangelogReadStallTest#heldMessageBodiesAreKeptAsPresenceMarkersNotRetained`.

**Alternatives**

* Leaving caching to the operator through `streamsProperty`. Rejected: the store is
  parsley's, the write pattern is parsley's, and an uncached ordering store is a cost no
  application would choose.
* Deduplicating writes inside the engine instead. Rejected: the engine would be
  re-implementing the host's cache, and the host's flushes at commit are what tie the
  writes to the step.
* Streaming the bootstrap read without a map at all. Not needed once held bodies are
  markers: what remains is one small entry per live key.

**Cost**

Memory per instance for the cache, shared with any application stores that enable it, sized
by `statestore.cache.max.bytes`. The bootstrap view no longer carries held bodies, which
nothing read. The engine constructor still decodes every held blob at restore to refuse
corruption early (D102); a length-only validator would cut that allocation further and is
recorded as a follow-up.

**Specification gap**

None.

### D111 — The runtime can be waited on, an emission may carry its own timestamp, and the seam's documentation says what the code does

**Context**

A review of the public surface against the runtime behind it found four places where the
seam's contract was missing or false.

First, there was no way to wait on a running `Parsley`. `start` returns as soon as each
process's Kafka Streams application has been started, so the canonical example in `AGENTS.md`
and `docs/index.md` — `try (Parsley p = Parsley.start(...)) { // runs until closed }` —
started every process and closed it in the same instant; an application copying it ran for
about one rebalance. Every real `main` had to hand-roll a latch, a shutdown hook and a polling
loop over `status()`, and nothing in the documentation showed that shape.

Second, an emission always carried the delivered message's timestamp (D15) with no way to
give it another. Along a causal chain of k hops every message carries the origin's time:
time-based retention is measured from the record timestamp, so an emission that answers a
message older than the sent topic's retention can be discarded on arrival, and downstream
event-time windows see the origin's time rather than the emission's. D15 rejected wall-clock
timestamps because they make replays nondeterministic; it did not consider a timestamp the
application chooses as a function of delivered data, which is as deterministic as the effect
it belongs to.

Third, five Javadocs described something other than the code. `StateReader` said reads were
"scoped to the key range of the step in progress"; they are served from the shard of each
store owned by the delivering task, so a key is found only if the delivering topic was keyed
so that the same partitioner put it on that partition, and nothing said so. `Channel#startingAt`
said the position "has no effect on a process resuming from committed state", which contradicts
D36: a channel added later to a process with prior state begins at `EARLIEST` whatever was
declared. `Store` did not say the seam matches stores by instance, so a second `Store.of` for
the same name is refused at the first access as if undeclared. `Handler` did not say what a
throwing handler means — the process stops, restarts into the same message and stops again,
because nothing is ever skipped — or how to continue past an application failure. And
`Parsley#start` claimed to return "once each is running or has been refused" when it returns
at the beginning of the first rebalance and starts nothing if any process is refused.

Fourth, the documentation around the code. There was no operations page: the consumer groups,
changelog and bootstrap client names, the admin and consumer calls the runtime makes and the
ACLs they need, the topic prerequisites and the meaning of each refusal an operator meets
were scattered across decisions or absent. The README described the library as "A Java
library for Kafka Streams applications" and pinned a test count that was wrong at the next
commit; `AGENTS.md` pinned the same count; `docs/llms.txt` omitted the `slf4j-api` dependency;
the decision records were not published on the site; and CI ran only on Java 25 although the
library declares 21 as its floor, so "Java 21 or newer" was inferred from a `--release` build
rather than tested.

**Decision**

`Parsley` gains `awaitStopped()` and `awaitStopped(Duration)`: a blocking join that returns
when any process stops — deliberately, to preserve the guarantee, or otherwise — or when
another thread calls `close()`. It is implemented with one latch per runtime, counted down by
the uncaught-exception handler, by a state listener on each Streams application entering
`ERROR` or `NOT_RUNNING`, and by `close()`. The return is the moment to read `status()`; the
method does not say why the wait ended, because `status()` already does and a second surface
for the same fact would be one more to defend (Structural 9). Pinned by `AwaitStoppedTest`:
the wait elapses while nothing has stopped, a process failure ends it, and closing ends it.
The canonical example is now `try (Parsley p = Parsley.start(...)) { p.awaitStopped(); }`,
and `docs/runtime.md` shows the shutdown-hook pairing.

`Effects.Emission` gains an `OptionalLong timestamp`; empty inherits the delivered message's
timestamp as before, so D15's default stands and its four-argument shape remains as a
constructor. `Effects.Builder#send` gains overloads taking a `long` timestamp, with and without
headers. A negative timestamp is refused at construction
(`ApiValidationTest#negativeOrNullEmissionTimestampsAreRefusedAtConstruction`); the processor
forwards the given timestamp or the delivered one
(`TopologyWiringTest#anEmissionInheritsTheDeliveredTimestampUnlessGivenItsOwn`). The Javadoc
says to derive it from delivered data and never from a clock, for the reason D15 gave. The
protocol reads no timestamp (Structural 7), so the choice cannot touch deliverability.

The five Javadocs above now describe the code: `StateReader` explains task-partition sharding
and that a self-channel is a repartition, `Channel#startingAt` states D36, `Store` states
instance identity, `Handler` states the throwing contract and the dead-letter shape, and
`Parsley#start` states its all-or-nothing, returns-at-rebalance contract and its
`IllegalStateException` arm. `docs/runtime.md` carries the same in "Keys, partitions and
state", and `docs/failing-closed.md` the application-failure paragraph.

`docs/operations.md` is the operations page: names, calls and ACLs, prerequisites, what
`status()` shows, the refusals and their remedies. The README states what the library is and
no longer pins a count; `AGENTS.md` names the count at the decision that last changed it;
`docs/llms.txt` lists every runtime dependency and the new page; the decision records are
published on the site by `scripts/build-decisions-index.py` at pages build; and CI runs the
suite on Java 21 and 25.

**Alternatives**

* A listener or callback surface for stops. D55 declined it as a surface to defend, and that
  stands: `awaitStopped` is a join, not a subscription; it delivers no event and carries no
  payload.
* Exposing the `KafkaStreams` handles so an application can register its own state listener.
  Rejected by D11, which still holds.
* A wait that ends only on a deliberate stop, letting transient failures pass. Rejected: the
  application cannot tell the two apart without `status()`, and any stop is the moment to look;
  a wait that outlived a process stopped by the consumer would hide exactly the stops D109
  made readable.
* A `Clock` or `Instant` handed to handlers for emission timestamps. Rejected on D15's ground:
  a timestamp the runtime supplies from a clock differs on replay, and the effects of a
  re-invoked handler must be identical.
* A header the application sets in place of a record timestamp. Rejected: retention and
  event-time windows read the record timestamp, not headers, which is the whole reason to want
  one.

**Cost**

Two methods on `Parsley` and one more component on `Emission`, both to be defended. The
four-argument `Emission` constructor keeps every existing construction compiling, but a record
pattern written against 0.2.0's four components — `case Emission(var c, var k, var v, var h)`
— no longer compiles and must name the fifth; the same holds for `ProcessStatus` since D103.
Both are source breaks for 0.3.0 and belong in its release note. The operations page and the
corrected Javadocs are claims about the runtime that must be kept true as it changes;
`EVIDENCE.md` names the pins that catch the executable ones.

**Specification gap**

None. The spec is silent on timestamps beyond Structural 7, which this respects.

### D112 — Five pins a mutation trial showed the suite lacked, and the sabotage mode D91 recorded as missing

**Context**

A review pass deleted or inverted one guard at a time and ran the fast suites. Five
mutations stayed green. Deleting the pre-feed facts apply in `ParsleyProcessor#process` — the
three lines that apply a round the facts thread has already deposited before the next record
is fed — left 88 kafka unit tests green, although without it a received topic's recreation
reported by a completed gather is acted on only at the next punctuation, so for up to one
facts interval records of the new incarnation are fed, delivered and committed under the old
identity, which D44 says affirmative evidence must stop at once; only the punctuator path was
pinned. Changing D74's `offset - 1 > covered` to `offset > covered` in
`refusePositionsDiscardedUnread` — which refuses every legitimate expiry restart at exactly the
covered boundary with a destructive remedy — survived the kafka fast set, because the one pin
uses a wide gap and needs a broker. Taking `frontierSizeRemove`'s varint delta from the wrong
side of the count survived 431 core tests: the bookkeeping changes only when a prune shrinks a
topic group from 129 through 127 partitions, and `frontierBytesAgreesWithTheEncodedHeader`
crosses that boundary upward only; the drift is one byte per crossing for the process's
lifetime, so the O(1) budget gate and `frontierBytes()` part from the header emitted.
Disabling the delivered-past prune in `onFacts` survived every core test: its one consequence
is that an aged-out past resurfaces as a join clamp when its channel joins the received set,
writing a coverage record for a channel this process never read, which D74's start-time check
then refuses as positions discarded unread. And the equal-position re-feed below the session
floor — D67 gap 3's other side — was pinned above the floor only (D106), so weakening the
in-execution check to strict-less-than there degraded to a silent replay drop with no test
red. D91 had separately recorded that the sabotage sweep has no mode for the silent-drop
direction of `COVERED_POSITION_FED`.

**Decision**

Each gap is closed by the test the trial wrote, red under the mutation and green on the
tree: `ProcessorRevivalTest#aDepositedRoundIsAppliedBeforeTheNextRecordIsFed` (a freeing
report deposited before a record releases the hold ahead of it, and a deposited recreation
refuses the next record as `CHANNEL_IDENTITY_CHANGED`);
`BootstrapPreCheckTest#reEstablishedPositionAtExactlyTheCoveredBoundaryStartsAndOnePastItRefuses`
(coverage written by a real engine over `MemoryOrderingStore`; covered + 1 starts, covered + 2
refuses); `ProcessEngineTest#frontierBytesTracksTheEncodedHeaderWhenAPruneShrinksAGroupAcrossTheVarintWidthBoundary`;
`ProcessEngineTest#aPrunedDeliveredPastEntryDoesNotBecomeCoverageWhenItsChannelJoins`; and
`ProcessEngineTest#feedingTheSamePositionTwiceInOneExecutionFailsClosedAsOutOfOrderOnBothSidesOfTheSessionFloor`.
The `TREAT_COVERED_FEED_AS_REPLAY` sabotage mode disarms only the refusal's covered branch
into `DUPLICATE_DROPPED`; `SabotageMetaTest#treatingACoveredFeedAsAReplayIsCaught` stages the
honest and sabotaged engines over the same report-then-feed contradiction. The mode carries no
sweep floor, for the reason D91 gave: the harness derives every read-position report from a
process's own progress, so no random seed reaches a successor-ahead report (calibrated at 0
of 300), and it joins `DELIVER_PAST_DEAD_HOLDS` as a deterministically evidenced mode
(`docs/verification.md`). No production code changed beyond the mode's one guard.

**Alternatives**

* Leaving the sweep floors as D43 calibrated them. The generator changed twice on this branch
  (D104's truncation bias toward held channels, D106's timestamp decorrelation), and D43's
  rule is that any generator change re-measures and re-sets the floors at half the count.
  Measured over the sweep's 120 seeds after both changes, catches were: IGNORE_CAUSES 59,
  NO_FIFO 10, REDELIVER_REFEEDS 57, UNDECODABLE_AS_ABSENT 79, SKIP_RECEIPT_MERGE 57,
  DROP_HELD 75, IGNORE_TRUNCATION 50, IGNORE_REMOVED_CHANNELS 32, SILENT_DROP 30,
  OVEREXPRESS 82; over 300 seeds IGNORE_RECREATION 13, and DELIVER_PAST_DEAD_HOLDS and
  TREAT_COVERED_FEED_AS_REPLAY 0. The floors are re-set to half of each. IGNORE_TRUNCATION
  rose from D43's 35 to 50, which is what D104's bias was for; NO_FIFO fell from 17 to 10,
  since truncation events now displace some of the interleavings that caught it, and its
  floor of 5 records that margin honestly rather than the stale 9 it passed by one seed.
* Extracting `refusePositionsDiscardedUnread` to a package-private seam instead of reflecting
  on it. Not taken here: D92 reserves seam extraction for methods with more than one pin, and
  the test names the method by string so a rename fails it loudly.

**Cost**

Six tests. The reflective pin binds a private method's name and signature.

**Specification gap**

None.

### D113 — Declared-topic resolution corroborates an unknown-topic answer before refusing (extends D84)

**Context**

A CI run of this branch failed on one leg with `declared topics could not be resolved;
refusing to start` for a topic the test had created a moment earlier. The describe that
resolves declared topics is served from one broker's metadata view, which can lag the
controller's creation of a topic; a single stale "unknown topic" answer refused the start.
D84 already refuses to conclude the ordering changelog's absence from one such answer, for
the same reason, and demands three consistent unknown answers half a second apart. Declared
topics had no such tolerance: the one describe either resolved or refused.

**Decision**

`resolveTopicsCorroborated` retries a describe that fails with `UnknownTopicOrPartitionException`
twice more, half a second apart, and refuses only on the third consistent answer, with the
same diagnosis and cause as before. Any other failure refuses at once, since nothing about
it is a matter of corroboration, and a refusal from the identity floor (D83) passes through
untouched. Pinned by `DeclaredTopicResolutionTest`: a lagging answer is retried and the topic
resolves once described; three unknown answers refuse naming the missing topic after exactly
three describes; a generic failure refuses after one; the identity refusal is neither retried
nor rewrapped. `BootstrapIntegrationTest#aDeclaredTopicThatDoesNotExistRefusesToStartNamingTheResolutionFailure`
still pins the refusal on a real broker.

**Alternatives**

* Waiting in the test after creating the topic. Rejected: the race is the runtime's, not the
  test's. An application that creates its topics and starts is the ordinary first deployment,
  and it should not refuse on a broker whose metadata is a moment behind.
* Retrying every describe failure. Rejected for the reason D84 gave: a timeout or an outage
  retried three times is a slower version of the same refusal, and it must not be mistaken
  for a matter of corroboration.

**Cost**

A genuinely missing topic is refused one second later than before.

**Specification gap**

None.
