# Architecture

Parsley is one package, `io.github.tobyjamesclements.parsley`, with two visibility tiers.

The **public tier** is the entire supported surface: the Streams runtime (`CausalTopic`,
`CausalStage` with per-source `SourceHandler`s and the narrow `StageContext`,
`CausalTopology`, `CausalStreams`), the plain-client `CausalClock` (constructed from an
`Admin`; observe, record own sends, stamp — fully opaque), `CausalHeaders`' names and
sender/seq readers for observability, and `testTopology()` for broker-less
`TopologyTestDriver` tests.
No protocol vocabulary escapes: the vector clock and channel types are internal. The
**package-private tier** is the protocol core, the adapter's plumbing, and every seam — the
offset queries carry soundness obligations (strict end-offset resolution, definitive absence)
that make them contracts users must not substitute, so tests get a pre-wired broker-less
topology rather than an injection point. The core is hidden deliberately: it is only sound under a host contract
no API can enforce — per-channel offset order in, atomic commit of store, offsets, and sends,
position advances from the real consumer, partitioning before stamping — and the one host
that upholds that contract ships in the same package.

Kafka's semantics are load-bearing throughout the core: offsets are broker-assigned (hence
sequence claims), visibility is transactional (hence the density adaptation and step-atomic
recovery), and liveness rides the consumer's position (hence position-advance bridging). The
core's freedom from a compile-time Kafka dependency is a **simulator seam, not transport
abstraction** — the deterministic simulator, the protocol's primary verifier
([verification](verification.md)), hosts the core from the test tree of the same package.

## The core surface

| Type | Role |
|---|---|
| `Channel`, `VectorClock` | `(topicId, partition)` identity; vector clocks with max-merge, dominance, truncation, and a sorted, versioned wire form |
| `DeliveryProtocol` | The host-facing surface: `onRecord`, `positionAdvance`, `prepareSend`, `resumePositions`, `truncate` |
| `CausalNode` | The implementation: gate, hold queues, density adaptation, stamp, rescope, restore |
| `StateStore` (SPI) | Keyed bytes, transactional with delivery — the host commits it atomically with consumed offsets and sends |
| `BrokerOffsets` (SPI) | Broker offset facts: sink end offsets (init seed) and log starts (truncation stability) |
| `InboundRecord`, `Delivery` | The envelope in (bytes, coordinate, clock), the release out (in causal order) |

The host contract, in one paragraph: feed records per channel in offset order through
`onRecord`; report consumer position advances through `positionAdvance`; stamp and tag every
outbound send with `prepareSend` (against its concrete destination channel — the host
partitions before stamping); process the returned `Delivery` list in order; commit the store,
the consumed offsets, and the sends atomically (EOS). Indications are pulled — deliveries come
back as ordered return values — because both Kafka Streams processing and the simulator are
synchronous.

## Receive path

`onRecord` seeds or bridges density (below), folds the record's clock into the channel's
advertised clock, enqueues, and cascades. The gate itself is specified in
[the delivery gate](../foundations/delivery-gate.md): heads only, normalise, restrict,
dominate. The cascade loops until no head is deliverable and no drained queue has a known
clean prefix left to fold.

## The stamp

Outbound records carry `stamp = frontier ∪ channelClocks ∪ carriedAncestry ∪ ownOutputs`
(pointwise max), computed at one site. Each term closes one route by which a real cause could
escape the claim:

| Term | Covers | Without it |
|---|---|---|
| frontier | Everything delivered here, contiguously | Direct causes escape |
| channelClocks | Per consumed channel, the folded clocks of received records: ancestry that arrived through channels this node does not consume | The gate's ignore branch becomes a hole |
| carriedAncestry | Delivered past on channels no longer consumed ([scope changes](state.md#scope-changes)) | A redeploy silently un-claims history |
| ownOutputs | The node's committed prior-incarnation sends (the init end-offset seed) | Own outputs carry no order across partitions and sink topics |

The broker performs the sender's clock increment (offset assignment), which the sender never
observes — there is no acknowledgement feed, and the stamping path never waits. Clocks carry a
second claim kind, **sequence claims**: `(channel, sender, seq)` claims every record the
sender sent to that channel up to its per-channel send sequence, assigned synchronously at
`prepareSend`. Every outbound record carries its sender tag, and a receiver resolves a
sequence claim the moment it has delivered that sender's record at or past the claimed
sequence (per-partition send order equals offset order under EOS, so FIFO delivery decides it
with one delivered-sequence mark per channel and sender). A stamp carries at most one
self-claim per sink channel — this incarnation's send counter — so the sequence surface is
bounded by the sink set; resolvable sequence claims in folded custody normalise to offset
claims at the stamping site. A failed send cannot orphan a claim: the claiming record and the
claimed send share a transaction, so they abort together. Same-channel sends need no claim at
all — per-channel FIFO delivers them in order everywhere.

`ownOutputs` lives in memory only and is constant after init: every declared sink's end
offset, an over-claim on real appended offsets (delay-only, therefore sound) that covers every
prior incarnation's committed sends. The same seed heals every restart-shaped gap, including
former sinks ([scope changes](state.md#scope-changes)); prior-incarnation sequence claims
echoed back through custody upgrade to this offset space at the stamping site.

## Hold queues are unbounded, and disk-backed

There is deliberately no backpressure surface. Per-channel fetch pausing cannot be honoured
from inside a Kafka Streams task — the poll loop's own pause/resume bookkeeping, the task's
internal buffers, and lag-aware record scheduling all sit between a processor and the fetch
layer, and all belong to Streams. Rather than ship a signal the primary host cannot act on,
the hold queue is unbounded and lives in the state store: a lagging cause channel grows disk
and changelog, never heap, and the operational response is monitoring hold depth and sizing
retention — the same economics that already bound the causal history a deployment keeps.
Records are never dropped, and a wedge is a loud stall, not a silent loss.

## Truncation: log-start stability

Stamp-side clocks (carried ancestry and the advertised channel clocks) are the two whose
width grows with the node's transitive upstream, and truncation is what bounds them. The
required bound must be *globally* stable — no present or **future** consumer's gate may still
need a dropped entry, because a missing claim at a gate is a causal violation, not a delay. A
membership-based protocol (registered nodes publishing frontiers) cannot deliver that bound
in Kafka's anonymous-consumer world: a from-earliest late joiner's baseline sits below any
frontier minimum, and any gating consumer outside the registry breaks it silently.

The coordination-free source that does qualify is **the log-start offset**. Records deleted
by retention are below every reachable baseline, present or future — the same "below first
sighting is out of scope" rule that makes seeding sound — so `logStart − 1` per channel is
unconditionally stable, and a channel whose topic no longer exists truncates entirely (a
recreated topic is a different channel, so an absent topic's claims are unclaimable forever).
The driver is `truncateToLogStarts`: query earliest offsets for `stampChannels()`, truncate.
The Streams adapter runs it on a punctuator (`truncationInterval`, default ten minutes); a
failed sweep skips a cycle rather than failing the task. Truncation therefore advances
exactly as fast as retention does — clock width is bounded by the causal history your
retention actually keeps.

## The Kafka Streams adapter

`CausalStage` assembles its sub-topology: sources fetched as raw bytes, one adapter
processor, sinks written as raw bytes. Topics are typed values (`CausalTopic`), each source
paired with its own independently-typed `SourceHandler`, and handlers act through the narrow
`StageContext`: `emit` to a declared sink (serialized with the topic's serdes, partitioned
deterministically, stamped at the single stamping site — an emit to an undeclared sink fails
loudly), `store` for stage-declared state stores, `schedule` for punctuators. There is no raw
forward and no processor context in user hands; serialization happens exactly once on each
side, inside the stage, so the stamp travels with the exact bytes it claims.

`CausalStreams.start` supplies the production wiring: it enforces `exactly_once_v2` and
`read_committed`, installs a client supplier whose main consumers record their post-poll
positions into a thread-local (the `positionAdvance` feed — `position()` read on the polling
thread is the only sound source; see
[liveness](../foundations/liveness.md#position-advance-bridging)), and resolves topic identity
and broker offset facts through an admin client. `TopologyTestDriver` runs without a broker
through `testTopology()`, which wires the internal seams itself.

Stages compose: `CausalTopology.of(stageA, stageB, ...)` assembles several named stages into
one Streams topology, connected through ordinary topics — one stage's sink is another's
source, and the connecting topic is a causal channel like any other, stamped on write and
gated on read. Hops add no acknowledgement waits (stamping is synchronous under sequence
claims), custody carries transitively across them, and each stage keeps its own state store
keyed by its name. Stage-connecting topics are explicit and must pre-exist — there are no
automatic repartition topics, which keeps the co-partitioning precondition visible at each
hop. Sender identity derives from the application id, the stage name, and the partition, so
it survives topology evolution (task ids embed the sub-topology index, which renumbers when
stages are added).

Current adapter limits: non-Parsley headers on held records are not carried through delivery,
and the adapter runs without an acknowledgement feed (own outputs stay in sequence space — see
the stamp section — because Streams attributes producer acknowledgements per thread, not per
task).
