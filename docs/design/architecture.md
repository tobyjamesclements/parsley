# Architecture

Parsley is one package, `io.github.tobyjamesclements.parsley`, with two visibility tiers.

The **public tier** is the entire supported surface, shaped as a functional core with
imperative edges. The core is the user's logic vocabulary, free of Kafka types: `Topic` and
`Codec` (typed topics with pure byte codecs; Kafka serdes bridge in at the edge), `Message`
(a delivery with its own coordinate), `Emission` (an output as a value, minted by
`Topic.send`), `Handler` (message to emissions), and `Fold` with `Step` (pure per-key state
step). The edges run it: `Stage` wires logic to topics, `Parsley` composes stages and is the
front door (`testTopology()` for broker-less `TopologyTestDriver` tests, `streams(props)`
for production), `CausalStreams` is the running application (a curated allowlist over Kafka
Streams — lifecycle, state, listeners, metrics, lag — with no accessor to the underlying
instance), and the plain-client `CausalClock` (constructed from an `Admin`; observe, record
own sends, stamp — fully opaque) with `CausalHeaders`' names and sender/seq readers for
observability.
No protocol vocabulary escapes: the vector clock and channel types are internal. The
**package-private tier** is the protocol implementation, the adapter's plumbing, and every
seam — the offset queries carry soundness obligations (strict end-offset resolution,
definitive absence) that make them contracts users must not substitute, so tests get a
pre-wired broker-less topology rather than an injection point. The protocol is hidden
deliberately: it is only sound under a host contract
no API can enforce — per-channel offset order in, atomic commit of store, offsets, and sends,
position advances from the real consumer, partitioning before stamping — and the one host
that upholds that contract ships in the same package.

Kafka's semantics are load-bearing throughout the protocol: offsets are broker-assigned
(hence sequence claims), visibility is transactional (hence the density adaptation and
step-atomic recovery), and liveness rides the consumer's position (hence position-advance
bridging). The implementation's freedom from a compile-time Kafka dependency is a
**simulator seam, not transport abstraction** — the deterministic simulator, the protocol's
primary verifier ([verification](verification.md)), hosts it from the test tree of the same
package.

## The protocol surface

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
observes — the stamping path never waits on it. Clocks carry a
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

`Stage` assembles its sub-topology: sources fetched as raw bytes, one adapter processor,
sinks written as raw bytes. Between them, user logic runs as pure functions: a delivery is
decoded with the source topic's codecs into a `Message` carrying its own coordinate (for a
record released from the hold queue, its own, not the release trigger's), the handler or
fold is applied, and the returned emissions go through the stamping door — encoded with the
sink topic's codecs, partitioned deterministically before stamping, undeclared sinks failing
loudly. A stateful stage's fold state lives in a second RocksDB store keyed by the message
key's encoded bytes, read before and written after each application, in the same transaction
as the delivery — causal order plus a pure fold makes the state a deterministic function of
the delivered history. Serialization happens exactly once on each side, inside the stage, so
the stamp travels with the exact bytes it claims. Stages are immutable: wiring is captured
by the topology's node suppliers at assembly, never stored on the stage.

A stage with declared [ticks](../guide/ticks.md) closes a loop through the broker: a
wall-clock punctuator emits one stamped record per interval to the stage's own tick topic,
routed back to the emitting task's partition, consumed as one of the stage's sources, and
gated like any other record — the protocol's own-channel handling makes a self-claim
structurally satisfied, so the loop cannot deadlock. Time thereby enters user logic only as
data on a delivered record, and the tick's stamp claims the task's consumed history at
emission.

`Parsley.streams` supplies the production wiring: it enforces `exactly_once_v2` and
`read_committed`, installs a client supplier whose main consumers record a post-poll view
into a thread-local — the consumer position paired with the highest record offset the polls
have returned, the `positionAdvance` feed (`position()` read on the polling thread is its
only sound source; see
[liveness](../foundations/liveness.md#position-advance-bridging)), and resolves topic identity
and broker offset facts through an admin client. The adapter reports a captured position
only once every returned record at or below it has been fed through the protocol: a
post-poll position runs ahead of records still buffered between poll and process, and
reported early it would jump the frontier past them and drop them as replays. The adapter self-checks both at runtime —
EOS from the task's configuration at init, captured positions at the first delivered
record — so a topology run outside `Parsley.streams` fails closed instead of wedging.
`TopologyTestDriver` runs without a broker through `testTopology()`, which assembles a fresh
broker-less topology per call.

Stages compose: `Parsley.of(stageA, stageB, ...)` assembles several named stages into
one Streams topology, connected through ordinary topics — one stage's sink is another's
source, and the connecting topic is a causal channel like any other, stamped on write and
gated on read. Hops cost only the broker round trip (stamping is synchronous under sequence
claims), custody carries transitively across them, and each stage keeps its own state store
keyed by its name. Stage-connecting topics are explicit and must pre-exist — there are no
automatic repartition topics, which keeps the co-partitioning precondition visible at each
hop. Sender identity derives from the application id, the stage name, and the partition, so
it survives topology evolution (task ids embed the sub-topology index, which renumbers when
stages are added).

Current adapter limits: non-Parsley headers on held records are not carried through delivery,
and own outputs are claimed in sequence space rather than folded from producer
acknowledgements (Streams attributes acknowledgements per thread, not per task — see the
stamp section).
