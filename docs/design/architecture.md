# Architecture

Parsley is one package, `io.github.tobyjamesclements.parsley`, with two visibility tiers.

The **public tier** is the entire supported surface: the Streams runtime (`CausalStage`,
`CausalStreams`), the plain-client ops (`EdgeClock`, the read side of `CausalHeaders`, with
`Clock` and `Channel` as their vocabulary), and the seams a `TopologyTestDriver` test injects
(`TopicIds`, `SendTracker`). The **package-private tier** is the protocol core and the
adapter's plumbing. The core is hidden deliberately: it is only sound under a host contract
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
| `Channel`, `Clock` | `(topicId, partition)` identity; vector clocks with max-merge, dominance, restriction, truncation, and a sorted, versioned wire form |
| `DeliveryProtocol` | The host-facing surface: `onRecord`, `positionAdvance`, `prepareSend`, `resumePositions`, `truncate` |
| `CausalNode` | The implementation: gate, hold queues, density adaptation, stamp, rescope, restore |
| `StateStore` (SPI) | Keyed bytes, transactional with delivery — the host commits it atomically with consumed offsets and sends |
| `SendTracker` (SPI) | Acknowledgements of own sends (offset upgrades) and sink end offsets |
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
| ownOutputs | The node's own acknowledged sends | Own outputs carry no order across partitions and sink topics |

The broker performs the sender's clock increment (offset assignment), learned asynchronously
from acknowledgements — but the stamping path never waits for it. Clocks carry a second claim
kind, **sequence claims**: `(channel, sender, seq)` claims every record the sender sent to
that channel up to its per-channel send sequence, assigned synchronously at
`prepareSend`. Every outbound record carries its sender tag, and a receiver resolves a
sequence claim the moment it has delivered that sender's record at or past the claimed
sequence (per-partition send order equals offset order under EOS, so FIFO delivery decides it
with one delivered-sequence mark per channel and sender). Acknowledged sends upgrade to offset
claims; resolvable sequence claims in folded custody normalise to offset claims at the
stamping site. A failed send cannot orphan a claim: the claiming record and the claimed send
share a transaction, so they abort together. Same-channel sends need no claim at all —
per-channel FIFO delivers them in order everywhere.

`ownOutputs` lives in memory only. At init it is seeded from every declared sink's end offset
— an over-claim on real appended offsets, delay-only and therefore sound — which dominates
anything a persisted copy could have held, so persisting it would be redundant. The same seed
heals every restart-shaped gap: acknowledgements lost with a crash, and former sinks
([scope changes](state.md#scope-changes)).

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

`CausalStage` assembles a topology: sources fetched as raw bytes, one adapter processor, sinks
written as raw bytes. Serialization happens exactly once on each side, inside the adapter —
inbound bytes are held verbatim while gated and deserialized at delivery; user forwards are
serialized at forward time so the stamp travels with the exact bytes it claims. The user
supplies an ordinary `Processor<K, V, KO, VO>`; its context's `forward` goes through the
stamping site, with an unnamed forward fanning out to every sink and a named forward keyed by
sink topic name.

`CausalStreams.start` supplies the production wiring: it enforces `exactly_once_v2` and
`read_committed`, installs a client supplier whose main consumers record their post-poll
positions into a thread-local (the `positionAdvance` feed — `position()` read on the polling
thread is the only sound source; see
[liveness](../foundations/liveness.md#position-advance-bridging)), and resolves topic identity
and sink end offsets through an admin client. `TopicIds` and the send-tracker factory are
injectable seams, which is how the adapter runs under `TopologyTestDriver` without a broker.

Current adapter limits: one causal stage per topology (the core has no such limit),
non-Parsley headers on held records are not carried through delivery, and the adapter runs
without an acknowledgement feed (own outputs stay in sequence space — see the stamp section —
because Streams attributes producer acknowledgements per thread, not per task).
