# Architecture

Parsley is two packages with a hard boundary between them.

`io.github.tobyjamesclements.parsley.core` is the protocol: no Kafka dependency, driven
synchronously by a host through pulled indications. `io.github.tobyjamesclements.parsley.kafka`
is one host — the Kafka Streams adapter plus the edge ops. The test tree contains a second
host, the simulator, which is the primary correctness gate
([verification](verification.md)).

## The core surface

| Type | Role |
|---|---|
| `Channel`, `Clock` | `(topicId, partition)` identity; vector clocks with max-merge, dominance, restriction, truncation, and a sorted, versioned wire form |
| `DeliveryProtocol` | The host-facing surface: `onRecord`, `positionAdvance`, `stampForSend`, `pauseWanted`, `resumePositions`, `truncate` |
| `CausalNode` | The implementation: gate, hold queues, density adaptation, stamp, rescope, restore |
| `StateStore` (SPI) | Keyed bytes, transactional with delivery — the host commits it atomically with consumed offsets and sends |
| `SendTracker` (SPI) | Acknowledgements of own sends, the crossing wait, and sink end offsets |
| `InboundRecord`, `Delivery` | The envelope in (bytes, coordinate, clock), the release out (in causal order) |

The host contract, in one paragraph: feed records per channel in offset order through
`onRecord`; report consumer position advances through `positionAdvance`; stamp every outbound
send with `stampForSend`; process the returned `Delivery` list in order; commit the store, the
consumed offsets, and the sends atomically (EOS); honour `pauseWanted` for backpressure.
Indications are pulled — deliveries come back as ordered return values — because both Kafka
Streams processing and the simulator are synchronous.

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
from acknowledgements. Before stamping, the node folds every pending acknowledgement and
performs the **crossing wait**: quiescence of unacknowledged own sends to channels other than
the destination. Same-channel sends need no claim — per-channel FIFO delivers them in order
everywhere. An observed send failure or a deadline fails the task; a stamp that might
under-claim the node's own outputs must die with its transaction.

`ownOutputs` lives in memory only. At init it is seeded from every declared sink's end offset
— an over-claim on real appended offsets, delay-only and therefore sound — which dominates
anything a persisted copy could have held, so persisting it would be redundant. The same seed
heals every restart-shaped gap: acknowledgements lost with a crash, and former sinks
([scope changes](state.md#scope-changes)).

## Backpressure

`pauseWanted(channel)` turns on when a channel's hold queue exceeds the configured bound. The
host pauses fetching that channel; causes never arrive on the channel whose head is blocked,
so pausing it cannot starve the release. Records are never dropped — this bounds memory, not
safety.

## Truncation

`truncate(stability)` drops entries at or below a supplied bound from the stamp-feeding clocks
(carried ancestry and channel clocks — the two whose width grows with the transitive
upstream). Soundness is conditional and the caller's responsibility: the bound must be
dominated by every node's frontier, so no gate anywhere still needs the dropped entries.
Parsley ships the hook and a verified wire format, not a stability coordination protocol.

## The Kafka Streams adapter

`CausalStage` assembles a topology: sources fetched as raw bytes, one adapter processor, sinks
written as raw bytes. Serialization happens exactly once on each side, inside the adapter —
inbound bytes are held verbatim while gated and deserialized at delivery; user forwards are
serialized at forward time so the stamp travels with the exact bytes it claims. The user
supplies an ordinary `Processor<K, V, KO, VO>`; its context's `forward` goes through the
stamping site, with an unnamed forward fanning out to every sink and a named forward keyed by
sink topic name.

`CausalStreams.start` supplies the production wiring: it enforces `exactly_once_v2` and
`read_committed`, registers the producer interceptor that captures acknowledgements (the
own-outputs feed), installs a client supplier whose main consumers record their post-poll
positions into a thread-local (the `positionAdvance` feed — `position()` read on the polling
thread is the only sound source; see
[liveness](../foundations/liveness.md#position-advance-bridging)), and resolves topic identity
and sink end offsets through an admin client. `TopicIds` and the send-tracker factory are
injectable seams, which is how the adapter runs under `TopologyTestDriver` without a broker.

Current adapter limits: one causal stage per topology (the core has no such limit),
non-Parsley headers on held records are not carried through delivery, and the crossing wait
conservatively awaits all pending sends because the sink partitioner runs after stamping.
