# The channels module

`ParsleyChannels` is the lowest of the three protocol modules (see the
[internals overview](index.md)): the adaptation that makes Kafka topic-partitions behave as the
reliable FIFO channels classical causal broadcast assumes (Hadzilacos and Toueg's reliable
channels; the links layer of the Cachin–Guerraoui–Rodrigues stack, minus point-to-point — a
partition is multi-producer fan-out). Everything that exists because Kafka violates a classical
channel assumption lives here, stated once. The module box, in the same request/indication/property
style the source Javadoc uses:

```
requests:   receive(topicId, partition, offset)      a record arrived on a channel: establish the
                                                     density baseline (seed) and bridge
                                                     consumer-skipped holes
            delivered(topicId, partition, offset)    the causal-broadcast module records a delivery;
                                                     contiguous frontier advance
            normalize(rawDeps, source)               strip the record's exact self-cycle before the
                                                     gate evaluates its clock; a pure function
            acknowledge(topicId, partition, offset)  a producer acknowledgement of this node's own
                                                     send, folded into ownOutputs
            awaitOwnOutputQuiescence(except)         the crossing wait: block until no own-sink send
                                                     outside the excluded destinations is
                                                     unacknowledged; throw, never stamp, on timeout
                                                     or on an observed send failure
            rescope(currentInputs, taskPartition)    reconcile restored state with the declared
                                                     input set at init (scope changes)
queries:    frontier()                               the contiguous delivered clock — the gate's view
            ownOutputs()                             the node's own acknowledged output positions
            stamp()                                  the outbound vector timestamp:
                                                     completeness ∪ ownOutputs ∪ highestDelivered
            completeness()                           frontier ∪ carried ancestry ∪ channel clocks
            alreadyDelivered(topicId, partition,     membership in the delivered set — the receive
                offset)                              path's replay-skip guard
properties: per-producer stamp monotonicity; contiguous frontier; normalised clocks;
            stamp over-claim soundness; unconditional merge (restore side)
```

The module is the single owner of all causal metadata a node persists: the contiguous frontier
clock, the per-input-channel clocks, the carried-ancestry clock, the own-outputs clock, and the
highest-received offsets, all folded into the single `"f"` value of the frontier store (see
[Wire format](../reference/wire-format.md#the-ns-frontier-f-value)); plus the forwarded-offset index, which
keeps its own keyed store. The [causal-broadcast module](causal-broadcast.md) runs the delivery
gate and the buffer over these operations.

## Coordinates and identity

A causal coordinate is `(topicId, partition)`, where `topicId` is the topic's stable Kafka UUID;
the value at a coordinate is a broker offset. Topic names are resolved to UUIDs once, at task
initialisation, and identity is bound to the UUID for the process lifetime. A topic deleted and
recreated under the same name gets a new UUID, so a coordinate never silently rebinds to a
different record — recreation reads as history loss, never as reordering. A background
topic-identity poll (`ParsleyTopicIdentityWatch`) enforces the per-lifetime binding for inputs and
sinks alike: a mid-run UUID change fails every task fast before it can ingest or stamp under the
stale identity. This is environmental assumption E1 of the
[causal consistency model](../foundations/causal-consistency.md#environmental-assumptions).

## Density: making a partition look gap-free

Classical channels deliver a dense sequence. A Kafka partition, observed through a
`read_committed` consumer, does not: transaction commit markers and aborted records occupy real
offsets the consumer never returns, retention means consumption need not start at offset 0, and
Parsley itself delivers records on a partition out of order (no head-of-line blocking). Three
mechanisms repair this:

- **Seeding** (`seedIfFirstSeen`). The first offset ever observed on a coordinate folds everything
  below it into the frontier. History below the first sighting is outside the module's purview —
  a finite-retention baseline, not an unfillable gap.
- **Bridging** (`bridge`). Kafka delivers a partition strictly in offset order, so when a record
  arrives above the previous highest received offset, the open interval between them was skipped
  permanently — a transaction marker or an aborted record, never a business record still in
  flight. The bridge folds the skipped run into the contiguous walk so a commit marker's offset
  cannot wedge the frontier forever. The per-channel `highestReceived` offsets that make this
  detection exact are persisted with the frontier.
- **The forwarded index and the contiguous absorb walk.** A record delivered above a gap (a later
  offset forwarded while an earlier one is still held) is marked in the forwarded index. Each
  frontier advance walks the longest run of consecutive marked offsets now achievable and absorbs
  it, so the frontier stays exactly "the highest offset delivered without a gap".

### The two projections of the delivered vector

Because delivery within a partition is not head-of-line blocking, the classical delivered vector
VT(p) splits into two projections, and the module maintains both:

- The **frontier** — the contiguous projection, "everything up to n delivered". This is the only
  clock the delivery gate ever consults.
- **`highestDelivered`** — the max projection, observed on every delivery including deliveries
  still above a contiguous-frontier gap. This feeds the outbound stamp only: an output emitted
  from an above-gap delivery is causally after that record, so the stamp must claim it even
  though the frontier cannot yet. The implied claim on the gap offsets below it names real
  appended offsets, so it can only delay downstream delivery, never reorder it (over-claim
  soundness). The clock is not persisted: the forwarded index's marks are exactly the
  above-frontier delivered offsets and commit in the same EOS transaction as the frontier blob,
  so a restart reconstructs it losslessly.

The split is Kafka-specific; the literature's VT(p) assumes FIFO delivery and needs only one
projection.

## Own outputs

In Birman–Schiper–Stephenson the sender increments its own vector entry at send. Here the
increment is the broker's offset assignment, which the producer learns asynchronously from
acknowledgements — so the module reconstructs the sender's own slot after the fact:

- A `ProducerInterceptor` (`ParsleyOwnOutputInterceptor`, injected through the public `producer.`
  config prefix) captures each acknowledged `(topic, partition, offset)` of the node's own sends
  into a registry, and `acknowledge` max-folds them into the `ownOutputs` clock. The fold runs
  before every stamp, so no coordinate acknowledged before a stamp can be missing from it.
- **The crossing wait.** Within one task invocation, a second output is causally after the first
  even when the two go to different sink topics or different partitions of one sink — but Kafka's
  FIFO guarantee covers only same-partition sends. Before stamping, the task therefore waits for
  pending acknowledgements on every other own-sink coordinate (`awaitOwnOutputQuiescence`). On
  timeout or on an observed send failure the wait throws and the EOS transaction dies: an unacked
  send has failed, so a potentially under-claiming stamp must die with it, never proceed.
- **The init-time end-offset seed.** The persisted `ownOutputs` can trail the final transaction's
  acknowledgements (state store caches flush before the producer flush completes acks), so at
  initialisation the clock is seeded from each sink's end offset — a deliberate over-claim on real
  appended offsets, delay-only and therefore sound. The seed is unconditional: sink resolution is
  strict at initialisation, so a declared sink whose UUID or end offsets cannot be resolved fails
  init loudly rather than starting with the seed and the acknowledgement fold silently off — which
  would under-claim the node's own outputs for the task's whole lifetime. A causal sink must
  therefore exist before the application starts.
- **The former-sink heal.** The end-offset seed covers only currently declared sinks, so the blob
  also records the declared sink set. At initialisation every previous sink that is no longer one
  is healed: end-offset acknowledgement when the topic survives under its recorded UUID, a purge
  when it is provably destroyed, and a loud failure when it cannot be resolved — a redeploy that
  drops or repurposes a sink never restarts with stamps under-claiming the node's own final
  outputs.

`ownOutputs` feeds the stamp only, never the gate. It only ever grows, which is what preserves
per-producer stamp monotonicity.

## Normalisation

Every inbound dependency clock is normalised before the gate evaluates it: the record's exact
self-cycle is removed, because an event never precedes itself (a record depending on its own
coordinate has, by being delivered, met that dependency). That is the entire step. It is a pure
function of the clock and the source coordinate — view-only, never rewriting recorded state — so
the gate may re-evaluate a held record as often as it likes with the same result.

## Scope changes

A redeploy can change a stage's declared input set while its state survives. The persisted `"f"`
blob records the input set it was written under, so `rescope` can diff it against the current
declaration at initialisation. The governing principle: **the causal past a node has delivered or
carried may be skipped, but never dropped and never re-entered.** Four cases:

1. **Destroyed coordinates.** A declared topic whose UUID changed was deleted and recreated; the
   old UUID's entries can never be delivered by any receiver, so they are the only entries removed
   outright from stamp-feeding state.
2. **Shrink.** Every other entry leaving scope — a removed input's frontier entry, a retired
   channel's advertised clock — max-merges into a **carried-ancestry** clock that the stamp keeps
   merging forever. Dropping it instead would under-claim the stamp and let a third party reorder
   an effect against its retired-channel cause.
3. **Growth.** An input declared now but not previously seeds its frontier at the node's carried
   value for that coordinate (read from `stamp()`, so a former own sink skips what its stamps
   already claimed), never at log-start: skip what you already ignored. An operator who wants the
   skipped history processed performs a full reset. The receive path's `alreadyDelivered` guard
   skips the re-fetched prefix during the replay (counted by the `replays-skipped` sensor).
4. **Fresh store.** Nothing to diff; the current declaration is recorded.

`rescope` reconciles clocks only. Restored held *records* whose source left scope get their own
disposition, applied in the causal-broadcast constructor immediately after the rescope:

| Held record's source | Disposition |
|---|---|
| A current input (this task's partition) | Restored unchanged: seed replay, candidate re-index. |
| A recreated input's old incarnation (destroyed UUID) | Purged from the buffer, with an INFO log carrying the count and coordinates. The incarnation is deleted, so no receiver can ever deliver these records; delivering them here would re-enter a destroyed coordinate the rescope just purged. History loss, never reordering. |
| A removed but still-existing input | Init fails loudly, naming the topics and per-topic counts. The records can neither be delivered (no registered source) nor silently discarded (fail-closed). Remedies: redeclare the input so they drain through ordinary causal delivery, or perform a full reset. |

## Persistence

The module self-persists the single `"f"` value inside every mutating request, before control
returns to the caller — the frontier advance is durable before a delivered record reaches the user
processor. All Parsley stores commit in one Kafka transaction (`exactly_once_v2` is required), so
the frontier, forwarded index, and buffer cannot tear against each other. The full binary layout
is in [Wire format](../reference/wire-format.md#the-ns-frontier-f-value).
