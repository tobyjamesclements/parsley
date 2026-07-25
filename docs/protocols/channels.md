# The channels module

`ParsleyChannels` is the lowest of the three protocol modules (see the
[protocols overview](index.md)): the adaptation that makes Kafka topic-partitions behave as the
reliable FIFO channels classical causal broadcast assumes (Hadzilacos and Toueg's reliable
channels;[^ht] the links layer of the Cachin–Guerraoui–Rodrigues stack, minus point-to-point — a
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
highest-received offsets, all folded into the single `"frontier"` value of the frontier store (see
[Wire format](../reference/wire-format.md#the-ns-frontier-value-key-frontier)); plus the forwarded-offset index, which
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
stale identity. This is
[environmental assumption E1](../foundations/assumptions.md#e1-stable-channel-identity).

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

A redeploy can change a stage's declared input set while its state survives. The persisted
`"frontier"` value records the input set it was written under, so `rescope` can diff it against the current
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

## Why the stamp is four clocks and not one

The preceding sections describe what each persisted clock holds. This section states why each one
has to exist. The outbound stamp must dominate every event that happened-before the record being
stamped. The contiguous frontier covers most of that past, and each of the other four clocks closes
one specific route by which a real cause escapes it. The four cover disjoint routes, which is why
the union has exactly these terms and not fewer.

| Clock | The causal past it covers | Without it |
|---|---|---|
| Channel clocks | Ancestry arriving through channels this node does not consume | The gate's ignore branch becomes unsound |
| Carried ancestry | This node's delivered past on channels it no longer consumes | A redeploy silently un-claims that past |
| Own outputs | This node's own earlier emissions | Its outputs carry no order across partitions or sink topics |
| Highest delivered | The processed record itself, when delivered above a frontier gap | An effect fails to claim its immediate cause |

### Channel clocks

Without the channel fold a node's stamp would name only coordinates it delivered itself, so
ancestry that reached it through a channel it does not consume would not survive the hop. Suppose
`p1` consumes `c2` but not `c4`, and a record on `c2` depends on `c4` partition 0 at offset 12.
`p1` delivers that record and forwards an effect. If the stamp carried no `c4` entry, a downstream
`p2` consuming both `p1`'s sink and `c4` would deliver the effect without waiting for that offset.

This clock is what makes the [delivery gate](../foundations/delivery-gate.md)'s ignore branch sound
rather than merely convenient. An unconsumed coordinate may be ignored only because the consumed
ancestry behind it is claimed directly in the same clock (I2 and I9). Removing the fold would turn
the ignore branch from a theorem into a hole.

### Carried ancestry

Without it, entries leave the stamp when a coordinate leaves the node's consumption scope. Suppose
`p1` consumed `c3` and delivered `c3` partition 0 at offset 50, and a redeploy then drops `c3` from
its declared inputs. Its next stamp would no longer name that offset, so a downstream `p2` consuming
both `p1`'s sink and `c3` would stop gating on a dependency it previously honoured.

Two properties fail together. An effect can precede its cause, and the stamp regresses across the
restart, which per-producer stamp monotonicity (I3) forbids. A configuration change would
retroactively weaken every stamp the node emits afterwards.

### Own outputs

Without it a node's outputs carry no ordering relative to each other. The failure does not appear on
a single sink partition, where the remaining clocks already make each stamp dominate the previous
one, so a held earlier record holds every later one (I3). It appears across partitions and across
sink topics. If `p1` forwards `m1` to `c2` and then `m2` to `c5`, Kafka's FIFO guarantee relates
neither pair, so nothing would make `m2` claim `m1`, and a downstream consumer of both topics could
deliver `m2` first.

This clock is also why the crossing wait exists. A node cannot claim its own previous output until
the broker has reported that output's offset.

### Highest delivered

Without it an effect emitted from an above-gap delivery names its cause's causes but not the cause.
Suppose `p1` delivers `c1` partition 0 at offset 40 while offset 38 is still held, leaving the
contiguous frontier at 37. A stamp built from the frontier alone would claim offset 37, so a
downstream `p2` consuming both `p1`'s sink and `c1` could deliver the effect without having
delivered offset 40, the record the effect came from.

The clock exists only because delivery within a partition is not head-of-line blocking. A classical
FIFO channel needs one projection of the delivered vector, and splitting delivery from contiguity
requires two.

## Persistence

The module self-persists the single `"frontier"` value inside every mutating request, before control
returns to the caller — the frontier advance is durable before a delivered record reaches the user
processor. All Parsley stores commit in one Kafka transaction (`exactly_once_v2` is required), so
the frontier, forwarded index, and buffer cannot tear against each other. The full binary layout
is in [Wire format](../reference/wire-format.md#the-ns-frontier-value-key-frontier).

## Cost

This module carries the term that usually dominates Parsley's per-record cost, because it owns the
persisted state. See the [consolidated cost model](index.md#cost-model) for how the rows fit
together.

- **State persistence, O(C · w).** All of the node's causal metadata (the frontier, the per-channel
  advertised clocks, the carried ancestry, the own-output positions, and the highest-received
  offsets) persists as the single `"frontier"` value, and the module rewrites that whole value on every
  advance: every delivered record, every producer acknowledgement, and every gossip fold. Each
  rewrite serialises the full channel state and issues one state-store put, so the cost scales with
  the total channel state rather than with the incoming header alone, and a store write costs far
  more than any of the clock walks in the causal-broadcast layer.
- **Channel-state restore on restart, O(C · w).** One point read of the persisted value followed by
  a parse linear in the total channel state. Independent of buffer depth. Init also resolves topic
  identities, reconciles the declared input and sink sets against the restored state, and seeds
  sink end offsets, which are a fixed number of admin and metadata round trips per task but usually
  dominate a restart in wall-clock terms.

Clock width itself grows here: the outbound stamp is the union of the frontier, the per-channel
advertised clocks, the carried ancestry, and the own outputs, and carried ancestry never shrinks. A
coordinate that entered the node's causal past stays claimed on every later stamp, so `w`
approaches the number of channels in the node's transitive upstream, not just the partitions the
task consumes.

[^ht]: Vassos Hadzilacos and Sam Toueg, "A Modular Approach to Fault-Tolerant Broadcasts and
    Related Problems", 1994, for the reliable-channel abstraction. See the
    [bibliography](../reference/bibliography.md).
