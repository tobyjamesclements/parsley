# The engine

`ParsleyEngine<K,V>` is the core of the causal guarantee — the receive-and-deliver half of the classic
causal broadcast algorithm (see [causal consistency model](causal-consistency.md)). It classifies
incoming records, manages the causal frontier and the held-record buffer, cascades releases as the
frontier advances, and fails the task fast on any dependency it can prove will never be satisfiable.
There is no eviction, no buffer limit, and no timeout: a record whose dependencies are not yet satisfied
stays buffered until they are, however long that takes — the buffer is a changelog-backed state store, so
it spills to disk rather than growing in memory.

## Engine algorithm, at a glance

```
receive(record):
    frontier.seedIfFirstSeen(record.coordinate)        # one-time per coordinate, folds in
                                                        # everything below the first-seen offset

    deps = consumedDependencies(record.dependencies)   # normalise (strip self-cycle and any
                                                        #   below-epoch-floor entry), then keep only
                                                        #   the coordinates this node consumes — every
                                                        #   other entry is IGNORED (counted by the
                                                        #   deps-out-of-scope-ignored metric, never a
                                                        #   failure)

    if frontier.dominates(deps):                       # the gate: this node has ITSELF delivered
                                                        #   every depended coordinate — a peer's claim
                                                        #   never substitutes for local delivery
        channels.delivered(record.coordinate)            # advance frontier, self-persist "f"
        channelStore.update(record.channel, rawDeps)   # stamp-only fold of the WHOLE clock (I9)
        out.add(record)
        propagate(record.coordinate)                   # cascade releases (see below)
    else:
        buffer.add(record)                             # hold until satisfied (no limit, no timeout)
        candidateIndex.index(record, deps, frontier)

    if frontier.tryAdvanceEpoch():                      # a delivery can close a pending epoch
        out.addAll(drainSatisfied())                    #   transition window; see topology-epochs.md
    return out

onWatermark(channel, offset, carriedFrontier):
    learnedSomethingNew = !stamp().dominates(carriedFrontier)  # I6: new against total knowledge
    channelStore.update(channel, carriedFrontier)      # stamp-only: feeds completeness(), not the gate
    channels.delivered(channel, offset)                   # the marker's OWN offset genuinely delivered
    propagate(channel)                                  # releases via the frontier advance alone
    if frontier.tryAdvanceEpoch(): drainSatisfied()
    return (delivered, learnedSomethingNew)             # caller relays downstream only if genuinely new
```

The gate is `frontier.dominates(consumedDependencies)`: this node's own contiguous delivered
frontier must cover every depended coordinate this node consumes; every other coordinate is
ignored, unconditionally. `completeness()` — the frontier max-merged with every
input channel's advertised dependencies (`ParsleyVectorClock.merge`) — is the *outbound stamp*, carrying
transitive ancestry downstream where each receiver's own gate verifies it locally; it never releases
anything here. See the [causal consistency model](causal-consistency.md) for why local delivery is
required and the topology contract it implies.

Each piece is covered in its own section below: [`receive()` algorithm](#receive-algorithm)
for admission, [Propagation cascade](#propagation-cascade-propagate) for the cascade, and
[Fail-closed failure model](#fail-closed-failure-model) for the two ways a record can prove itself
unsatisfiable.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `frontier` | `ParsleyChannels` | All causal state: contiguous frontier clock, per-input-channel clocks, `completeness()`, forwarded-offset index, epoch floor, and baseline seeding — self-persisting as the single `"f"` value. Channel clocks are the dependencies advertised on each `(topicId, partition)` this node consumes, seeded at registration for bookkeeping even though a silent channel contributes nothing to the completeness merge |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable set of held records — unbounded, no eviction |
| `candidateIndex` | `ParsleyCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `consumed` | `ParsleyVectorClock.CoordinatePredicate` | The gate's consumed(c) predicate: a registered input channel, on the partition this task owns. A dependency on a consumed coordinate gates on the local frontier; any other dependency is ignored, with a metric |
| `ownSinkTopics` | `ParsleyVectorClock.CoordinatePredicate` | Coordinates for a topic this node itself produces; feeds only the reflected-claim diagnostic metric, never the gate or the folds |

`ParsleyChannels` owns the causal state and the Lamport operations: `completeness()` (the delivery
predicate's input), `delivered(coordinate)` (advance the frontier and notify), and
`seedIfFirstSeen(coordinate)` (establish the baseline for a newly observed coordinate) — plus, when
topology-epoch coordination is configured, the per-coordinate epoch floor (see
[topology epochs](topology-epochs.md)).

`channelStore` backs the node's outbound stamp. `completeness()` is this node's own frontier
max-merged with every input channel's advertised dependencies (`ParsleyVectorClock.merge`): each channel
contributes the dependencies it has advertised, so transitive ancestry — a coordinate an upstream
channel delivered that this node may not itself consume — flows through to downstream receivers,
whose own gates verify it against their own delivery history. The delivery gate here is
`frontier.dominates(consumedDependencies(deps))` — this node's own contiguous frontier, exclusively;
an advertised claim never substitutes for local delivery of the cause. The same `completeness()`
stamps forwarded records and protocol watermarks. See the
[causal consistency model](causal-consistency.md) for the soundness argument and the topology
contract it implies.

## `receive()` algorithm

1. The dependency clock is decoded once at the boundary (`ParsleyMessage.from`): a missing header
   becomes an empty, vacuously satisfied clock. An undecodable header is handled by
   `ParsleyProcessor.onUnresolvableClock()` before the engine is called — the task fails fast
   (`ParsleyVectorClockResolutionException`); a record is never forwarded on an unknown premise. The
   engine always receives a typed `ParsleyVectorClock`.
2. Seed the frontier if this is the coordinate's first sighting (`seedIfFirstSeen` — consumption need
   not start at offset 0), cascading any releases the seed enables.
3. Compute `consumedDependencies`: normalise the clock (strip the self-cycle — a `(topicId, partition)` entry whose required offset equals the record's own source offset — and anything below the current topology-epoch floor; a no-op when epoch coordination is off), then keep only the coordinates this node consumes. Every other coordinate is the gate's *ignore branch*: unconditionally ignored — sound because transitively complete stamps merged unconditionally claim every consumed ancestor directly in the same clock — and counted by the `deps-out-of-scope-ignored` metric, never a failure.
4. Apply the gate: `frontier.dominates(consumedDependencies)` — this node's own contiguous delivered frontier must cover every depended consumed coordinate; a claim advertised on another channel never substitutes for local delivery. If it passes: advance the frontier, fold the record's whole raw dependency clock into its channel's clock — a stamp-only update feeding `completeness()`, made only at genuine gated delivery so the stamp never carries a claim from a record that was not actually forwarded — add the record to output, and call `propagate()`.
5. Otherwise: add the record to the buffer (assigned an insertion sequence and a `bufferedAt` timestamp, no size or time limit) and index its coordinates unsatisfied by the frontier in the candidate index.
6. If this delivery advanced the frontier past a pending epoch-transition boundary, `frontier.tryAdvanceEpoch()` closes the window and drains anything the raised floor releases. See [topology epochs](topology-epochs.md).

A missing header becomes an empty dependency clock, which the gate satisfies trivially (step 4). It
never reaches the buffer. The frontier still advances on these records, so buffered records waiting
on that coordinate are not permanently stalled.

## Propagation cascade (`propagate`)

When the frontier advances on coordinate `C`, `propagate` uses a worklist algorithm to cascade
releases without scanning the full buffer. This is Lamport's transitivity rule in code: if A → B
and A has been delivered, B can now be delivered; and if B → C, C follows in the same pass.

```
toScan = {C}
while toScan not empty:
  nextScan = {}
  for each coordinate in toScan:
    candidates = candidateIndex.findCandidates(topicId, partition, frontierOffset)
    for each candidate:
      entry = buffer.get(candidate.recordId)
      if entry == null: prune stale index entry, skip
      if frontier.dominates(consumedDependencies(entry.dependencies)):
        channels.delivered(entry's source coordinate)     # committed the moment it is decided,
        channelStore.update(entry's channel, deps')     #   never staged for a later batch step
        buffer.remove(sequence)
        nextScan.add(entry's source coordinate)
        output.add(entry.record)
  toScan = nextScan
```

Each iteration finds records whose dependencies are now met after the most recent frontier advances. Those releases can in turn advance the frontier on further coordinates, triggering additional releases. The loop terminates when no new records become satisfiable.

Stale index entries for records that have already been released are discovered and pruned lazily during this scan rather than eagerly on removal.

## Fail-closed failure model

There is no diversion sink and no partial-forwarding fallback: any record this engine can prove is
unsatisfiable unconditionally fails the owning Streams task, which Kafka Streams then retries or an
operator recovers from — never forwarded on an unproven causal premise, never silently discarded.
Two distinct failures, each with its own exception (a dependency on a coordinate this node does
not consume is *not* a failure: it falls to the gate's ignore branch, counted by the
`deps-out-of-scope-ignored` metric — see
[causal-consistency](causal-consistency.md)):

- **A poisoned buffered record** (`ParsleyBufferDeserializationException`) — a held record's key or
  value can no longer be deserialised on the forward path (typically an incompatible Schema Registry
  change while the record was buffered). The engine deserialises only once a record is proven
  deliverable, so a held, undecodable record that is *not* yet releasable never surfaces this — it
  only fires on an actual forward attempt. The record remains in the buffer changelog for recovery
  once the schema is fixed or rolled back.
- **An unresolvable dependency header** (`ParsleyVectorClockResolutionException`) — handled by
  `ParsleyProcessor` before the engine is ever called (see step 1 of the `receive()` algorithm above);
  unconditionally fails the task, never forwarded on an unknown premise.

All three are logged with metadata only — coordinate, dependency clock, header keys, payload
lengths — never the payload bytes themselves.

## Frontier persistence ordering

`ParsleyChannels` self-persists its single `"f"` value inside `delivered()` (and `seedIfFirstSeen()`), before control returns to the engine and the record is added to the output list. This ordering guarantees that the changelog write for the frontier advance is durable before the record reaches the user processor. There is no separate callback: the frontier owns its persistence.

## Buffer store

`ParsleyBufferStore<K,V>` is an interface with two implementations:

- **`StoreBackedBufferStore`** (production): wraps a `KeyValueStore<Long, byte[]>` backed by RocksDB and changelog-replicated. On construction, it makes a single pass over all existing keys to seed the monotonic `nextSequence` counter and the `size` field. No separate rehydration step is needed: the store is the buffer.
- **`MockBufferStore`** (tests): wraps a `TreeMap<Long, ParsleyMessage<K,V>>` with equivalent semantics.

## Candidate index

`ParsleyCandidateIndex` is backed by `StoreBackedCandidateIndex`, which wraps a `KeyValueStore<byte[], byte[]>`. The 36-byte composite key (topicId + partition + requiredOffset + recordId) sorts lexicographically in RocksDB, enabling a bounded range scan to find all records waiting on a given coordinate up to the new frontier offset.

The store value is always an empty byte array (`PRESENT` marker). The key encodes everything needed to find and validate a candidate.

On engine construction, if the buffer is non-empty (restart recovery), the engine makes a single pass over all buffer entries to populate the candidate index. This rebuilds the secondary index from the authoritative buffer state.
