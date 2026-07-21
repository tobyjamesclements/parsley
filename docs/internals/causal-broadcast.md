# The causal-broadcast module

`ParsleyCausalBroadcast<K,V>` is the middle protocol module (see the
[internals overview](overview.md)) and the core of the causal guarantee: the receive-and-deliver
algorithm of Birman–Schiper–Stephenson (ISIS CBCAST), run over the reliable FIFO channels the
[channels module](channels.md) adapts Kafka topic-partitions into. It gates incoming records,
manages the hold-back buffer, cascades releases as the frontier advances, and stamps every
outbound record at a single site. The module box:

```
requests:   broadcast(record) → stamped record   attach the outbound vector timestamp (the
                                                 timestamp-assignment half of BSS; the underlying
                                                 send is Kafka's produce), after the
                                                 acknowledgement fold and the crossing wait
            receive(message) → Outcome           the BSS receive: gate → deliver-or-hold →
                                                 cascade; the returned ordered list is the
                                                 deliver indication, in pull style
queries:    completeness()                       the delivered/advertised boundary
            frontier()                           the contiguous delivered clock
properties: causal delivery (a record reaches the delegate only after every consumed dependency
            has been locally, contiguously delivered; no timeout, no eviction); stamp transitive
            completeness; unconditional merge (stamp side)
```

There is no eviction, no buffer limit, and no timeout: a record whose dependencies are not yet
satisfied stays buffered until they are, however long that takes — the buffer is a
changelog-backed state store, so it spills to disk rather than growing in memory.

## The algorithm, at a glance

```
receive(record):
    channels.receive(record.coordinate)               # L1: seed the baseline, bridge
                                                      #   consumer-skipped holes

    deps = consumedDependencies(record.dependencies)  # normalise (strip the self-cycle), then keep
                                                      #   only the coordinates this node consumes —
                                                      #   every other entry is IGNORED (counted by
                                                      #   the deps-out-of-scope-ignored metric,
                                                      #   never a failure)

    if frontier.dominates(deps):                      # the gate: this node has ITSELF delivered
                                                      #   every consumed dependency — a peer's
                                                      #   claim never substitutes for local delivery
        channels.delivered(record.coordinate)         # advance the frontier, self-persist "f"
        channelUpdate(record.channel, rawDeps)        # stamp-only fold of the WHOLE clock
        out.add(record)
        propagate(record.coordinate)                  # cascade releases (see below)
    else:
        buffer.add(record)                            # hold until satisfied (no limit, no timeout)
        candidateIndex.index(record, deps, frontier)

    return out

broadcast(record):                                    # the single stamping site — business
    channels.foldAcknowledgedOutputs()                #   forwards and null messages alike
    channels.awaitOwnOutputQuiescence(except)         # crossing wait; throws rather than stamp
    return record + header(channels.stamp())          # completeness ∪ ownOutputs ∪ highestDelivered
```

The gate is `frontier.dominates(consumedDependencies)`: this node's own contiguous delivered
frontier must cover every depended coordinate this node consumes; every other coordinate is
ignored, unconditionally. `completeness()` — the frontier max-merged with the carried ancestry
and every input channel's advertised clock — feeds the *outbound stamp*, which carries transitive
ancestry downstream where each receiver's own gate verifies it locally; it never releases anything
here. See the [causal consistency model](causal-consistency.md) for why local delivery is required
and why ignoring unconsumed coordinates is sound.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `channels` | `ParsleyChannels` | The [channels module](channels.md): all persisted causal state — contiguous frontier, channel clocks, carried ancestry, own outputs, highest received — self-persisting as the single `"f"` value |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable hold-back queue of held records — unbounded, no eviction |
| `candidateIndex` | `ParsleyCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `consumed` | `ParsleyVectorClock.CoordinatePredicate` | The gate's consumed(c) predicate: a registered input channel, on the partition this task owns. A dependency on a consumed coordinate gates on the local frontier; any other dependency is ignored, with a metric |
| `ownSinkTopics` | `ParsleyVectorClock.CoordinatePredicate` | Coordinates for a topic this node itself produces; feeds only the reflected-claim diagnostic metric, never the gate or the folds |

## `receive()` algorithm

1. The dependency clock is decoded once at the boundary (`ParsleyMessage.from`): a missing header
   becomes an empty, vacuously satisfied clock. An undecodable header is handled by
   `ParsleyProcessor.onUnresolvableClock()` before this module is called — the task fails fast
   (`CausalVectorClockResolutionException`); a record is never forwarded on an unknown premise.
   The module always receives a typed `ParsleyVectorClock`.
2. `channels.receive` establishes the coordinate's baseline if this is its first sighting
   (consumption need not start at offset 0) and bridges any consumer-skipped run below the record,
   cascading any releases either step enables.
3. Compute `consumedDependencies`: normalise the clock (strip the exact self-cycle — a pure
   function of the clock and the source coordinate), then keep only the coordinates this node
   consumes. Every other coordinate is the gate's *ignore branch*: unconditionally ignored — sound
   because transitively complete stamps merged unconditionally claim every consumed ancestor
   directly in the same clock — and counted by the `deps-out-of-scope-ignored` metric, never a
   failure.
4. Apply the gate: `frontier.dominates(consumedDependencies)` — this node's own contiguous
   delivered frontier must cover every depended consumed coordinate; a claim advertised on another
   channel never substitutes for local delivery. If it passes: advance the frontier, fold the
   record's whole raw dependency clock into its channel's advertised clock — a stamp-only update,
   made only at genuine gated delivery so the stamp never carries a claim from a record that was
   not actually forwarded — add the record to the output, and call `propagate()`.
5. Otherwise: add the record to the buffer (assigned an insertion sequence and a `bufferedAt`
   timestamp, no size or time limit) and index its unsatisfied coordinates in the candidate index.

A missing header becomes an empty dependency clock, which the gate satisfies trivially (step 4).
It never reaches the buffer. The frontier still advances on these records, so buffered records
waiting on that coordinate are not permanently stalled.

## `broadcast()`: the single stamping site

Every outbound record — a delegate's business forward and a protocol null message alike — passes
through `broadcast()`, so the two cannot diverge. In order:

1. Fold pending producer acknowledgements into the own-outputs clock, so no coordinate
   acknowledged before this stamp can be missing from it.
2. Run the crossing wait (`awaitOwnOutputQuiescence`): block until no own-sink send outside the
   excluded destination set is unacknowledged. A business forward excludes nothing — its
   destination partition is unknowable at stamp time (the sink partitioner runs downstream of
   `forward()`), and over-waiting only ever folds more acknowledged positions, which is sound. A
   null message excludes its exact destination set (each sink at the task's own partition), which
   same-partition FIFO already covers. On timeout or an observed send failure the wait throws and
   the EOS transaction dies — never stamp-and-proceed.
3. Attach the stamp: `completeness ∪ ownOutputs ∪ highestDelivered`, the node's total knowledge.
   The merge is unconditional over everything the node has delivered, carried, or heard
   advertised — including coordinates on channels it does not consume, which is the custody chain
   the ignore branch's soundness stands on.

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
        channels.delivered(entry's source coordinate)   # committed the moment it is decided,
        channelUpdate(entry's channel, deps)            #   never staged for a later batch step
        buffer.remove(sequence)
        nextScan.add(entry's source coordinate)
        output.add(entry.record)
  toScan = nextScan
```

Each iteration finds records whose dependencies are now met after the most recent frontier
advances. Those releases can in turn advance the frontier on further coordinates, triggering
additional releases. The loop terminates when no new records become satisfiable.

Stale index entries for records that have already been released are discovered and pruned lazily
during this scan rather than eagerly on removal.

## Fail-closed failure model

There is no diversion sink and no partial-forwarding fallback: any record this module can prove is
unsatisfiable unconditionally fails the owning Streams task, which Kafka Streams then retries or
an operator recovers from — never forwarded on an unproven causal premise, never silently
discarded. Two distinct failures, each with its own exception (a dependency on a coordinate this
node does not consume is *not* a failure: it falls to the gate's ignore branch, counted by the
`deps-out-of-scope-ignored` metric — see the
[causal consistency model](causal-consistency.md)):

- **A poisoned buffered record** (`CausalBufferDeserializationException`) — a held record's key or
  value can no longer be deserialised on the forward path (typically an incompatible Schema
  Registry change while the record was buffered). Deserialisation happens only once a record is
  proven deliverable, so a held, undecodable record that is *not* yet releasable never surfaces
  this — it only fires on an actual forward attempt. The record remains in the buffer changelog
  for recovery once the schema is fixed or rolled back.
- **An unresolvable dependency header** (`CausalVectorClockResolutionException`) — handled by
  `ParsleyProcessor` before this module is ever called (see step 1 of the `receive()` algorithm
  above); unconditionally fails the task, never forwarded on an unknown premise.

Both are logged with metadata only — coordinate, dependency clock, header keys, payload lengths —
never the payload bytes themselves.

## Frontier persistence ordering

`ParsleyChannels` self-persists its single `"f"` value inside `delivered()` (and
`seedIfFirstSeen()`), before control returns to this module and the record is added to the output
list. This ordering guarantees that the changelog write for the frontier advance is durable before
the record reaches the user processor. There is no separate callback: the channels module owns its
persistence.

## Buffer store

`ParsleyBufferStore<K,V>` is an interface with two implementations:

- **`StoreBackedBufferStore`** (production): wraps a `KeyValueStore<Long, byte[]>` backed by
  RocksDB and changelog-replicated. On construction, it makes a single pass over all existing keys
  to seed the monotonic `nextSequence` counter and the `size` field. No separate rehydration step
  is needed: the store is the buffer.
- **`MockBufferStore`** (tests): wraps a `TreeMap<Long, ParsleyMessage<K,V>>` with equivalent
  semantics.

## Candidate index

`ParsleyCandidateIndex` is backed by `StoreBackedCandidateIndex`, which wraps a
`KeyValueStore<byte[], byte[]>`. The 36-byte composite key (topicId + partition + requiredOffset +
recordId) sorts lexicographically in RocksDB, enabling a bounded range scan to find all records
waiting on a given coordinate up to the new frontier offset.

The store value is always an empty byte array (`PRESENT` marker). The key encodes everything
needed to find and validate a candidate.

On construction, if the buffer is non-empty (restart recovery), a single pass over all buffer
entries repopulates the candidate index. This rebuilds the secondary index from the authoritative
buffer state.
