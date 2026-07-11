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
    channelAdvanced = !channelClock(record.channel).dominates(record.dependencies)
    channelStore.update(record.channel, record.dependencies)  # receipt-time: record the branch's
                                                               # proven completeness before gating
    frontier.seedIfFirstSeen(record.coordinate)        # one-time per coordinate, folds in
                                                        # everything below the first-seen offset

    if isUnreachableDependency(record):                # a dependency names a coordinate this node has
        throw ParsleyUnreachableDependencyException    #   no input channel for at all: fail-closed,
                                                        #   never vacuously satisfied

    deps = effectiveDependencies(record.dependencies)  # strip self-cycle, this node's own sink
                                                        # coordinates, and any below-epoch-floor entry

    if completeness().dominates(deps):                 # the gate: some reachable channel has
                                                        #   confirmed every depended coordinate
        frontier.deliver(record.coordinate)            # advance frontier, self-persist "f"
        out.add(record)
        propagate(record.coordinate)                   # cascade releases (see below)
    else:
        buffer.add(record)                             # hold until satisfied (no limit, no timeout)
        candidateIndex.index(record, deps, completeness())

    if channelAdvanced and channelStore.size() > 1:    # a fan-in branch advanced: re-scan, since a
        out.addAll(drainSatisfied())                   # channel advance can lift completeness for held records
    if frontier.tryAdvanceEpoch():                      # a normal delivery can close a pending epoch
        out.addAll(drainSatisfied())                    #   transition window; see topology-epochs.md
    return out

onWatermark(channel, carriedFrontier):
    channelStore.update(channel, carriedFrontier)      # advance the source channel's clock
    delivered = drainSatisfied()                        # release records the new completeness permits
    return (delivered, channelAdvanced)                 # caller relays downstream only if genuinely new
```

`completeness()` is this node's own frontier max-merged with every input channel's advertised
dependencies (`ParsleyClock.merge`): a record is deliverable once any reachable channel has confirmed
each coordinate it depends on — no cross-channel unanimity required. See the
[causal consistency model](causal-consistency.md) for why a single witness suffices and the topology
contract it implies.

Each piece is covered in its own section below: [`receive()` algorithm](#receive-algorithm)
for admission, [Propagation cascade](#propagation-cascade-propagate) for the cascade, and
[Fail-closed failure model](#fail-closed-failure-model) for the three ways a record can prove itself
unsatisfiable.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `frontier` | `ParsleyFrontier` | All causal state: contiguous frontier clock, per-input-channel clocks, `completeness()`, forwarded-offset index, epoch floor, and baseline seeding — self-persisting as the single `"f"` value. Channel clocks are the dependencies advertised on each `(topicId, partition)` this node consumes, seeded at registration for bookkeeping even though a silent channel contributes nothing to the completeness merge |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable set of held records — unbounded, no eviction |
| `candidateIndex` | `ParsleyCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `inScope` | `ParsleyClock.CoordinatePredicate` | Coordinates this node could ever genuinely confirm (a registered input channel, on the partition this task owns). A dependency outside it fails the task fast rather than being treated as satisfied |
| `ownSinkTopics` | `ParsleyClock.CoordinatePredicate` | Coordinates for a topic this node itself produces; stripped from any inbound dependency or marker clock before the gate |

`ParsleyFrontier` owns the causal state and the Lamport operations: `completeness()` (the delivery
predicate's input), `deliver(coordinate)` (advance the frontier and notify), and
`seedIfFirstSeen(coordinate)` (establish the baseline for a newly observed coordinate) — plus, when
topology-epoch coordination is configured, the per-coordinate epoch floor (see
[topology epochs](topology-epochs.md)).

`channelStore` backs the delivery gate and the node's outbound stamp. `completeness()` is this node's
own frontier max-merged with every input channel's advertised dependencies (`ParsleyClock.merge`):
each channel contributes the dependencies it has advertised, and this node's own coordinates always
win the merge against any channel's separate view of them. A coordinate no channel has ever observed
is absent from the result. The single gate is
`completeness().dominates(effectiveDependencies(deps))` — a record is delivered once any reachable
channel has confirmed each coordinate it depends on. The same `completeness()` stamps forwarded
records and protocol watermarks. See the [causal consistency model](causal-consistency.md) for why a
single witness suffices and the topology contract it implies.

## `receive()` algorithm

1. The dependency clock is decoded once at the boundary (`ParsleyMessage.from`): a missing header
   (logged at `DEBUG`) becomes an empty, vacuously satisfied clock. An undecodable header is handled
   by `ParsleyProcessor.onUnresolvableClock()` before the engine is called: under the default `fail`
   policy the task is failed fast (`ParsleyClockResolutionException`); under `continue` the clock is
   replaced with `ParsleyClock.empty()` (logged at `WARN`) and the engine receives it as vacuously
   satisfied. Either way, the engine always receives a typed `ParsleyClock`.
2. Update the source channel's clock with the record's dependencies, at receipt time, before the gate. This is how a channel advertises its progress: a record's carried frontier is its branch's proven completeness the moment it arrives, regardless of whether this node has delivered the record yet. Recording it here lets `completeness()` advance as soon as a channel advertises a coordinate, and prevents a mutual deadlock between two sibling records that each depend on a shared ancestor. Whether this advanced the channel is remembered for step 7.
3. Check reachability: if any of the record's declared coordinates (after preprocessing) names a topic-partition `inScope` rejects, the engine cannot ever confirm it no matter how long it waits. Fail-closed rather than vacuously satisfied — throw `ParsleyUnreachableDependencyException`, never buffer, never treat as satisfied.
4. Compute `effectiveDependencies`: strip the self-cycle (a `(topicId, partition)` entry whose required offset equals the record's own source offset — `ParsleyClock.without`), strip any coordinate this node itself produces (`ownSinkTopics`), and strip anything below the current topology-epoch floor (`ParsleyClock.strippedBelow`; a no-op when epoch coordination is off). This is the only dependency preprocessing.
5. Apply the gate: `completeness().dominates(effectiveDependencies)`. `completeness()` is the max-merge across every input channel plus this node's own frontier, so a single genuine witness suffices. If it passes: advance frontier, add the record to output, call `propagate()`.
6. Otherwise: add the record to the buffer (assigned an insertion sequence and a `bufferedAt` timestamp, no size or time limit) and index its unsatisfied coordinates in the candidate index.
7. If the receipt-time channel update in step 2 advanced this channel *and* the node has more than one input channel, run `drainSatisfied()` — a full-buffer re-scan — to release any held record the lifted completeness now permits. This release is not reachable through the candidate index that drives `propagate()` (which keys on frontier advances). A single-channel node's completeness is its own frontier, fully covered by `propagate`, so it skips this.
8. If this delivery advanced completeness past a pending epoch-transition boundary, `frontier.tryAdvanceEpoch()` closes the window and this step drains anything the raised floor releases. See [topology epochs](topology-epochs.md).

A missing header, or an undecodable one under `continue` policy, becomes an empty dependency clock,
which `completeness().dominates` satisfies trivially (step 5). It never reaches the buffer. The
frontier still advances on these records, so buffered records waiting on that coordinate are not
permanently stalled.

## Propagation cascade (`propagate`)

When the frontier advances on coordinate `C`, `propagate` uses a worklist algorithm to cascade
releases without scanning the full buffer. This is Lamport's transitivity rule in code: if A → B
and A has been delivered, B can now be delivered; and if B → C, C follows in the same pass.

```
toScan = {C}
while toScan not empty:
  for each coordinate in toScan:
    candidates = candidateIndex.findCandidates(topicId, partition, newOffset)
    for each candidate:
      entry = buffer.get(candidate.recordId)
      if entry == null: prune stale index entry, skip
      if isUnreachableDependency(entry): throw ParsleyUnreachableDependencyException
      if frontier.isDeliverable(effectiveDependencies(entry.dependencies)):
        releasable.add(entry)
  toScan = {}
  for each releasable entry:
    buffer.remove(sequence)
    frontier.deliver(entry's source coordinate)
    toScan.add(entry's source coordinate)
    output.add(entry.record)
```

Each iteration finds records whose dependencies are now met after the most recent frontier advances. Those releases can in turn advance the frontier on further coordinates, triggering additional releases. The loop terminates when no new records become satisfiable.

Stale index entries for records that have already been released are discovered and pruned lazily during this scan rather than eagerly on removal.

## Fail-closed failure model

There is no diversion sink and no partial-forwarding fallback: any record this engine can prove is
unsatisfiable unconditionally fails the owning Streams task, which Kafka Streams then retries or an
operator recovers from — never forwarded on an unproven causal premise, never silently discarded.
Three distinct failures, each with its own exception:

- **An unreachable dependency** (`ParsleyUnreachableDependencyException`) — a record's declared
  dependencies name a coordinate this node has no input channel for at all (an undeclared topic, or a
  partition a different task instance owns). The engine can prove it can never check the coordinate,
  never that the coordinate is genuinely irrelevant, so it is fail-closed rather than vacuous —
  checked in `receive()` before buffering and again in `propagate()`/`drainSatisfied()` for a record
  buffered by an older binary version that predates the check.
- **A poisoned buffered record** (`ParsleyBufferDeserializationException`) — a held record's key or
  value can no longer be deserialised on the forward path (typically an incompatible Schema Registry
  change while the record was buffered). The engine deserialises only once a record is proven
  deliverable, so a held, undecodable record that is *not* yet releasable never surfaces this — it
  only fires on an actual forward attempt. The record remains in the buffer changelog for recovery
  once the schema is fixed or rolled back.
- **An unresolvable dependency header** (`ParsleyClockResolutionException`) — handled by
  `ParsleyProcessor` before the engine is ever called (see step 1 of the `receive()` algorithm above);
  unconditionally fails the task, never forwarded on an unknown premise.

All three are logged with metadata only — coordinate, dependency clock, header keys, payload
lengths — never the payload bytes themselves.

## Frontier persistence ordering

`ParsleyFrontier` self-persists its single `"f"` value inside `deliver()` (and `seedIfFirstSeen()`), before control returns to the engine and the record is added to the output list. This ordering guarantees that the changelog write for the frontier advance is durable before the record reaches the user processor. There is no separate callback: the frontier owns its persistence.

## Buffer store

`ParsleyBufferStore<K,V>` is an interface with two implementations:

- **`RocksBufferStore`** (production): wraps a `KeyValueStore<Long, byte[]>` backed by RocksDB and changelog-replicated. On construction, it makes a single pass over all existing keys to seed the monotonic `nextSequence` counter and the `size` field. No separate rehydration step is needed: the store is the buffer.
- **`MockBufferStore`** (tests): wraps a `TreeMap<Long, ParsleyMessage<K,V>>` with equivalent semantics.

## Candidate index

`ParsleyCandidateIndex` is backed by `RocksCandidateIndex`, which wraps a `KeyValueStore<byte[], byte[]>`. The 36-byte composite key (topicId + partition + requiredOffset + recordId) sorts lexicographically in RocksDB, enabling a bounded range scan to find all records waiting on a given coordinate up to the new frontier offset.

The store value is always an empty byte array (`PRESENT` marker). The key encodes everything needed to find and validate a candidate.

On engine construction, if the buffer is non-empty (restart recovery), the engine makes a single pass over all buffer entries to populate the candidate index. This rebuilds the secondary index from the authoritative buffer state.
