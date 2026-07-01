# The engine

`ParsleyEngine<K,V>` is the core of the causal guarantee. It classifies incoming records, manages the causal frontier and the held-record buffer, cascades releases as the frontier advances, and enforces buffer limits.

## Engine algorithm, at a glance

```
onRecord(record):
    channelAdvanced = !channelClock(record.channel).dominates(record.dependencies)
    channelStore.update(record.channel, record.dependencies)  # receipt-time: record the branch's
                                                               # proven completeness before gating
    frontier.seedIfFirstSeen(record.coordinate)        # one-time per coordinate, folds in
                                                        # everything below the first-seen offset
    deps = record.dependencies.withoutSelfReference()  # strip the record's own offset only

    if completeness().dominates(deps):                 # the gate: every input channel has confirmed
                                                        #   every depended coordinate
        frontier.deliver(record.coordinate)            # advance frontier, fire FrontierCallback
        out.add(record)
        propagate(record.coordinate)                   # cascade releases (see below)
    else:
        buffer.add(record)                             # hold until satisfied
        candidateIndex.index(record, deps, completeness())
        if buffer.size() >= sizeLimit:
            out.addAll(evictOverflow())                # bring buffer back under the limit

    if channelAdvanced and channelStore.size() > 1:    # a fan-in branch advanced: re-scan, since a
        out.addAll(drainSatisfied())                   # channel advance can lift completeness for held records
    return out

onWatermark(channel, carriedFrontier):
    channelStore.update(channel, carriedFrontier)      # advance the source channel's clock
    return drainSatisfied()                             # release records the new completeness permits

evictOverflow() / evictExpired():
    candidates = oldest/expired buffered entries
    evictOrFail(candidates):
        if failOnEvictionLimit:
            throw ParsleyBufferEvictionLimitException  # candidates remain buffered
        else:
            evictSequences(candidates)                 # force-forward out of causal order,
                                                         # cascading via propagate per release
```

`completeness()` is the per-coordinate minimum across every input channel (`ParsleyClock.intersectMin`):
a record is deliverable only once every channel has confirmed every coordinate it depends on. See the
[causal consistency model](causal-consistency.md) for why the gate is strict and the topology contract
it implies.

Each piece is covered in its own section below: [`onRecord()` algorithm](#onrecord-algorithm)
for admission, [Propagation cascade](#propagation-cascade-propagate) for the cascade, and
[Eviction](#eviction) for the fail/continue policy split.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `frontier` | `ParsleyFrontier` | Causal state: contiguous frontier clock, forwarded-offset index, baseline seeding |
| `channelStore` | `ParsleyChannelClockStore` | Durable per-input-channel clock: the dependencies advertised on each `(topicId, partition)` this node consumes; seeded at registration so silent channels are present in the completeness fold |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable set of held records |
| `candidateIndex` | `ParsleyCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `sizeLimit` | `int` | Buffer depth at which eviction triggers |
| `evictionInterval` | `Duration` | Optional interval for time-based eviction |

`ParsleyFrontier` owns the three Lamport operations: `isDeliverable(deps)` (the delivery
predicate), `deliver(coordinate, callback)` (advance the frontier and notify), and
`seedIfFirstSeen(coordinate)` (establish the baseline for a newly observed coordinate).

`channelStore` backs the delivery gate and the node's outbound stamp. `completeness()` is the
per-coordinate minimum across every input channel (`ParsleyClock.intersectMin`): each channel
contributes the dependencies it has advertised plus its own contiguous delivered position, and a
coordinate any channel has not observed is absent from the result. The single gate is
`completeness().dominates(deps.withoutSelfReference())` — a record is delivered only once every input
channel has confirmed every coordinate it depends on. The same `completeness()` stamps forwarded
records and protocol watermarks. See the [causal consistency model](causal-consistency.md) for why the
gate is strict and the topology contract it implies.

## `onRecord()` algorithm

1. The dependency clock is decoded once at the boundary (`ParsleyMessage.from`): a missing header
   (logged at `DEBUG`) becomes an empty, vacuously satisfied clock. An undecodable header is handled
   by `ParsleyProcessor.onUnresolvableClock()` before the engine is called: under the default `fail`
   policy the task is failed fast; under `continue` the clock is replaced with `ParsleyClock.empty()`
   (logged at `WARN`) and the engine receives it as vacuously satisfied. Either way, the engine
   always receives a typed `ParsleyClock`.
2. Update the source channel's clock with the record's dependencies, at receipt time, before the gate. This is how a channel advertises its progress: a record's carried frontier is its branch's proven completeness the moment it arrives, regardless of whether this node has delivered the record yet. Recording it here lets `completeness()` advance as soon as a channel advertises a coordinate, and prevents a mutual deadlock between two sibling records that each depend on a shared ancestor. Whether this advanced the channel is remembered for step 6.
3. Strip the self-cycle from the dependencies. A `(topicId, partition)` entry whose required offset equals the record's own source offset on that coordinate is removed (`ParsleyClock.without`), so a record never blocks on its own position. This is the *only* dependency preprocessing — there is no in-scope filtering.
4. Apply the gate: `completeness().dominates(deps)`. `completeness()` is the per-coordinate minimum across every input channel, so a coordinate is satisfied only when every channel has confirmed it. If it passes: advance frontier, add the record to output, call `propagate()`.
5. Otherwise: add record to buffer (assigned an insertion sequence and a `bufferedAt` timestamp), index its unsatisfied coordinates (those `completeness()` does not yet cover) in the candidate index. If buffer depth >= `sizeLimit`, call `evictOverflow()`.
6. If the receipt-time channel update in step 2 advanced this channel *and* the node has more than one input channel, run `drainSatisfied()` — a full-buffer re-scan — to release any held record the lifted completeness now permits. This release is not reachable through the candidate index that drives `propagate()` (which keys on frontier advances). A single-channel node's completeness is its own frontier, fully covered by `propagate`, so it skips this.

A missing header, or an undecodable one under `continue` policy, becomes an empty dependency clock,
which `completeness().dominates` satisfies trivially (step 4). It never reaches the buffer. The
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
      if frontier.isDeliverable(entry.dependencies.without(self)):
        releasable.add(entry)
  toScan = {}
  for each releasable entry:
    buffer.remove(sequence)
    frontier.deliver(entry's source coordinate)
    toScan.add(entry's source coordinate)
    output.add(entry.record)
```

Each iteration finds records whose dependencies are now met after the most recent frontier advances. Those releases can in turn advance the frontier on further coordinates, triggering additional releases. The loop terminates when no new records become satisfiable.

Wait-index entries for records that have already been released or evicted are discovered and pruned lazily during this scan rather than eagerly on removal.

## Eviction

Both eviction paths select a subset of `buffer.entries()` (oldest-first by insertion sequence,
equivalently by `bufferedAt`) and hand it to a shared `evictEntries()` helper. For each selected entry:

1. Log the eviction at `WARN` with the causal gap (`required.missing(frontier)`) and call
   `metrics.recordViolation()`.
2. Remove from buffer and candidate index.
3. Advance the frontier at the entry's source coordinate and add the record to the forward list.
   Eviction never drops or diverts a record. It delivers the record out of causal order instead.

### `evictOverflow()` — size limit

Called inline from `onRecord()` once buffer depth reaches `sizeLimit`. It evicts only the oldest
`buffer.size() - sizeLimit + 1` entries, which is just enough to bring the buffer back under the
limit, and leaves younger records held. In the common case, where depth is checked after every single
admission, this evicts exactly one record per overflow and slides the window forward.

A second call site exists in `ParsleyProcessor.init()`, to enforce the limit once against a buffer
restored from a changelog after a restart. This matters after a reconfiguration that lowers
`ofSize(...)` before restarting, because the restored buffer can legitimately hold more entries than
the new limit allows, and the inline check above only runs on the next admission, which may never
come. This cannot happen synchronously inside `init()`. Kafka Streams does not finish wiring the
task's `RecordCollector` until every processor in the topology returns from `init()`, so forwarding a
`FORWARD_UNSAFE` eviction at that point throws an NPE. Instead, `init()` schedules a self-cancelling,
one-shot `WALL_CLOCK_TIME` punctuation that calls `evictOverflow()` on its first firing and cancels
itself immediately after. The formula does not depend on *when* it is called, so the same method
serves both call sites without change. If a new record is admitted via the inline path before the
punctuation fires, that path's own overflow check already restores the invariant, and the
punctuation's subsequent call is a no-op.

### `evictExpired()` — duration limit

Called by the processor's wall-clock punctuator at the configured interval. Walks `buffer.entries()`
from the oldest, evicting every entry whose `bufferedAt` is older than `now - duration`, and stops
at the first entry that hasn't aged out yet (everything after it is younger still). A no-op when no
duration limit is configured.

The causal gap logged for each eviction (`required.missing(frontier)`) encodes the shortfall,
not the absolute required offset: each coordinate's value in the gap clock is `required - observed`
at eviction time.

## Frontier persistence ordering

`FrontierCallback` fires inside `advanceFrontier()`, before the record is added to the output list. The `ParsleyProcessor` persists the frontier to the state store inside this callback. This ordering guarantees that the changelog write for the frontier advance is durable before the record reaches the user processor.

## Buffer store

`ParsleyBufferStore<K,V>` is an interface with two implementations:

- **`RocksBufferStore`** (production): wraps a `KeyValueStore<Long, byte[]>` backed by RocksDB and changelog-replicated. On construction, it makes a single pass over all existing keys to seed the monotonic `nextSequence` counter and the `size` field. No separate rehydration step is needed: the store is the buffer.
- **`MockBufferStore`** (tests): wraps a `TreeMap<Long, ParsleyMessage<K,V>>` with equivalent semantics.

## Candidate index

`ParsleyCandidateIndex` is backed by `RocksCandidateIndex`, which wraps a `KeyValueStore<byte[], byte[]>`. The 36-byte composite key (topicId + partition + requiredOffset + recordId) sorts lexicographically in RocksDB, enabling a bounded range scan to find all records waiting on a given coordinate up to the new frontier offset.

The store value is always an empty byte array (`PRESENT` marker). The key encodes everything needed to find and validate a candidate.

On engine construction, if the buffer is non-empty (restart recovery), the engine makes a single pass over all buffer entries to populate the candidate index. This rebuilds the secondary index from the authoritative buffer state.
