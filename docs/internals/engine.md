# The engine

`ParsleyEngine<K,V>` is the core of the causal guarantee. It classifies incoming records, manages the causal frontier and the held-record buffer, cascades releases as the frontier advances, and enforces buffer limits.

## Engine algorithm, at a glance

```
onRecord(record):
    frontier.seedIfFirstSeen(record.coordinate)        # one-time per coordinate, folds in
                                                        # everything below the first-seen offset
    deps = effectiveDependencies(record.dependencies)  # scope to in-scope coords, strip self-ref

    if frontier.isDeliverable(deps):
        frontier.deliver(record.coordinate)            # advance frontier, fire FrontierCallback
        out.add(record)
        propagate(record.coordinate)                   # cascade releases (see below)
    else:
        buffer.add(record)                             # hold until satisfied
        candidateIndex.index(record, deps)
        if buffer.size() >= sizeLimit:
            out.addAll(evictOverflow())                # bring buffer back under the limit

    return out

evictOverflow() / evictExpired():
    candidates = oldest/expired buffered entries
    evictOrFail(candidates):
        if failOnEvictionLimit:
            throw ParsleyBufferEvictionLimitException  # candidates remain buffered
        else:
            evictSequences(candidates)                 # force-forward out of causal order,
                                                         # cascading via propagate per release
```

Each piece is covered in its own section below: [`onRecord()` algorithm](#onrecord-algorithm)
for admission, [Propagation cascade](#propagation-cascade-propagate) for the cascade, and
[Eviction](#eviction) for the fail/continue policy split.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `frontier` | `ParsleyFrontier` | Causal state: contiguous frontier clock, forwarded-offset index, baseline seeding |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable set of held records |
| `candidateIndex` | `ParsleyCandidateIndex` | Secondary index: coordinate -> candidate record IDs |
| `sizeLimit` | `int` | Buffer depth at which eviction triggers |
| `evictionInterval` | `Duration` | Optional interval for time-based eviction |

`ParsleyFrontier` owns the three Lamport operations: `isDeliverable(deps)` (the delivery
predicate), `deliver(coordinate, callback)` (advance the frontier and notify), and
`seedIfFirstSeen(coordinate)` (establish the baseline for a newly observed coordinate). See the
[causal consistency model](causal-consistency.md) page for the theoretical context.

## `onRecord()` algorithm

1. The dependency clock is decoded once at the boundary (`ParsleyMessage.from`): a missing header
   (logged at `DEBUG`) becomes an empty, vacuously satisfied clock. An undecodable header is handled
   by `ParsleyProcessor.onUnresolvableClock()` before the engine is called: under the default `fail`
   policy the task is failed fast; under `continue` the clock is replaced with `ParsleyClock.empty()`
   (logged at `WARN`) and the engine receives it as vacuously satisfied. Either way, the engine
   always receives a typed `ParsleyClock`.
2. Strip self-referential entries from the dependencies. A `(topicId, partition)` entry whose
   required offset equals the record's own source offset on that coordinate is removed
   (`ParsleyClock.without`). This prevents a record from blocking on its own position in the log.
3. If `frontier.isDeliverable(deps)`: advance frontier, add the record to output, call `propagate()`.
4. Otherwise: add record to buffer (assigned an insertion sequence and a `bufferedAt` timestamp), index unsatisfied coordinates in the candidate index. If buffer depth >= `sizeLimit`, call `evictOverflow()`.

A missing header, or an undecodable one under `continue` policy, always falls into the satisfied
branch (step 3). It never reaches the buffer. The frontier still advances on these records, so
buffered records waiting on that coordinate are not permanently stalled.

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
