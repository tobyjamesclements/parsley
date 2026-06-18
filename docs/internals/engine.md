# The engine

`ParsleyEngine<K,V>` is the core of the causal guarantee. It classifies incoming records, manages the causal frontier and the held-record buffer, cascades releases as the frontier advances, and enforces buffer limits.

## Key state

| Field | Type | Purpose |
|---|---|---|
| `frontier` | `CausalFrontier` | Highest admitted offset per `(topicId, partition)` |
| `buffer` | `ParsleyBufferStore<K,V>` | Durable set of held records |
| `waitIndex` | `ParsleyWaitIndex` | Secondary index: coordinate -> candidate record IDs |
| `policy` | `CausalBufferPolicy` | What to do when a limit fires |
| `sizeLimit` | `int` | Buffer depth at which eviction triggers |
| `evictionInterval` | `Duration` | Optional interval for time-based eviction |

## `onRecord()` algorithm

1. Decode `parsley-causal-dependencies` header.
    - Missing: report `MISSING_HEADER` violation, advance frontier, apply policy.
    - Undecodable: report `UNRESOLVABLE_DEPENDENCIES` violation, advance frontier, apply policy.
2. Strip self-referential entries from the decoded dependencies. A `(topicId, partition)` entry whose required offset is >= the record's own source offset on that coordinate is removed. This prevents a record from blocking on its own position in the log.
3. If `deps.isSatisfiedBy(frontier)`: advance frontier, add record to output, call `drainInto()`.
4. Otherwise: add record to buffer (assigned an insertion sequence), index unsatisfied coordinates in the wait index. If buffer depth >= `sizeLimit`, call `evictNow()`.

Records with missing or undecodable dependency headers are handled by the buffer policy (forward, drop, or dead-letter). The frontier always advances so buffered records waiting on that coordinate are not permanently stalled.

## Drain cascade (`drainInto`)

When the frontier advances on coordinate `C`, `drainInto` uses a worklist algorithm to cascade releases without scanning the full buffer.

```
toScan = {C}
while toScan not empty:
  for each coordinate in toScan:
    candidates = waitIndex.findCandidates(topicId, partition, newOffset)
    for each candidate:
      entry = buffer.get(candidate.recordId)
      if entry == null: prune stale index entry, skip
      if entry.dependencies.withoutSelfReference(...).isSatisfiedBy(frontier):
        releasable.add(entry)
  toScan = {}
  for each releasable entry:
    buffer.remove(sequence)
    advance frontier at entry's source coordinate
    toScan.add(entry's source coordinate)
    output.add(entry.record)
```

Each iteration finds records whose dependencies are now met after the most recent frontier advances. Those releases can in turn advance the frontier on further coordinates, triggering additional releases. The loop terminates when no new records become satisfiable.

Wait-index entries for records that have already been released or evicted are discovered and pruned lazily during this scan rather than eagerly on removal.

## `evictNow()` algorithm

Retrieves all buffer entries in insertion-sequence order. For each entry:

1. Report `LIMIT_REACHED` violation via `CausalViolationHandler`.
2. Remove from buffer and wait index.
3. Apply policy:
    - **ForwardUnsafe**: advance frontier, add to forward list.
    - **Drop**: discard.
    - **DeadLetter**: append DLQ headers, route to dead-letter sink.

DLQ headers added per evicted record:

| Header | Content |
|---|---|
| `parsley-dlq-reason` | `LIMIT_REACHED` (UTF-8) |
| `parsley-dlq-required-dependencies` | Serialised `CausalDependencies` the record required |
| `parsley-dlq-gap` | Per-position shortfall encoded as `CausalDependencies` |

The gap encodes the shortfall, not the absolute required offset. Each `CausalPosition.offset()` in the gap is `required - observed` at eviction time.

## Frontier persistence ordering

`FrontierCallback` fires inside `advanceFrontier()`, before the record is added to the output list. The `ParsleyProcessor` persists the frontier to the state store inside this callback. This ordering guarantees that the changelog write for the frontier advance is durable before the record reaches the user processor.

## Buffer store

`ParsleyBufferStore<K,V>` is an interface with two implementations:

- **`RocksBufferStore`** (production): wraps a `KeyValueStore<Long, byte[]>` backed by RocksDB and changelog-replicated. On construction, it makes a single pass over all existing keys to seed the monotonic `nextSequence` counter and the `size` field. No separate rehydration step is needed: the store is the buffer.
- **`MockBufferStore`** (tests): wraps a `TreeMap<Long, ParsleyRecord<K,V>>` with equivalent semantics.

## Wait index

`ParsleyWaitIndex` is backed by `RocksWaitIndex`, which wraps a `KeyValueStore<byte[], byte[]>`. The 36-byte composite key (topicId + partition + requiredOffset + recordId) sorts lexicographically in RocksDB, enabling a bounded range scan to find all records waiting on a given coordinate up to the new frontier offset.

The store value is always an empty byte array (`PRESENT` marker). The key encodes everything needed to find and validate a candidate.

On engine construction, if the buffer is non-empty (restart recovery), the engine makes a single pass over all buffer entries to populate the wait index. This rebuilds the secondary index from the authoritative buffer state.
