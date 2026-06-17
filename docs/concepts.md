# Concepts

## Causal dependencies

`CausalDependencies` is a snapshot of what a producer had observed when it sent a record — a map
from `(topicId, partition)` to the highest offset seen on that coordinate. Parsley serialises this
as a compact binary header (`parsley-causal-dependencies`) attached to every outgoing record.

On the consumer side, the clock is decoded into a `CausalDependencies` value: the minimum frontier
a downstream consumer must have reached before the record may be delivered. A record is **causally
ready** when `CausalDependencies.isSatisfiedBy(frontier)` returns `true`.

Clock keys are Kafka **topic UUIDs**, not topic names. UUID-keyed clocks survive topic deletion and
recreation: a new incarnation of `prices` gets a new UUID and is treated as a distinct dependency,
so records stamped against the old `prices` are never accidentally satisfied by the new one.

## The frontier

The **causal frontier** (`CausalFrontier`) tracks the highest offset
it has successfully delivered on each `(topicId, partition)` coordinate. Every time a record is
forwarded, the frontier advances. Frontiers from multiple sources can be merged with
`CausalFrontier.merge`, taking the per-coordinate maximum.

The frontier is **persisted** before each record is forwarded (via a changelog-backed state store),
so it survives restarts and rebalances.

## The causal buffer

Records whose dependencies are not yet satisfied are held in a **causal buffer** — also
changelog-backed, so held records survive a restart without re-delivery from the broker.

When a coordinate in the frontier advances (because a causally ready record was forwarded),
Parsley checks the buffer for records that were waiting on that coordinate. If any are now
satisfied, they are released and their source coordinates advance the frontier in turn — cascading
until no further releases occur. This cascade is index-accelerated: only records indexed on the
advancing coordinate are re-evaluated, avoiding a full buffer scan.

## Buffer limits

A `CausalBufferLimit` bounds how long or how large the buffer may grow before a **policy** fires:

| Limit | Factory | Description |
|---|---|---|
| Size | `CausalBufferLimit.ofSize(n)` | Fire when the buffer holds ≥ `n` records |
| Duration | `CausalBufferLimit.ofDuration(d)` | Fire after the buffer has held records for `d` (time-based; requires the processor to call eviction on schedule) |
| First-of | `CausalBufferLimit.first(a, b, ...)` | Fire when the first of several limits fires |

## Buffer policies

A `CausalBufferPolicy` determines what happens when the limit fires. **All policies** report a
`CausalViolation` for every evicted record.

| Policy | Factory | On eviction |
|---|---|---|
| Forward unsafe | `CausalBufferPolicy.forwardUnsafe(limit)` | Forwards the record out-of-order (lenient — delivery is preserved, ordering is not) |
| Drop | `CausalBufferPolicy.drop(limit)` | Discards the record (strict — no out-of-order delivery) |
| Dead letter | `CausalBufferPolicy.deadLetter(limit, topic)` | Routes the record to a dead-letter sink with additional headers describing the gap (strict) |

## Causal violations

A `CausalViolation` is reported via `CausalViolationHandler` whenever the causal guarantee cannot
be upheld for a specific record. Three reasons exist:

| Reason | Meaning |
|---|---|
| `MISSING_HEADER` | The record carries no `parsley-causal-dependencies` header (not stamped by Parsley) |
| `UNRESOLVABLE_CLOCK` | The header is present but cannot be deserialised (corrupt or unsupported wire version) |
| `LIMIT_REACHED` | The record was evicted from the buffer because a limit fired |

Records with `MISSING_HEADER` or `UNRESOLVABLE_CLOCK` are **forwarded immediately** — Parsley
cannot reason about their ordering and treats them permissively rather than blocking delivery.

Each violation includes the current frontier, the required dependencies, and the **causal gap**:
a per-coordinate shortfall showing exactly how far the frontier was behind at the time of eviction.

## Co-partitioning

Parsley evaluates dependencies against the partitions assigned to a single consumer instance. For
the guarantee to hold, an instance must be assigned **all** partitions in a causally related set.
The standard approach is to **co-partition** related topics so causally related messages share the
same partition number across topics, allowing each instance to own partition *N* everywhere.

Parsley does not detect or enforce co-partitioning — a misconfigured topology evaluates against an
incomplete partition set silently.
