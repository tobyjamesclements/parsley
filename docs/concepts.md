# Concepts

## Causal dependencies

`CausalDependencies` is a snapshot of what a producer had observed when it sent a record — a map
from `(topicId, partition)` to the highest offset seen on that coordinate. Parsley serialises this
as a compact binary header (`parsley-causal-dependencies`) attached to every outgoing record.

On the consumer side, it is decoded into a `CausalDependencies` value: the minimum frontier
a downstream consumer must have reached before the record may be delivered. A record is **causally
ready** once the consumer's frontier has observed at least the offset its dependencies require on
every coordinate.

Keys are Kafka **topic UUIDs**, not topic names. UUID-keyed dependencies survive topic deletion and
recreation: a new incarnation of `prices` gets a new UUID and is treated as a distinct dependency,
so records stamped against the old `prices` are never accidentally satisfied by the new one.

## The frontier

The **causal frontier** is the node's internal clock: the highest offset it has successfully
delivered on each `(topicId, partition)` coordinate. Every time a record is forwarded, the frontier
advances (taking the per-coordinate maximum). It is an implementation detail — there is no public
frontier type; to propagate causal context downstream, read a consumed record's dependencies with
`CausalDependencies.fromRecord(record)` and stamp them on what you produce.

The frontier is **persisted** before each record is forwarded, so it survives restarts and rebalances.

## The causal buffer

Records whose dependencies are not yet satisfied are held in a **causal buffer**. Held records
survive a restart; no re-delivery from the broker is required.

When a coordinate in the frontier advances (because a causally ready record was forwarded),
Parsley checks the buffer for records that were waiting on that coordinate. If any are now
satisfied, they are released and their source coordinates advance the frontier in turn — cascading
until no further releases occur.

## Buffer limits

A `CausalBufferLimit` bounds how long or how large the buffer may grow before eviction fires:

| Limit | Factory | Description |
|---|---|---|
| Size | `CausalBufferLimit.ofSize(n)` | Fire when the buffer holds ≥ `n` records |
| Duration | `CausalBufferLimit.ofDuration(d)` | Fire after the buffer has held records for `d` (time-based; requires the processor to call eviction on schedule) |
| First-of | `CausalBufferLimit.first(a, b, ...)` | Fire when the first of several limits fires |

## Always-forward delivery

Parsley never drops or diverts a record. Every record reaches the user's `process()`/`poll()`
exactly once. In the common case it is delivered **in causal order** — the frontier had observed its
dependencies by delivery time (immediately, after a wait, or trivially: no dependencies claimed, or
an undecodable header, both treated as vacuously satisfied). The exception is **eviction**: when the
configured `CausalBufferLimit` fires before a held record's dependencies are satisfied, the record is
delivered anyway, out of causal order. The frontier always advances on delivery, so records buffered
downstream are not permanently stalled by an eviction.

## Causal violations

Every eviction is logged with the current frontier, the required dependencies, and the **causal
gap** — a per-coordinate shortfall showing exactly how far the frontier was behind at the time of
eviction — and counted via Parsley's eviction metric. Eviction is surfaced operationally (logs +
metric), not as a per-record signal on the delivered record.

## Co-partitioning

Parsley evaluates dependencies against the partitions assigned to a single consumer instance. For
the guarantee to hold, an instance must be assigned **all** partitions in a causally related set.
The standard approach is to **co-partition** related topics so causally related messages share the
same partition number across topics, allowing each instance to own partition *N* everywhere.

Parsley does not detect or enforce co-partitioning — a misconfigured topology evaluates against an
incomplete partition set silently.
