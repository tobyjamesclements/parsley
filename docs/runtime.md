# Runtime

## Start

`Parsley.start` performs the following for each declared process.

1. Resolves topic identities through the admin client, and fails where a received topic is
   missing.
2. Reads the group's committed positions. For received partitions with none, it pre-commits
   the declared initial position, taking earliest as the log start and latest as the end.
   The commit goes through a generation-fenced group membership rather than an admin
   alteration, so a stale paused bootstrap cannot overwrite a newer lifetime's positions.
   This gives `auto.offset.reset=none` a defined starting point and establishes the
   first-receipt baseline. On later starts, existing commits win.
3. Builds the topology and starts Kafka Streams with `processing.guarantee=exactly_once_v2`,
   from which `isolation.level=read_committed` follows.

Prior state pins the task width. The ordering store's changelog cannot change partition
count, so a width-changing declaration refuses with its remedy rather than failing inside the
host's internal-topic validation.

A channel that rejoins the received set resumes rather than re-entering delivered past. This
is backed by the `fedUpTo` record even where group positions have expired. A missing position
alongside prior state re-establishes earliest rather than the declared latest.

## Configuration lockdown

Configuration carrying the guarantee is set by the runtime and cannot be overridden.
`ParsleyConfig.Builder.streamsProperty` rejects the owned keys, among them
`processing.guarantee`, `isolation.level`, `auto.offset.reset`, `group.id` and
`transactional.id`. Neither the `Topology` nor raw configuration is exposed, so no documented
operation runs the topology without exactly-once semantics.

## Facts ingestion

Inside the processor, a punctuator periodically ingests position facts: committed positions,
log-start positions, and topic existence and identity. A name resolving to a different
identity is affirmative evidence of recreation. An unknown identity is corroborated against
its last-known name and debounced in time. An authorization denial is treated as denial
rather than death.

Gathering runs on one background thread per runtime. The punctuator snapshots the inputs,
applies each completed round exactly once on the stream thread, and never blocks on the
cluster. Every fact is a per-position lower bound, so a round applied one interval late
releases and prunes exactly what a fresh one would.

This punctuator is internal plumbing for ingesting the host's read-position reports, which is
the only transport Kafka Streams offers for them. It delivers nothing that was not received
from a channel, and the decision it triggers remains the pure function described in
[Delivery](delivery.md). Time never appears among that function's inputs. The public API
offers no timers and no scheduled callbacks.

## Zombie safety

Facts are monotone. Any delivery a superseded execution performs on stale state sits in a
transaction the host fences and aborts, so it has not occurred.

## The seam

```java
Effects handle(Delivery<K, V> delivery, StateReader state)
```

`Delivery` carries the delivered message. `StateReader` is a read-only typed view over the
process's declared stores. Effects return through the return value: typed sends, statically
typed per channel, and state writes.

The logic receives no other capability. Sending never blocks on the deliverability of the
message sent. Emissions are stamped with the current frontier and forwarded within the step.
