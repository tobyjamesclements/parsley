# Runtime

## Start

`Parsley.start` performs the following for each declared process, and returns once each
process's Kafka Streams application has been started — that is, into its first rebalance.
A refusal the bootstrap can see is thrown from the call; one raised inside task
initialisation on the host's threads surfaces through `Parsley.status()`.

1. Resolves topic identities through the admin client, and fails where any declared topic,
   received or sent, is missing.
2. Reads the group's committed positions. For received partitions with none, it pre-commits
   the declared initial position, taking earliest as the log start and latest as the end.
   The commit goes through a generation-fenced group membership rather than an admin
   alteration, so a stale paused bootstrap cannot overwrite a newer lifetime's positions.
   This gives `auto.offset.reset=none` a defined starting point and establishes the
   first-receipt baseline. On later starts, existing commits win.
3. Builds the topology and starts Kafka Streams with `processing.guarantee=exactly_once_v2`,
   from which `isolation.level=read_committed` follows. Before starting, it waits for any
   other instance's bootstrap member to leave the group, so two instances cold-starting
   together do not refuse each other's join; a join that still meets one replaces its
   stream thread and joins again.

Prior state pins the task width. The ordering store's changelog cannot change partition
count, so a width-changing declaration refuses with its remedy rather than failing inside the
host's internal-topic validation.

A channel that rejoins the received set resumes rather than re-entering delivered past. This
is backed by the `fedUpTo` record even where group positions have expired. A missing position
alongside prior state re-establishes earliest rather than the declared latest.

## Observing a process

`Parsley.status()` reports each process's lifecycle state and, when it has stopped to
preserve the guarantee, the refusal and its diagnosis. For a running process it also reports
each task's delivery state: what is held, which cause each head is waiting for, and the size
of the causal frontier every emission carries ([Failing closed](failing-closed.md#diagnosis)).
The snapshot is taken on the task's thread once per facts interval, so the call itself never
touches a stream thread or a broker.

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

Where a task holds messages, the round also probes the channels their heads wait on, under
`read_committed`, for a trailing run of positions that will never yield a message — an
aborted transaction at the end of a channel, which the host's committed position never
covers after a restart. The probe assigns every such channel at once and runs one bounded
poll loop, so it costs about a second per round however many channels are hinted; a channel
that itself holds messages is never probed, since its settled position is its head. The seed
round at task initialisation does not probe, so initialisation never waits on the broker for
it.

The dead-topic and recreation verdicts mature over an unbroken window of affirmative
answers; the window's continuity is judged on the time no round was asking, from one
round's end to the next round's first question, so a round's own queries and probes never
restart it.

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
An emission carries the delivered message's timestamp unless the handler gives it one of its
own; time-based retention and downstream event-time windows read that timestamp, so a
message emitted long after the one it answers — the release at the end of a compensation
chain — may want its own, derived from delivered data rather than a clock, since a handler
may run again for the same message and must return the same effects.

## Keys, partitions and state

A task is partition *p* of every topic its process receives, and it owns partition *p* of
every store the process declares. A read or write through the seam reaches that shard and no
other, so a key is found only if the delivering topic was keyed so that the same partitioner
put it on *p*. Two topics received by one process must therefore be partitioned alike where
their keys are meant to meet, and producers outside Parsley must partition by the same rule
— Kafka's default, unless every writer agrees on another. To keep state about a different
attribute than the delivered key, emit a message keyed by that attribute to a topic this or
another process receives; a channel a process both sends to and receives from is a
repartition. Received topics may have unequal partition counts; a task beyond a topic's width
receives nothing from it.

A handler that throws fails its step. The process stops, and on restart is fed the same
message and fails again: Parsley never skips a message. To continue past an application
failure, catch it and return effects that record it deterministically — an emission to a
declared dead-letter channel, or a state write — rather than throwing.

## Hosts

The host above is Kafka Streams, the default. `ParsleyConfig.Builder.host(Host.KAFKA_CLIENTS)`
opts a process into an experimental host over the plain kafka-clients consumer and producer
(D114). The seam, the delivery decision and the wire format are the same; what differs is
plumbing. The consumer's own position after each poll is the read-position report, so no
probe runs; the read position commits at the head of each channel's hold-back buffer and a
restart re-feeds the buffer from the log, so no held message is persisted and no record-size
limit applies to holds; initial positions commit under the group's generation in the first
transaction, so no bootstrap member joins; ordering state and application stores live in
compacted topics at the task width, one partition per task, materialised in memory on
assignment. The costs the record lists: stores are in memory and a held message stays decoded
on the heap.
