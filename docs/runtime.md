# Runtime

## Start

`Parsley.start` performs the following for each declared process, and returns once each
process's Kafka Streams application has been started — that is, into its first rebalance.
A refusal the bootstrap can see is thrown from the call; one raised inside task
initialisation on the host's threads surfaces through `Parsley.status()`.

1. Resolves topic identities through the admin client, and fails where any declared topic,
   received or sent, is missing.
2. Reads the group's committed positions and establishes, for each received partition, the
   position Kafka Streams will feed first, which every task takes as its start position at
   initialisation. Where prior state exists and a partition's committed position is missing
   — expired during a long stop — it pre-commits the ordering state's covered position plus
   one, the next position the previous execution would have read. Whether retention still
   holds it is decided at the first fetch: `auto.offset.reset=none` refuses a position below
   the log start, and the process stops with `POSITIONS_DISCARDED_UNREAD` (D115). Without
   prior state it pre-commits the declared initial position through one `listOffsets`, taking
   earliest as the log start and latest as the end. The commit goes through a
   generation-fenced group membership rather than an admin alteration, so a stale paused
   bootstrap cannot overwrite a newer lifetime's positions. This gives `auto.offset.reset=none`
   a defined starting point and establishes the first-receipt baseline. On later starts,
   existing commits win.
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
alongside prior state re-establishes coverage plus one rather than the declared latest, and
earliest where the ordering state covers nothing on the channel.

## Observing a process

`Parsley.status()` reports each process's lifecycle state and, when it has stopped to
preserve the guarantee, the refusal and its diagnosis. For a running process it also reports
each task's delivery state: what is held, which cause each head is waiting for, and the size
of the causal frontier every emission carries ([Failing closed](failing-closed.md#diagnosis)).
The snapshot is taken on the task's thread once per status interval
(`ParsleyConfig.statusInterval`) and asks nothing of a broker, so the call itself never
touches a stream thread or a broker.

## Configuration lockdown

Configuration carrying the guarantee is set by the runtime and cannot be overridden.
`ParsleyConfig.Builder.streamsProperty` rejects the owned keys, among them
`processing.guarantee`, `isolation.level`, `auto.offset.reset`, `group.id` and
`transactional.id`. Neither the `Topology` nor raw configuration is exposed, so no documented
operation runs the topology without exactly-once semantics.

## Identity at task initialisation

Nothing is asked of the broker between deliveries; the periodic round that once gathered
committed positions, log starts and topic identity is gone (D115). A cause names the offset
of a committed record, so receiving that record is what satisfies it, and the positions
between records that yield no message are settled by receipt of the next record on the
channel. The one question a task puts to the substrate is asked at its initialisation —
`ParsleyProcessor.init`, which Kafka Streams runs on the stream thread inside the rebalance,
at every creation and re-creation of the task.

There, `ParsleyProcessor.init` asks a `TopicIdentitySource` — in production
`AdminTopicIdentitySource`, backed by the admin client — about every topic id the task's state
names: the received topics at the identities resolved at start, and every topic in the
restored frontier. Each id is described by id. One that resolves is alive, and its name is
learned for later initialisations of the process's tasks. One the broker does not know is not
yet dead — a describe denial masks a live topic as unknown by id, and a broker's metadata view
can lag — so it is judged by its last-known name over three answers half a second apart. The
name gone in all three confirms deletion; the name resolving to another id in all three
confirms recreation; a denial, an unavailable answer, or the name resolving to the very id
asked about keeps it alive. A denial is denial, never death. An id whose name was never
learned is never confirmed dead: it lingers in the frontier, costing expression size and
never safety. A describe failure that is not the substrate's unknown-topic answer — a timeout,
an outage — is not evidence about any id: the initialisation proceeds with a warning, every
cause and every hold stays, and the question stays pending — each status punctuation asks it
again until it is answered, and the answer is applied as the initialisation's would have
been. The check is event-driven and eventual, never periodic.

The engine takes the verdicts through `ProcessEngine.onIdentityReport`. A received channel
whose topic was recreated under its name refuses `CHANNEL_IDENTITY_CHANGED`: records fed
under the old identity can no longer be trusted. A received channel whose topic was deleted
while messages from it remain held refuses `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES`, since
their place in causal order can no longer be preserved (D46); with nothing held it is settled
to the end of the channel (D21), and a hold waiting on it goes on the next punctuation or
record — nothing is delivered from initialisation itself (D34). Dead and recreated channels
are pruned from the frontier and the delivered past; that is the only pruning, and retention
never prunes. The runtime keeps no verdict windows, no eviction and no rescission.

A received topic deleted and recreated under its name while a process that receives it runs
is SPEC Assumption 17's territory: the recreation is detected at the next task
initialisation, and what was delivered in between is outside the guarantee. As observed on
Kafka Streams 4.3, deleting a received topic does not stop the process at once: the next
transactional commit times out, the host re-creates the task, and its initialisation runs the
identity check, which refuses. A rebalance that finds a source topic missing instead stops the
stream thread with the host's `MissingSourceTopicException`, which the runtime names
`SOURCE_TOPIC_MISSING` — a transient, with no `refusalReason`. Its log line says to restart;
the start then refuses `CHANNEL_IDENTITY_CHANGED` for a recreated topic, refuses a
still-missing one at resolution, and resumes where a broker's metadata merely lagged.

## The status punctuation

Each task schedules one wall-clock punctuation, every `ParsleyConfig.statusInterval` — one
second by default; the builder refuses a null, non-positive or sub-millisecond value. It
drains what receipt, or the initialisation's identity report, already released; flushes holds
to the ordering store, so a message held at the moment of a crash is still held after the
restart (D102); observes the frontier for the once-only warning at 80% of the metadata budget
(D53); and publishes the task's `TaskStatus` for `status()` (D103). It touches no broker and
ingests nothing.

The punctuation delivers nothing that was not received from a channel, and the decision it
triggers remains the pure function described in [Delivery](delivery.md). Time never appears
among that function's inputs. The public API offers no timers and no scheduled callbacks.

## Zombie safety

Any delivery a superseded execution performs on stale state sits in a transaction the host
fences and aborts, so it has not occurred.

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
