# Runbooks

The pages before this one say what stops a process and why. This page says what an operator
does about it: how to tell a refusal from a failure, what each refusal asks for, how to read
a process that holds and does not move, and how to reset a process when a refusal asks for
that. Each runbook names the checks to run and the order to run them in.

One rule governs all of them. A held or stopped process is never fixed by reordering,
skipping or timing out. A held message is waiting for a cause, and a refusal marks a point
past which causal order could not be preserved. Each remedy below either removes the
condition — a lagging producer, a limit set too low, a declaration that no longer matches its
state — or accepts a causal boundary deliberately, as a reset. Nothing here makes a process
deliver past a message.

## Before an incident

None of the following can be added once a process has stopped.

**Log the status.** `Parsley.status()` is the diagnosis surface. Its per-task detail — what
is held, which cause each head waits for, the frontier's width — is published by a task while
it runs and retired when the task closes. After a stop, `status()` still carries the
process's `refusalReason` and `failureDetail`, but no task detail: what was held at the
moment of the stop is no longer readable from the process. Log the status on a timer while
the application runs, and once more when the wait ends:

```java
try (Parsley parsley = Parsley.start(config, shipper)) {
    parsley.awaitStopped();
    parsley.status().forEach((name, status) -> log.error("{}: {} refusal={} detail={}",
            name, status.state(), status.refusalReason(), status.failureDetail()));
}
```

Export the same snapshot through whatever health endpoint or metrics registry the deployment
already has. Parsley exports nothing itself: no metrics, no callback
([what the library does not do](#what-the-library-does-not-do)).

**Fix the restart rule.** A refusal recurs identically on restart; a transient does not. A
supervisor that restarts on every exit turns a refusal into a loop that re-runs the
bootstrap, re-fails, and pages nobody. The rule to wire in:

| `status()` shows | Restart? |
|---|---|
| `refusalReason` present, other than `COVERED_POSITION_FED` | No. It recurs. Follow the reason's runbook and page a person. |
| `refusalReason` of `COVERED_POSITION_FED` | Once. An engine invariant guard with no known trigger: the restart resumes from the committed record, and a recurrence means the invariant broke — report it with the logs rather than restart again ([runbook](#covered_position_fed)). |
| `refusalReason` absent, `failureDetail` naming a broker, timeout, topic or partition condition | Yes, with backoff, once the cluster is reachable ([runbooks](#restart-resolves-it)). |
| `refusalReason` absent, `failureDetail` naming the handler's own exception | No. The same message is fed again and the handler throws again ([runbook](#a-handler-that-throws)). |

Nothing in `status()` separates the last two shapes for a supervisor; only the text of
`failureDetail` does. A restart bounded by an attempt count, with the last `failureDetail`
in the page, is the practical rule.

**Know the names.** Every command below takes the application id `<prefix>-<process>` and
the changelog names listed under [Operations](operations.md#names-on-the-cluster). Know
`state.dir` too: the Kafka Streams default is `kafka-streams` under the JVM's temporary
directory, `/tmp/kafka-streams` on most systems, and a reset visits it on every instance.

**Hold the tools and the rights.** The runbooks use the Kafka command-line tools against the
cluster: `kafka-consumer-groups`, `kafka-topics`, `kafka-get-offsets`, `kafka-configs` and
`kafka-console-consumer`. A reset needs Delete on the process's group and on its changelogs,
rights the running application deliberately does not hold; keep them with the operator.

**Set retention with stops and lag in mind.** A message held for a cause is safe: it is in
the ordering changelog, and retention discarding its copy on the topic does not touch it.
What retention must cover is the process's read position. Retention on every received topic
must cover the longest the process may be stopped and the longest it may lag, since a
committed read position that retention passes refuses at the fetch
([Operations](operations.md#sizing)). The [retention clock](#the-retention-clock) below says
how to see it running down.

## Triage

A stop announces itself four ways: `awaitStopped()` returns, `healthy()` turns false, the
log carries an `ERROR` line ending "(failing closed)", or `Parsley.start` throws.

### If `start` threw

The start is all-or-nothing, so an exception from it means nothing is running.

| Exception | Meaning | Action |
|---|---|---|
| `ParsleyFailClosedException` | A refusal the bootstrap could see: state, positions or a declaration that cannot be resumed against. The message names the reason and its remedy. | The reason's runbook below. |
| `IllegalStateException` ending "Retry this start." | A broker's metadata view lagged, or a sibling instance was mid-start. | Retry. |
| Any other `IllegalStateException` | A prerequisite is missing or the cluster could not be queried: a declared topic does not exist, the changelog could not be read, positions could not be listed. | Create the topic, restore connectivity, check the ACLs; then retry. |
| `IllegalArgumentException` | The declaration is wrong: a duplicate name, a reserved name, two stores composing one changelog name. | Fix the declaration. |

### If a process stopped after start

Read `status()` for the process. There are three shapes.

1. `state` is `STOPPED` and `refusalReason` is present: a deliberate refusal.
   `failureDetail` carries the diagnosis, which names the process, usually the topic,
   partition and position, and where one exists the remedy. Go to the reason's runbook.
2. `state` is `STOPPED` and `refusalReason` is absent: an application failure or a substrate
   failure. `failureDetail` is the outermost exception's message; the host's wrapping names
   the task, topic, partition and offset of the record in flight. See
   [restart resolves it](#restart-resolves-it) and [a handler that throws](#a-handler-that-throws).
3. `state` is `STOPPED` and nothing is recorded: the application closed the handle.

The reasons, and what each asks for:

| Reason | A restart alone | A reset | Runbook |
|---|---|---|---|
| `COVERED_POSITION_FED` | Once; a recurrence is reported, not restarted | No | [Restart resolves it](#covered_position_fed) |
| `HANDLER_RETURNED_NULL_EFFECTS` | After the code fix | No | [The application](#handler_returned_null_effects) |
| `EMISSION_TO_UNDECLARED_CHANNEL` | After the declaration fix | No | [The application](#emission_to_undeclared_channel) |
| `STATE_ACCESS_TO_UNDECLARED_STORE` | After the declaration fix | No | [The application](#state_access_to_undeclared_store) |
| `RESERVED_HEADER_USED` | After the code fix | No | [The application](#reserved_header_used) |
| `APPLICATION_PAYLOAD_UNSERIALIZABLE` | After the code fix | No | [The application](#application_payload_unserializable) |
| `APPLICATION_PAYLOAD_UNDECODABLE` | After the serde fix, or once a serde's dependency is back | No | [The application](#application_payload_undecodable) |
| `METADATA_BUDGET_EXCEEDED` | After raising the budget and the record limits | No | [Configuration](#metadata_budget_exceeded) |
| `SUBSTRATE_MISCONFIGURED` | After the cluster fix | No | [Configuration](#substrate_misconfigured) |
| `TASK_WIDTH_CHANGED` | After restoring the declaration | Otherwise yes | [Configuration](#task_width_changed) |
| `CHANNEL_REMOVED_WITH_HELD_MESSAGES` | After restoring the channel | Otherwise yes | [Configuration](#channel_removed_with_held_messages) |
| `POSITIONS_DISCARDED_UNREAD` | No | Yes | [The causal past is gone](#positions_discarded_unread) |
| `ORDERING_STATE_LOST` | No | Yes, unless the changelog can be restored | [The causal past is gone](#ordering_state_lost) |
| `CHANNEL_IDENTITY_CHANGED` | No | Yes | [The causal past is gone](#channel_identity_changed) |
| `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` | No | Yes | [The causal past is gone](#channel_deleted_with_undelivered_messages) |
| `UNKNOWN_ORDERING_STATE_FORMAT` | With the build that wrote the state, for a version mismatch | Otherwise yes | [The causal past is gone](#unknown_ordering_state_format) |
| `UNDECODABLE_METADATA` | No | Only a reset that starts past the message | [Neither restart nor reset](#undecodable_metadata) |
| `OUT_OF_ORDER_FEED` | Once, for the in-execution shape | Yes, for the dead-channel shape | [Neither restart nor reset](#out_of_order_feed) |

### Contain

What else is affected, in the order to check it.

- **The other instances.** A refusal stops the instance that met it, not the application as
  a whole. The task migrates to another instance, which meets the same condition and stops
  in turn, one rebalance at a time. Each instance's status shows the refusal only once that
  instance has met it. Expect a slow cascade, and do not read the instances still running
  as evidence that the condition is confined.
- **Downstream processes.** A stopped process sends nothing, so every process that receives
  from it waits for causes it cannot send while stopped. Those processes hold; they do not
  fail. Their status names the stopped process's output topic as the blocker, with the
  required position at or beyond that topic's end. Fix the upstream stop; never reset a
  downstream process for it.
- **The other processes in the handle.** Each declared process is its own Kafka Streams
  application, and a stop in one leaves the others running. `awaitStopped()` returns on
  the first stop; closing the handle stops them all. Whether to keep the healthy ones up
  is the application's choice.

### Do not

- Move the group's offsets with `kafka-consumer-groups --reset-offsets`. A commit the
  bootstrap did not write, with no ordering records behind it, refuses the next start with
  `ORDERING_STATE_LOST`. Otherwise the next start adopts the moved offset as its start
  position, with no refusal: moved forward, the positions it jumped are covered as never
  arriving and are never delivered; moved backwards, the records fed again below the
  session floor are dropped as replays. Neither reaches a running process — the tool moves
  offsets only in an inactive group.
- Delete the ordering changelog alone, or the offsets alone. Each produces a different
  refusal, not a reset.
- Run the Kafka Streams application reset tool by itself. It resets offsets and deletes
  internal topics but leaves local state.
- Add a timeout, a skip or a reorder in application code, or catch a refusal in a handler.
  Every refusal raised inside the handler frame is latched and rethrown at the frame's
  boundary, so a catch changes nothing but the stack trace.
- Restart in a loop on a refusal.

## Runbooks by reason

Each runbook gives the shape of `failureDetail`, what happened, what to check, what to do,
and whether anything must be reset.

### Restart resolves it

#### COVERED_POSITION_FED

**Shape.** `fed <channel>@<n> which this execution's own coverage already records as fed or
never arriving (fedUpTo=<m>), above the session floor of <f>; the host's feed and the
engine's record contradict each other. A restart resumes from the committed record, and this
refusal then does not recur.`

**What happened.** An engine invariant broke. Above the session floor — the coverage a task
initialises with — coverage is raised only by this execution's own receipts, or to the
fed-to-end sentinel by the identity check for a channel whose topic is gone, and a feed
below either is refused first as `OUT_OF_ORDER_FEED`. This branch is kept so that a
contradiction between the host's feed and the engine's record can never fall through to a
silent drop or a delivery; nothing known reaches it (D115).

**Check.** The log around the stop, in full, and the last logged status before it.

**Do.** Restart the instance once. It resumes from the committed record, and the refusal
does not recur. If it recurs, keep the logs and report it; do not keep restarting.

#### A partition was added while the process ran

**Shape.** No `refusalReason`. The `ERROR` line says either that a received partition has
no committed read position, or that the partition shape of the process's topics changed
while it ran; `failureDetail` carries the client's own message beneath the host's wrapping.

**What happened.** Parsley resolves partition counts at start and pre-commits a position for
each. A partition that appears afterwards has no committed position, and
`auto.offset.reset=none` stops the task rather than guess one.

**Do.** Restart the application. The start re-resolves every topic and pre-commits the new
partitions at earliest, whatever the channel declares — a process with prior state always
begins a new partition at earliest. If the widened topic was the process's widest received
topic, the restart refuses with [`TASK_WIDTH_CHANGED`](#task_width_changed) instead. Either
way, adding partitions changes which partition a key lands on, so state kept per key under
the old count is now split across two shards ([Runtime](runtime.md#keys-partitions-and-state)).

#### A received topic went missing during a rebalance

**Shape.** No `refusalReason`. The `ERROR` line says that a received topic was missing when
the host rebalanced — deleted, or deleted and recreated under its name, while the process
ran — and names the remedy; `failureDetail` carries the host's own message, "One or more
source topics were missing during rebalance", beneath the wrapping.

**What happened.** A rebalance found a source topic gone. A received topic is assumed not to
be deleted and recreated under its name while a process that receives it runs, and not to be
deleted while anything from it is still owed (SPEC Assumption 17); when a rebalance finds it
missing, Kafka Streams stops the thread with this transient, and whether the topic is gone
or merely renewed is judged at the next start, not here.

**Do.** Restart the application. A topic still missing refuses the start at resolution until
it is restored or removed from the declaration, and the removal is refused with
[`CHANNEL_REMOVED_WITH_HELD_MESSAGES`](#channel_removed_with_held_messages) while messages
from it remain held; one recreated under its name refuses with
[`CHANNEL_IDENTITY_CHANGED`](#channel_identity_changed); one that merely lagged in a broker's
metadata resumes, and nothing is lost.

#### The cluster was unreachable

**Shape.** No `refusalReason`; `failureDetail` names a `TimeoutException`, a transaction
timeout, a coordinator not available, or a similar client failure.

**What happened.** Kafka Streams retried until its own task timeout and then ended the
thread. Nothing was delivered out of order and nothing was lost: held messages are in the
ordering changelog, and a step that did not commit is re-fed on restart.

**Do.** Restart once the cluster is reachable. A held message survives a full broker bounce
neither lost nor released, so no reset is ever the answer to an outage.

### The application must change

Every refusal here is raised by the handler frame — the declared serdes, the handler, the
effects it returned — and none touches ordering state. The remedy is a code or declaration
change, a deploy, and a restart; the process resumes into the same message and delivers it.
Nothing is reset.

#### HANDLER_RETURNED_NULL_EFFECTS

**Shape.** `handler for <topic> returned null effects; return Effects.none() for a step that
changes nothing`

**Do.** Return `Effects.none()` from the branch that returned `null`. Deploy and restart.

#### EMISSION_TO_UNDECLARED_CHANNEL

**Shape.** `<process> emitted to undeclared channel <topic>`

**What happened.** The handler sent to a topic outside the process's declared send set.
Membership is by topic name.

**Do.** Either the handler is wrong, or the declaration is: add the channel with
`.sends(channel)` if the emission is intended. The topic must exist before the start. A send
set can change freely between executions; it touches no ordering state. Deploy and restart.

#### STATE_ACCESS_TO_UNDECLARED_STORE

**Shape.** `<read|write> targets a store not declared by stores(...): <store>`, with
`(a Store instance other than the declared one)` when a store of that name is declared.

**What happened.** Either the store is not in `.stores(...)`, or the handler used a second
`Store.of` for the same name. The seam matches stores by instance.

**Do.** Declare the store, or pass the declared instance to the handler. A store added to the
declaration gets its changelog created on the next start. Deploy and restart.

#### RESERVED_HEADER_USED

**Shape.** `serializer for <topic> wrote reserved header '<name>'`

**What happened.** A header on an emission began with Parsley's reserved prefix. The
emission's own headers are checked as the handler built them; a serializer that adds
headers of its own is the usual source.

**Do.** Rename the application header, or configure the serializer not to write into the
reserved namespace. Deploy and restart.

#### APPLICATION_PAYLOAD_UNSERIALIZABLE

**Shape.** `<topic> payload could not be serialized by the declared serde`,
`<store> state write key serialized to null; the declared key serde could not encode it`,
or `<store> state read key could not be serialized by the declared serde`.

**What happened.** A declared serde threw, or returned `null` for a key. A `Channel` or
`Store` instance whose types differ from the declared one — a look-alike built with other
serdes — is the common cause; the declared channel's serdes produce the bytes whatever
instance carried the effect.

**Do.** Fix the serde or the type at the effect. Effects are serialized during planning,
before any write applies, so the failing step left nothing partial behind. Deploy and
restart.

#### APPLICATION_PAYLOAD_UNDECODABLE

**Shape.** `<topic>@<position>` for a delivered message's payload, or `<store> stored value
could not be decoded by the declared serde` for a state read.

**What happened.** The declared deserializer threw. Two very different conditions produce
this, and the first check is which one.

**Check.** Whether the deserializer depends on something outside the bytes — a schema
registry, a key service — and whether that dependency was reachable at the time. If it was
not, this is the one refusal that does not recur: a restart after the dependency is back
decodes the same bytes. If the dependency was fine, the bytes are the problem: read the
record with `kafka-console-consumer --topic <topic> --partition <p> --offset <position>
--max-messages 1 --isolation-level read_committed` and decode it by hand.

**Do.** For a delivered payload whose bytes are wrong: fix whatever produced them, and then
get the process past the record. Parsley never skips a message, so the way past is a serde
that decodes the bad bytes into a value the handler recognises and dead-letters — an emission
to a declared channel, or a state write — rather than throwing. For a stored value: the bytes
were written by an earlier version of the value serde, so the fix is a serde that reads both
encodings. Never delete a store's changelog to clear a bad value; under exactly-once
semantics it committed together with the ordering state and read positions, and removing it
alone loses application state with nothing to replay it from.

#### A handler that throws

**Shape.** No `refusalReason`; `failureDetail` is the host's wrapping of the handler's
exception, naming the task, topic, partition and offset.

**What happened.** An application failure. The process stops, and a restart feeds the same
message again and fails again, because nothing is ever skipped.

**Do.** Fix the bug, or catch the failure inside the handler and return effects that record
it deterministically — a dead-letter emission or a state write — so the step commits. A
handler that calls out to a dependency is outside what the guarantee covers; if the failure
came from such a call, restart once the dependency is back, and consider moving the call
out of the handler. Nothing is reset.

### The configuration or the cluster must change

#### METADATA_BUDGET_EXCEEDED

**Shape.** One of three sites. On receipt: `<channel>@<position> carries <n> bytes of causal
metadata; the configured budget is <b> bytes`. On merge: `the causal frontier reached <n>
bytes (<k> channels)`. On emission: `expressing the causal frontier needs <n> bytes`.

**What happened.** The frontier every emission carries outgrew `metadataBudgetBytes`, 256 KiB
by default. Its steady-state size is the sum of partition counts over the transitive
upstream closure of the process's inputs, at roughly 26 bytes for a topic's first partition
and 9 for each further one ([Model](model.md#pruning-and-growth)). The 80% warning in the
log is the early signal for this refusal. A receipt-site refusal names a message whose
sender's frontier is already wider than this process's budget.

**Check.** `frontierChannels` and `frontierBytes` from the logged status against the budget.
Whether a new upstream topic, or a widened one, joined the closure. For a receipt-site
refusal, whether the message came from a Parsley process or from a producer stamping
`parsley.causes` itself — a gateway forwarding a session token, for instance, whose token
may be over-broad ([Session consistency](session.md)).

**Do.** Compute the closure's size and set `metadataBudgetBytes` above it with headroom, on
this process and on every process the frontier reaches, since a frontier expressed by one
process is received by the next. Then check the record limits: a held message is persisted
with its causes, and an emission carries the frontier on the sent topic, so the ordering
changelog's and every sent topic's `max.message.bytes`, and the producer's
`max.request.size`, must cover the largest payload plus the budget
([Operations](operations.md#sizing)). Restart. The refused message is fed again and
delivered. Never lower the budget below a running process's frontier: a restored frontier
past a shrunken budget refuses at the next emission.

#### SUBSTRATE_MISCONFIGURED

Three shapes, each naming a different fix.

**A record exceeded a size limit.** `a record exceeded a size limit, typically a held
message's persisted form against the ordering changelog's max.message.bytes`. A held message
is written to the ordering changelog with its payload, headers and causes, which the
metadata budget alone does not bound. Raise the limit on the changelog topic:

```
kafka-configs --bootstrap-server <servers> --alter --entity-type topics \
    --entity-name <prefix>-<process>-__parsley.ordering-changelog \
    --add-config max.message.bytes=<bytes>
```

and, if the producer's limit is lower, set `producer.max.request.size` through
`ParsleyConfig.Builder.streamsProperty`. Restart; the held message is persisted and
delivered. If the failing topic is a sent topic rather than the changelog, raise that topic's
limit instead.

**A persistent group-protocol conflict.** `the group join has been refused as a protocol
conflict for longer than <window>, so a member speaking another group protocol persists in
this group`. Something is in the consumer group under this application id that is not this
process: a consumer configured with the application id as its `group.id`, or another
instance's bootstrap member that never left. List the members with
`kafka-consumer-groups --describe --group <app-id> --members`; a bootstrap member's client
id begins `parsley-bootstrap-`, and one whose instance died leaves at its session timeout on
its own. Remove the foreign consumer, then restart.

**A topic has no topic id.** `topic '<name>' has no topic ID; brokers below the supported
3.7.0 floor cannot provide channel identity`. Refused at start. Channel identity is what
lets Parsley tell a recreated topic from the original, and brokers before 3.7.0 do not
assign it. Upgrade the brokers.

#### TASK_WIDTH_CHANGED

**Shape.** `this process's ordering state was built for <n> task(s) but the declaration now
induces <m> (the widest received topic's partition count changed)`. Refused at start.

**What happened.** A process runs as many tasks as its widest received topic has partitions,
and the ordering changelog was created with that count. Either the declaration changed —
a received topic was added or removed and the maximum moved — or partitions were added to
the widest topic. Kafka cannot remove partitions, so the second cannot be undone.

**Do.** If the declaration changed, restore it and restart: nothing is lost. If partitions
were added to the widest topic, the ordering changelog cannot follow, and the only way
forward is a [reset](#resetting-a-process). Before it, note that the wider topic also
re-maps keys to partitions, so per-key application state is split across shards from the
reset onwards. Plan partition counts with headroom on the widest topic.

#### CHANNEL_REMOVED_WITH_HELD_MESSAGES

**Shape.** At start: `received messages remain undelivered on [<topicId>/<partition>, ...],
which the new declaration no longer receives`, naming channels by topic id. At task
initialisation: `held message at <channel>@<position> but the channel is no longer in the
declared received-channel set`.

**What happened.** The declaration dropped a received channel while messages from it were
still held for a cause. Dropping it would strand them: they can never be delivered, and the
process's later sends would express nothing about them.

**Check.** Map the topic id to a name with `kafka-topics --describe`, which prints each
topic's id. Confirm the topic still exists; if it does not, this is
[`CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES`](#channel_deleted_with_undelivered_messages).

**Do.** Put the channel back in the declaration, with a handler that does the right thing for
the remaining messages, and restart. Watch `status()` until every task reports no held
messages on that topic, then remove the channel and restart again. That path loses nothing.
A reset is the alternative, and discards the held messages with the rest of the causal past.

### The causal past is gone

Every refusal here means the process's record of what it delivered, or the positions that
record refers to, no longer exists or can no longer be trusted. Nothing can be restored from
inside the process; the choices are to restore the missing thing from outside — a changelog
from a backup, a build that reads the state — or to [reset](#resetting-a-process) and start
a new causal lifetime deliberately.

#### POSITIONS_DISCARDED_UNREAD

**Shape.** One site, the consumer's fetch: `the broker no longer retains this process's
committed read position; positions were discarded before they were read`. The log line's
cause, beneath the runtime's message, is the consumer's own `OffsetOutOfRangeException`,
which names the partition and the position that could not be fetched.

**What happened.** Retention passed the process's committed read position on a received
partition while the process was stopped or lagging. Under `auto.offset.reset=none` the
consumer refuses the fetch rather than skipping to the log start, and the runtime names the
refusal. It is met while running, when retention passes a lagging read position; on the
first fetch after a start whose group offsets had expired, which resumes at the ordering
state's covered position plus one; or on the first fetch after a stop long enough for
retention to pass the committed position. A held message is never the position at risk: it
is in the ordering changelog and delivers from there once its causes settle, whatever its
topic still retains.

**Check.** `kafka-consumer-groups --describe --group <app-id>` for the group's committed
offset on the partition the cause names, and `kafka-get-offsets --topic <topic>
--partitions <p> --time earliest` for the current log start; the positions between them are
what was discarded. The topic's `retention.ms` and `retention.bytes` against how long the process was
stopped, or how far behind it ran. Whether the topic's data survives anywhere else — a
mirror, a backup — which is rare.

**Do.** Record the gap: those positions are what the application has lost, and its own
reconciliation may need them. Then reset. Afterwards, set retention on every received topic
to cover the longest stop and the longest lag the application can see, and alert on the
[retention clock](#the-retention-clock) before it runs out.

#### ORDERING_STATE_LOST

**Shape.** `committed read positions exist for <topic-partition>, stamped by a previous Kafka
Streams execution | committed outside parsley (external tooling, or pre-seeded offsets), but
partition <p> of this process's ordering-store changelog holds no ordering records | this
process's ordering-store changelog does not exist`. Refused at start.

**What happened.** Every committed step writes ordering state and read positions together.
Offsets without the ordering records that must accompany them mean one of: the changelog was
deleted while the offsets were kept (a reset done in the wrong order); its records were
purged (`kafka-delete-records`, or a `cleanup.policy` changed away from `compact` so
retention aged them out); or the offsets were seeded from outside before a first start.

**Check.** `kafka-topics --describe --topic <prefix>-<process>-__parsley.ordering-changelog`
for existence and partition count; `kafka-configs --describe` on it for `cleanup.policy`,
which must be `compact`; the provenance the diagnosis names.

**Do.** If the changelog's records can be restored from a backup, restore them and start.
Otherwise finish the reset: delete the group's offsets and any remaining changelogs, and
start fresh. For pre-seeded offsets, delete them; Parsley establishes its own initial
positions and accepts no others ([what the library does not do](#what-the-library-does-not-do)).

#### CHANNEL_IDENTITY_CHANGED

**Shape.** Three sites. At start: `topics [<names>] now resolve to different identities than
this process's state was built against`. At task initialisation, from the stored name
binding: `topic '<name>' now resolves to <channel> but this process's state was built
against a previous incarnation`. At task initialisation, from the identity check: `the topic
of received channel <channel> was deleted and recreated under the same name while this
process ran`.

**What happened.** A received topic was deleted and recreated under the same name. Positions
in the old log mean nothing in the new one. The third verdict comes from the identity check
every task initialisation runs: the old id no longer resolves, and the name resolves to
another id in three answers half a second apart. A recreation while a task runs is detected
when the task next initialises — Kafka Streams re-creates the task once the commit against
the deleted topic times out, or stops the thread with the
[missing-source-topic transient](#a-received-topic-went-missing-during-a-rebalance) — and
what the process delivered in between is outside the guarantee (SPEC Assumption 17).

**Check.** `kafka-topics --describe` shows the topic's current id; the diagnosis names the
one the state was built against. Whether the recreation was intended.

**Do.** Reset. A related trap looks identical: reusing an application-id prefix and process
name for a different pipeline inherits the old process's state, and every topic it named
resolves to something else. Start a different pipeline under a different prefix, or reset
before reusing one.

#### CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES

**Shape.** `<n> received message(s) from <channel> remain undelivered but the channel's
topic no longer exists; their place in causal order can no longer be preserved`.

**What happened.** A received topic was deleted while this process held messages from it,
which breaches the deletion-hygiene assumption: a topic is deleted only once nothing holds
an undelivered message from it. The held messages cannot be delivered in order, and cannot
be skipped. The refusal is raised at task initialisation, by the identity check that finds
the topic gone — its name unknown in three answers half a second apart — while the task
still holds messages from it.

**Check.** The last logged status before the stop for what was held on that channel, since
the process can no longer say. Whether the topic is coming back under the same name; if so
its identity differs and the next start refuses with
[`CHANNEL_IDENTITY_CHANGED`](#channel_identity_changed).

**Do.** Remove the channel from the declaration, or recreate the topic, then reset. The held
messages are lost with the causal past; the logged status is the only record of what they
were.

#### UNKNOWN_ORDERING_STATE_FORMAT

Four shapes, and the first needs no reset.

**A version this build cannot read.** `ordering store format not understood by this build`.
The state was written by a newer build than the one now running. Run the build that wrote
it. Nothing is wrong with the state.

**State without its version entry.** `ordering state present without its format version
entry; the earliest records of the ordering changelog have been lost`. The version entry is
written once by a task's first step, and compaction retains it forever; its absence means
the changelog's earliest records were aged out, which a `cleanup.policy` other than
`compact` does. Check the changelog's `cleanup.policy`, restore it to `compact`, and reset.

**A corrupt entry.** `corrupt ordering key`, `corrupt ordering value`, `corrupt held blob`,
`held message ... is in the hold-back buffer but absent from the store`, or `held messages
restored out of position order`. Something other than Parsley wrote to the changelog, or the
local state store is damaged. Wipe the process's local state directory on the affected
instance first and restart: the store restores from the changelog, and if the changelog is
sound the refusal does not recur. If it recurs, the changelog itself is damaged; reset, and
report it.

**A forged header absorbed.** `restored frontier names the reserved zero topic id` or
`restored <what> names position <p> on <channel>, which no channel can assign`. A producer
stamped a `parsley.causes` header naming a coordinate no channel can have, and the state
absorbed it before receipt learned to refuse it. Find and fix the producer, then reset.

### Neither restart nor reset

#### UNDECODABLE_METADATA

**Shape.** `<channel>@<position>: <detail>`, where the detail says what about the header
is not the frozen grammar: `truncated causes header`, `malformed causes header`, `non-minimal
varint`, `topics not strictly ascending`, a zero topic id, a negative or reserved position.

**What happened.** A record on a received topic carries a `parsley.causes` header that is
not the wire format. A process on this version of the wire format cannot produce one, so
the writer is something else: a producer on another version of the wire format, a gateway
stamping a session token by hand, a foreign application using the reserved header name. The
message cannot be treated as having no causes, and everything received after it on that
channel is held behind it.

**Check.** Read the record with `kafka-console-consumer --topic <topic> --partition <p>
--offset <position> --max-messages 1 --property print.headers=true --isolation-level
read_committed` and identify the producer from its key, value or other headers.

**Do.** Fix the producer, so no more such records arrive. The refusing record stays on the
topic, and no restart passes it: Parsley skips nothing, and offers no way to skip one
message. The only ways past are a [reset](#resetting-a-process) with the affected channel
declared `startingAt(LATEST)`, which skips everything retained on it, or a reset taken once
retention has discarded the record. Both are causal boundaries, and both are the operator's
deliberate act.

#### OUT_OF_ORDER_FEED

Two shapes with different remedies.

**Within an execution.** `fed <channel>@<p> after this execution was already fed position
<q> of the same channel`. The host fed a channel backwards inside one execution, which
Kafka Streams does not do. Restart: the check is per execution, so a restart that feeds in
order clears it. If it recurs at the same positions, keep the logs and report it.

**On a channel recorded as gone.** `fed <channel>@<p> on a channel recorded as no longer
existing`. An identity check at a task initialisation had concluded that the channel's topic
was deleted, the process recorded it as fed to its end, and records then arrived from that
identity. A topic id never returns once deleted, so the verdict was wrong; the check that
gave it is the suspect, and the refusal is durable. Reset, keep the logs from the
initialisation that gave the verdict, and report it.

### Reading a stopped process's state

No runbook above can ask the stopped process what it holds. What can be read is its
ordering changelog: `OrderingStateInspector` in the `core` package answers, from the
changelog's latest value per key, which channels hold messages, how far each channel was
covered, and which topic identity each name was bound to. Reading the changelog into that
map is the operator's work; nothing in the library does it for a stopped process.

## A message is held and not moving

A held message is not a failure. It is waiting for a cause, and the diagnosis is which
cause and why it has not arrived. `status()` names both, once per status interval, for every
task the instance runs ([Failing closed](failing-closed.md#diagnosis)).

### Confirm it is a hold

`state` is `RUNNING`, `refusalReason` is empty, and a task's `heldMessages` is above zero
and not falling. A `state` of `REBALANCING` that persists is the host, not a hold: a member
that cannot join, or a task restoring a large changelog, which for a deep hold-back backlog
takes time.

### Read the head's blockers

For each held channel, the status carries the position of the head — the oldest held
message, the one the decision reads — and every cause it still waits for, each with the
position required and the position the cause's channel has settled to. The shape of the
blocker says what to do.

**No blockers.** The head is deliverable and goes on the next drain, which runs on the next
record fed or the next status punctuation. A head that stays deliverable across status
intervals means the task's thread is not running: check the thread.

**Settled position empty.** The task starts that channel at position 0 and has received
nothing on it yet, so nothing on it is settled. Check that the partition exists — the
topic's partition count from `kafka-topics --describe` — and whether the group's offset on
it is moving, from `kafka-consumer-groups --describe --group <app-id>`.

**Settled below required, and the required position exists.** Compare the required position
with the channel's end from `kafka-get-offsets --topic <topic> --partitions <p> --time
latest`. Required at or below the end means the cause is on the channel and this task has not
reached it. Two sub-cases:

- The cause's channel itself holds messages. Then its settled position is its own head less
  one, and the hold is a chain: read that channel's head and its blockers, and follow the
  chain until a blocker's channel holds nothing. That channel is the root.
- The cause's channel holds nothing. This task is behind on it. `kafka-consumer-groups
  --describe --group <app-id>` shows the group's current offset and lag on the partition;
  the task's settled position is what it has been fed, and lag is what closes it, so lag
  here is throughput, a paused partition, or a thread that has stopped polling. An offset
  already past the required position with settled still below it is the shape after next.

**Settled below required, and the required position does not exist yet.** Required beyond
the channel's end means the cause has not been produced. Nothing a process expresses names
an unassigned position, so this has two honest explanations. The position lies between the
last stable offset and the end — inside a transaction still open on that channel — which
resolves when the transaction commits or the broker aborts it at its timeout, ten seconds
under the Kafka Streams defaults. Or the metadata is stale or forged: a token minted against
another cluster, a hand-stamped header. Read the held record's header and find its producer.

**Required at an offset no committed record occupies.** The group's offset on the partition
stands past the required position with no lag, and settled still trails it. The cause names
a position no `read_committed` reader is ever served: a transaction marker, an aborted
batch's offset, the log-end offset at the moment of stamping. That is an out-of-contract
cause ([wire format](wire-format.md#grammar), constraint 8): no receipt settles it, elapsed
time never does, and the hold stays, visibly, until a later record on the channel arrives. A
Parsley process never stamps one. Read the held record's header and find the producer of
the stamp — a gateway minting a token from a producer's log-end offset is the natural
mistake — and fix it; the next record on the channel releases the hold.

**The blocker's channel's topic no longer exists.** `kafka-topics --describe` does not find
the topic, or finds it under another id. A cause on a deleted channel is settled by the
deletion, and a task learns of a deletion from the identity check at its next
initialisation, never while it runs. On Kafka Streams the deletion of a received topic
reaches the process within a minute or two — the commit times out and the host re-creates
the task, or a rebalance stops the thread
([runbook](#a-received-topic-went-missing-during-a-rebalance)) — and the check at that
initialisation settles the channel to its end, sending the hold on at the next punctuation,
provided the task holds nothing from that channel itself; a topic recreated under its name
refuses [`CHANNEL_IDENTITY_CHANGED`](#channel_identity_changed) instead. If the process has
stopped, restart it: a start with the topic still missing refuses at resolution until the
topic is removed from the declaration, and a channel outside the received set no longer
constrains this process. If the check could not be made, the log
[says so](#the-identity-check-could-not-be-made).

**The blocker's topic is another Parsley process's output.** Look at that process. If it has
stopped, its refusal is the root cause, and this hold clears when it is fixed. If it is
holding, follow its blockers upstream. A stopped process stalls every process downstream of
it, and the stall is visible in each downstream status as a blocker on the stopped process's
output topic.

### The identity check could not be made

Every task initialisation asks the cluster about every topic its state names — the received
topics and every topic in the frontier — and settles or prunes the channels whose topics are
confirmed gone. Two log lines say the asking failed. "topic identity could not be checked;
continuing on the identities resolved at start, and asking again at the next status
punctuation", with the cause, means the check could not be made: the admin client could not
reach the cluster, or a describe failed in a way that is not the broker's unknown-topic
answer. The question stays pending, and each status punctuation asks it again until it is
answered — the line repeats once per interval while it does — and the answer is then applied
as the initialisation's would have been. "describe denied for topic '<name>' (<id>);
treating as denied, not dead" means the application's principal lacks Describe on that
topic, which the check declines to read as deletion; that answer counts as an answer, so it
is not asked again until the task next initialises. Neither is evidence: no channel is
settled or pruned on the strength of either, so a dead channel a hold waits on stays
unsettled, and its causes stay expressed on every send, until the check gets an answer.
Restore the connectivity and the task asks again by itself; restore the rights, then restart
the application so that its tasks initialise and ask again.

### The retention clock

A held message is not what retention threatens: it is in the ordering changelog, and
retention discarding its copy on the topic changes nothing. What retention runs against is
the task's committed read position on each received partition. When the log start passes
that position the next fetch refuses with
[`POSITIONS_DISCARDED_UNREAD`](#positions_discarded_unread), and the positions between are
gone. The clock matters for a process that is stopped, or one that lags; a process that
keeps up stays ahead of it. The status does not show how close it is. To see it:

1. `kafka-consumer-groups --describe --group <app-id>` gives the group's committed offset on
   the partition. After a stop long enough for the group's offsets to expire, the read
   position is the ordering state's covered position plus one, which `OrderingStateInspector`
   reads from the changelog.
2. `kafka-get-offsets --topic <topic> --partitions <p> --time earliest` gives the log
   start. The distance between the two, in positions, is the headroom.
3. `kafka-console-consumer --topic <topic> --partition <p> --offset <committed>
   --max-messages 1 --property print.timestamp=true` gives the timestamp of the record at
   the committed offset; with the topic's `retention.ms`, that is the time left.

Raising `retention.ms` on the topic is the one action that buys time without touching
order:

```
kafka-configs --bootstrap-server <servers> --alter --entity-type topics \
    --entity-name <topic> --add-config retention.ms=<longer>
```

Do it before the log start reaches the committed offset. Once it has, nothing recovers the
positions between, and the process refuses at its next fetch.

### Do not

- Restart expecting the hold to clear. A restart rebuilds the same hold from the changelog.
  It is harmless, and it changes nothing.
- Move the group's offsets on the blocker's channel to "unblock" it. The group must be
  inactive for the tool to act, and the next start adopts the moved offset as the start
  position. Moved forward, the positions it jumped are covered as never arriving: the hold
  releases, and the messages between are never delivered. Moved backwards, the records fed
  again below the session floor are dropped as replays, and the hold is exactly where it
  was.
- Delete and recreate a topic to clear it. That is a new channel, and the process stops
  with `CHANNEL_IDENTITY_CHANGED`.

## Resetting a process

A reset discards the process's causal past: its ordering state, its committed read
positions, and its application state. From the reset onwards the process is a new causal
lifetime. Messages it receives after the reset may be delivered before causes delivered
before it, and messages it sends express nothing about anything delivered before it. That
boundary is what the refusals asking for a reset protect, and a reset is the operator
accepting it on purpose.

### Before

- **Confirm the runbook asks for it.** Only the refusals under
  [the causal past is gone](#the-causal-past-is-gone), the failed alternatives under
  [`TASK_WIDTH_CHANGED`](#task_width_changed) and
  [`CHANNEL_REMOVED_WITH_HELD_MESSAGES`](#channel_removed_with_held_messages), and the
  deliberate step past an [undecodable header](#undecodable_metadata) call for a reset.
  Anywhere else it destroys state for nothing.
- **Stop every instance.** `kafka-consumer-groups --describe --group <app-id> --members`
  must list no members. A live instance commits offsets and writes the changelog
  underneath the reset, and the next start meets a different refusal.
- **Record the boundary.** The last logged status, the `failureDetail`, the group's
  committed offsets from `kafka-consumer-groups --describe`, and each received partition's
  log start and end from `kafka-get-offsets`. Together they say which positions the reset
  re-reads or skips, which is the record the application's own reconciliation needs.
- **Choose each channel's initial position.** `EARLIEST` re-reads everything retained on the
  channel; deliveries happen again, and the effects they produce are sent again as new
  messages, which every downstream process receives as new. `LATEST` skips everything
  retained, and nothing between the old position and the end is ever delivered. There is
  no position in between ([what the library does not do](#what-the-library-does-not-do)).
- **Decide about application state.** Under exactly-once semantics the declared stores
  committed together with the ordering state, so re-reading from `EARLIEST` against kept
  stores applies every delivery a second time. The procedure below deletes them. Keeping a
  store's changelog while starting `LATEST` on every channel leaves it as of the last
  committed step, which is consistent; that is a choice to make deliberately and record,
  not a default.

### Steps

With `<prefix>-<process>` as the application id and `inventory` as a declared store:

```
kafka-consumer-groups --bootstrap-server <servers> --delete --group <prefix>-<process>
kafka-topics --bootstrap-server <servers> --delete --topic <prefix>-<process>-__parsley.ordering-changelog
kafka-topics --bootstrap-server <servers> --delete --topic <prefix>-<process>-inventory-changelog
rm -rf <state.dir>/<prefix>-<process>        # on every instance
```

Then start again. The bootstrap resolves topics, pre-commits each channel's declared initial
position, and creates fresh changelogs. The transactional ids Kafka Streams derives need no
action; a new producer under the same id fences the old.

Doing it in another order produces a different refusal rather than a reset: offsets deleted
with the changelog kept resumes at the covered position plus one and refuses at the first
fetch with `POSITIONS_DISCARDED_UNREAD` if retention has moved past it; the changelog deleted
with offsets kept refuses with `ORDERING_STATE_LOST`. The Kafka Streams application reset tool deletes internal
topics and resets offsets but leaves local state; use the steps above.

### After

- The log carries `committed initial positions for [...]` for every received partition.
- `status()` reports `RUNNING`, and each task's entry appears within a status interval.
- Under `EARLIEST`, `heldMessages` rises and falls as the retained log is re-read; under
  `LATEST` it stays near zero.
- Downstream processes that were holding on this process's output clear as it sends. What
  they receive after the reset carries a frontier that names only positions this lifetime
  has seen.

## What the library does not do

Limits an operator should know before an incident, each the boundary of a runbook above.

- **Start from a chosen position.** A channel starts at `EARLIEST` or `LATEST`, and the
  bootstrap refuses committed offsets it did not write. There is no way to resume a process
  at a position an operator picks, and so no way past one bad record short of skipping
  everything retained.
- **Skip a message.** No refusal, no failure and no held message can be stepped over. The way
  past an undeliverable payload is a serde and handler that accept it; the way past an
  undecodable header is a reset.
- **Reset from the API.** The reset is four commands across two tools and every instance's
  filesystem, and the order matters. Nothing in the library performs it or checks it.
- **Report a hold's age or its retention headroom.** The status carries the head's position,
  not its timestamp, and not the channel's committed offset, log start or end; the retention
  clock and the lagging-or-not-produced distinction are computed from the Kafka tools.
- **Push anything.** The status is pull-only: no metrics, no listener, no callback. A stop
  is observed through `awaitStopped()` or by polling `healthy()`.
- **Separate an application failure from a substrate transient.** Both stop the process
  with no `refusalReason`; only `failureDetail` tells them apart.
- **Say what a stopped process holds.** Task detail leaves the status when the task closes.
  The changelog carries it, and `OrderingStateInspector` reads it, but reading the changelog
  is the operator's work.
