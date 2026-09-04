# Operations

What a running Parsley application looks like from the cluster, what must exist before it
starts, how it scales, what a reset discards, and how to size the two limits an operator
owns. What to do when a process stops or holds is in [Runbooks](runbooks.md).

## Names on the cluster

Every declared process becomes one Kafka Streams application with its own consumer group.
With `ParsleyConfig.builder(bootstrap, "shop")` and a process named `shipper`:

| What | Name |
|---|---|
| Application id and consumer group | `shop-shipper` |
| Ordering-state changelog | `shop-shipper-__parsley.ordering-changelog` |
| Changelog of a declared store `inventory` | `shop-shipper-inventory-changelog` |
| Transactional ids | derived by Kafka Streams from the application id |

The changelogs are created by Kafka Streams on first start, compacted. Every declared topic —
received and sent — must exist before `Parsley.start`; nothing is auto-created, and a missing
topic refuses the start.

## What Parsley asks the cluster

Beyond the Streams application's own consumer and producer, each start and each facts round
uses the admin client and two plain consumers: describing topics by name and by id, listing
the earliest offsets under `read_committed`, listing the group's committed offsets, and — at
start when initial positions are missing — joining the group as a short-lived bootstrap
member to commit them under the group's generation fence. A facts round also assigns a
groupless consumer to the partitions a held head waits on and polls a few times, a second at
most. The ACLs those need
are Describe on every declared topic and on the group, Read on the group and on the received
topics, and whatever Streams itself needs to create and write its changelogs. A Describe
denial is treated as denial, never as a topic's deletion.

## Scaling

Tasks are induced by the widest received topic: a process receiving a 12-partition topic
runs as 12 tasks, spread over every instance started with the same application-id prefix.
Adding instances rebalances tasks; each task's ordering state lives in its own changelog
partition and follows it. Several instances may cold-start together: the runtime waits for
another instance's bootstrap member to leave the group before joining, and a join that still
meets one replaces its stream thread and joins again.

Widening the widest received topic changes the task width, which the ordering changelog
cannot follow, so it refuses at start with `TASK_WIDTH_CHANGED`; widening a narrower topic is
re-resolved on restart. Plan partition counts with the widest topic in mind.

## Resetting a process

Several refusals end with "reset the process's state and group offsets deliberately". The
reset discards the process's causal past: messages received after it may deliver before
causes that were delivered before it, which is the boundary the refusal protects. The checks
to make before it, the steps in the order that matters, and what to verify afterwards are in
[Runbooks](runbooks.md#resetting-a-process).

## Sizing

**Metadata budget.** Each emission carries the causal frontier: about 26 bytes for a topic's
first partition and 9 for each further partition, over the transitive upstream closure of
the process's inputs, external producers that stamp causes included. Set
`metadataBudgetBytes` comfortably above that sum; the default is 256 KiB, `status()` reports
each task's current width, and a warning is logged once at 80%.

**Record limits.** A held message is persisted to the ordering changelog with its payload,
its application headers and its causes, so the changelog's `max.message.bytes` and the
producer's `max.request.size` must cover the largest received record plus the metadata
budget plus a few hundred bytes of framing. The default 1 MiB record limit and the default
budget are compatible only for records well under 750 KiB; a held message beyond the limit
stops the process with `SUBSTRATE_MISCONFIGURED` naming the limit to raise.

**Retention.** Retention must cover hold-back time, not only consumer lag: a message held
behind a lagging cause and then discarded by retention stops the holder with
`POSITIONS_DISCARDED_UNREAD`, because its senders may since have pruned it from the causes
they express ([Failing closed](failing-closed.md)).

## Reading the status

`Parsley.status()` reports each process's state, its refusal reason where it stopped
deliberately, and per task what is held and which cause each hold waits for, with the
frontier's size and how long ago broker facts were applied. A held message is not a failure:
the diagnosis is the named cause. A refusal recurs identically on restart, except
`COVERED_POSITION_FED` raised because the execution was superseded, which a restart recovers.
What to do about each shape the status can show — a refusal, a stop without one, a hold that
does not move — is in [Runbooks](runbooks.md).
