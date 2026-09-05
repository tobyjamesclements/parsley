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

Beyond the Streams application's own consumer and producer, each start uses the admin client
and up to two plain consumers: describing the declared topics by name, reading the ordering
changelog for prior state where one exists, listing the group's committed offsets, listing
the earliest or latest offset under `read_committed` — only for received partitions with
neither a committed position nor a covered position in the ordering state — and, where
committed positions are missing, joining the group as a short-lived bootstrap member to
commit the start positions under the group's generation fence. Each task initialisation
describes by id, once, the topics the task's state names, and corroborates by name any id
the broker does not know and whose name it has learned — a deletion or recreation is
confirmed only by three consistent answers half a second apart; an id whose name was never
learned is left alive. Nothing is asked of the broker between deliveries, and no consumer
beyond the Streams application's own is assigned to the partitions a held head waits on:
receipt of the named record is what settles a cause (D115). The ACLs those need are
Describe on every declared topic and on the group, Read on the group and on the received
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

**Retention.** A held message survives retention: it is persisted to the ordering changelog
and delivers from there, in order, once its causes settle, so retention need not cover
hold-back time. It must cover the longest stop and the longest lag on any received
partition: a committed read position below the log start — or, where the committed offset
has expired, the ordering state's covered position plus one — refuses at the fetch with
`POSITIONS_DISCARDED_UNREAD` ([Failing closed](failing-closed.md)). Add whatever a reset
that re-reads from `EARLIEST` should still find.

## Reading the status

`Parsley.status()` reports each process's state, its refusal reason where it stopped
deliberately, and per task what is held and which cause each hold waits for, with the
frontier's size. A held message is not a failure: the diagnosis is the named cause. Every
refusal recurs identically on restart, except `COVERED_POSITION_FED`, which has no known
trigger and which a restart clears. What to do about each shape the status can show — a
refusal, a stop without one, a hold that does not move — is in [Runbooks](runbooks.md).
