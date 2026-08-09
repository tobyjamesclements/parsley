# Model

## Specification terms in Kafka

| Term | Kafka realisation |
|---|---|
| Channel | Topic-partition, identified by topic ID and partition |
| Position | Record offset |
| Message | Record: key bytes, value bytes, headers, offset |
| Process | One Kafka Streams task of the application built for a declared process |
| Step | One Kafka Streams transaction |
| Earliest retained position | Log-start offset |

A channel is identified by topic ID rather than topic name. A topic deleted and recreated
under the same name yields a different channel, so positions recorded against the old log
cannot be read as positions in the new one.

One declared process becomes one Kafka Streams application, with its own `application.id`
and therefore its own consumer group. This is what allows two declared processes to receive
the same channel, which a single Streams application cannot do, since it cannot register one
topic in two source nodes.

Each application holds one subtopology: byte-level sources for the received topics, one
processor node, byte-level sinks for the send topics. Streams induces one task per partition
group. That task is the specification's process, and its received-channel set is partition
*i* of each received topic.

## The causal frontier

A message's metadata expresses its causes as (channel, position) pairs. Each process keeps a
frontier: per channel, the highest position known to be a cause of that process's subsequent
sends.

One pair per channel suffices. Replacing two pairs on one channel by the greater position is
compression, since expressing a position at or above a cause's position expresses the cause.

The frontier grows from two events.

**Delivery.** Delivering message M from channel *c* at position *p* merges `(c, p)`. The
delivery happened before every later send, so M is a cause of them.

**Receipt.** Receiving M merges every pair in M's metadata, whether or not M is delivered.
Happened-before passes through receipt, so M's causes are already causes of subsequent sends.
A process that has seen a message but not yet delivered it is still constrained by what that
message carried.

Sends do not enter the frontier. Causality flows through delivery-before-send, so a process's
own send becomes a cause of its later sends only once some process delivers it and that
delivery flows back. One consequence is that a message sent to a channel its sender also
receives from carries only positions strictly below its own, because offsets are assigned in
append order.

## Pruning and growth

A pair `(c, p)` is dropped when *p* falls below *c*'s log-start offset, or when *c*'s topic
identity no longer resolves. Log-start facts may be stale, and a stale fact is a lower bound,
so staleness delays pruning without over-pruning.

Frontier size follows the causal graph rather than a process's own declaration. Receipt
merges every pair a message carried, including channels the receiving process never receives,
which is required for downstream re-expression. Steady-state size approaches the sum of
partition counts over the transitive upstream closure.

At 28 bytes per entry, the encoding reaches Kafka's default 1 MiB record ceiling near 37,000
entries. Reaching it inside the producer would stop the process with no diagnosis from
Parsley, so a metadata budget is applied first: `ParsleyConfig.metadataBudgetBytes`, 256 KiB
by default. Exceeding it stops the process with an attributable reason. Frontier size and
encoded width are logged each facts round, and again at 80% of budget.
