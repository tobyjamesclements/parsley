# Model

## Terms

Every term of art in this repository names the following.

**Happened-before.** Lamport's irreflexive partial order over events: same-process
precedence, plus send-to-receipt edges, closed transitively (Lamport, *Time, Clocks, and the
Ordering of Events in a Distributed System*, CACM 1978).

**Cause, causal delivery.** A causes B when a delivery of A happened before B was sent.
Causal delivery means no process that delivers both delivers them inverted. This is the
delivery discipline of causal broadcast (Birman and Joseph, *Reliable Communication in the
Presence of Failures*, TOCS 1987), transplanted onto channels.

**Causal frontier.** A map from channel to the highest position known causal. It plays the
role a vector clock plays in process-indexed causal broadcast (Fidge 1988; Mattern 1989),
indexed by channel because the metadata carries no process identity.

**Hold-back buffer.** The causal-broadcast queue of received but not yet deliverable
messages, here per channel and persistent.

**Exactly-once, EOS.** Kafka's transactional processing, `exactly_once_v2` with
`read_committed`: a step's state changes, sends and consumed read positions commit atomically
or not at all.

**LSO, last stable offset.** Kafka's barrier for `read_committed` readers. It is the first
offset of the earliest still-open transaction on a partition, and the high watermark where
none is open. Everything below it is settled as committed data, or as aborted and control
positions that will never yield a message.

**Fail closed.** Stop delivering rather than weaken the guarantee.

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

Encoded size depends on the frontier's shape: a topic's first partition costs 26 bytes and
each further partition of the same topic 9 (a byte more per field once partition ids or
counts pass 127), so the encoding reaches Kafka's default 1 MiB record ceiling between
roughly 40,000 entries when every topic contributes one partition and 116,000 when few
topics contribute many. Reaching it inside the producer would stop the process with no
diagnosis from Parsley, so a metadata budget is applied first:
`ParsleyConfig.metadataBudgetBytes`, 256 KiB by default. Exceeding it stops the process with
an attributable reason. Frontier size and encoded width are logged each facts round, and
again at 80% of budget.
