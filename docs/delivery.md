# Delivery

## Settled positions

Per received channel *c* the engine tracks two things.

`fedUpTo(c)` is the highest position such that every position at or below it has either been
fed to this process as a message or will never arrive as one. Receipt advances it while an
execution runs; at initialisation the start position raises it once, and a received channel
whose topic the identity check finds deleted is settled to its end (D21).

Receipt of a record at offset *o* on *c* sets `fedUpTo(c) := max(fedUpTo(c), o)`. Within an
execution the host feeds each partition in offset order, and Kafka's per-partition order
means every earlier offset was already fed or will never yield a record. Aborted
transactions, control records and compaction account for the positions that yield nothing.
A cause names the offset of a committed record on its channel (wire-format constraint 8), so
receiving that record is what satisfies it; the markers and aborted batches between records
are settled by the receipt of the next record on the channel.

At each execution start the host reports the process's start position *s* on *c* — the
position it will feed first, as the bootstrap established it at `Parsley.start` — giving
`fedUpTo(c) := max(fedUpTo(c), s - 1)`. Every position below the start position was fed to an
earlier execution and committed, or lies below the position the process was started at; a
start position of 0 covers nothing. Beyond receipt, the start position and a channel's
deletion, nothing advances `fedUpTo(c)` — in particular not elapsed time, and nothing asked
of the broker between deliveries (D115).

`held(c)` is the hold-back buffer: received but undelivered messages of *c*, in position
order, persisted.

Settled position is derived from these rather than stored:

```
settled(c) = held(c) is empty ? fedUpTo(c) : head(held(c)).position - 1
```

Every position at or below `settled(c)` is delivered or will never arrive. Positions below
the buffer head that were fed are delivered, since the head is the lowest undelivered one,
and gaps are covered by the fed-or-never meaning of `fedUpTo`.

## The decision

```
decide(causes, receivedChannels, settled)
```

Message M is deliverable when, for every pair `(c, p)` in M's causes, either *c* lies outside
`receivedChannels`, or `settled(c)` is defined and `p <= settled(c)`.

A cause on a channel the process does not receive imposes no constraint. The process will
never deliver that message, so no order it could observe can be inverted. This also covers
dead incarnations of recreated topics, whose topic identity matches no received channel.

The function reads no clock, no timestamps, and nothing outside its arguments. The same unit
decides every delivery, and it is callable without a host.

## Order within and across channels

Only the head of each channel's buffer is offered to the decision, which enforces per-channel
FIFO structurally.

Across channels, delivery is a deterministic drain: scan the received channels in a fixed
total order, deliver every deliverable head, and repeat until a full pass delivers nothing.
Determinism means replay after a restart reproduces the same order from the same state.

## Baselines and duplicates

Before anything is known of *c*, `settled(c)` is undefined, and any dependency on *c* holds
the message while *c* is received. A start position above 0 is what first defines it; at 0
the first receipt on *c* does. On a first start the start position is the declared initial
position, resolved against the channel — its earliest retained position or its end. On later
starts it is the group's committed position. Where prior state exists but the committed
position has expired, it is the covered position the ordering state holds for *c* — its
`fedUpTo(c)` — plus one, the next position the previous execution would have read; where the
ordering state covers nothing on *c*, a channel that has joined the received set, it is the
channel's earliest retained position (D115). `fedUpTo(c)` is also restored from the ordering
state at each start, and the start position only ever raises it, so positions below the
first receipt count as already satisfied.

A record at an offset at or below `fedUpTo(c)` that is not in the buffer was already
delivered in a committed step. The host re-feeds exactly those records whose read positions
were not committed, so such a record is dropped rather than redelivered. An aborted step
rolls `fedUpTo` back with everything else, so a redelivery after an abort is not a duplicate.
