# Delivery

## Settled positions

Per received channel *c* the engine tracks two things.

`fedUpTo(c)` is the highest position such that every position at or below it has either been
fed to this process as a message or will never arrive as one. Two inputs advance it.

Receipt of a record at offset *o* on *c* sets `fedUpTo(c) := max(fedUpTo(c), o)`. Within an
execution the host feeds each partition in offset order, and Kafka's per-partition order
means every earlier offset was already fed or will never yield a record. Aborted
transactions, control records and compaction account for the positions that yield nothing.

The seed round at task initialisation contributes the group's committed offset *n*, giving
`fedUpTo(c) := max(fedUpTo(c), n - 1)`. A committed position asserts that everything below it
was fed or never will be. This is the baseline for a channel the process has received
nothing on: positions below where it began reading count as satisfied.

Nothing else advances `fedUpTo`, and nothing else needs to. A cause names the offset of a
record that some process delivered, so it is a real committed record; a receiver of that
channel is fed it, or the first surviving record above it, and that receipt settles the
cause. The aborted batches and control records between the receiver's last receipt and the
cause are skipped by the receipt itself, and those above the cause are irrelevant to it.

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
the message while *c* is received. The committed position the seed round reads initialises
`fedUpTo(c)`, so positions below the first receipt count as already satisfied.

A record at an offset at or below `fedUpTo(c)` that is not in the buffer was already
delivered in a committed step. The host re-feeds exactly those records whose read positions
were not committed, so such a record is dropped rather than redelivered. An aborted step
rolls `fedUpTo` back with everything else, so a redelivery after an abort is not a duplicate.
