# The gossip module

`ParsleyGossip` is the top protocol module (see the [internals overview](overview.md)): clock
dissemination over the topology's own channels, in the epidemic sense of Demers et al. 1987 —
each node relays causal progress onward only while it is news, so knowledge spreads across every
path, cycles included, and quiesces when everyone has converged. Its records are *null messages*
in the Chandy–Misra–Bryant sense: a timestamp-carrying record whose value is literally null,
occupying a real offset on its channel purely to make the sender's clock observable downstream.
The module box:

```
requests:    receive(channel, offset, carried)   the null message's own offset is delivered via
               → (deliveries,                    the channels and causal-broadcast modules (it
                  learnedSomethingNew)           advances the contiguous frontier — the only thing
                                                 that can release held records); its carried clock
                                                 feeds the channel's advertised view and the stamp
                                                 only, never the gate
indications: advertise(key, timestamp)           a stamped, ready-to-forward null message, emitted
               → null message                    when a delivery produced no business output (or a
                                                 received null message carried news)
relay rule:  relay iff the carried clock is not  "new" is judged against the node's total
             dominated by the node's total       knowledge (the stamp: frontier ∪ channel clocks ∪
             knowledge                           carried ancestry ∪ ownOutputs ∪ highestDelivered)
properties:  relay on strict advance; liveness of completeness propagation
```

The module sits on top of the [causal-broadcast module](causal-broadcast.md) as a liveness layer —
a protocol extension, not part of the CBCAST core. Nothing here ever releases a record the
delivery gate would hold (a peer's carried claim is not local delivery), and removing the layer
entirely would cost only progress visibility on non-emitting paths, never ordering.

## Why the layer exists

The outbound stamp advances only as the node delivers records or hears its channels advertise
progress, so a node must keep advertising even when it produces no business output. Without that,
a filter that drops a record, a not-yet-emitting aggregate, or a held record would silently stall
downstream completeness: consumers further down would never learn that progress happened above
them. The gossip layer keeps causal progress observable on every channel, converging on any
topology shape including cycles.

## Emission

The emission half lives at the call sites in `ParsleyProcessor` (the transport —
`context.forward()` plus `ParsleyMarkerPartition` routing — is Kafka Streams glue, exactly as the
causal-broadcast module's underlying send is Kafka's produce). Three situations emit a null
message via `advertise`:

- a delivered record for which the delegate forwarded no business record;
- a received record that was buffered (nothing delivered) but whose receipt still advanced
  completeness, so the progress must propagate without flooding no-op messages;
- a received null message that carried news (the relay rule below).

`advertise` builds a record with a null value, the `_parsley_null_message` header, and the
triggering record's key, stamped by the single stamping site
(`ParsleyCausalBroadcast.broadcast`), so a null message's clock and a business record's clock
cannot diverge by construction. `ParsleyMarkerPartition` routes it to the forwarding task's own
owned partition on every sink regardless of key — any partition would be *safe* (carried clocks
never gate), but own-partition routing gives deterministic per-partition coverage.

Its timestamp is the *triggering record's* timestamp — the delivered record's on the non-emitting
path, the buffered record's on the heartbeat path, the received null message's own on the relay
path — never the wall clock. A record's timestamp carries no causal meaning (only the headers do),
but Kafka Streams advances downstream stream time from every polled record's timestamp before the
record is classified, so a wall-clock-stamped null message emitted during a reprocessing run over
historic event times would yank downstream delegates' windows, grace periods, and suppressions to
now. Under trigger timestamps, downstream stream time advances only as the data's time does. The
retention consequence: a sink segment holding only null messages looks old to broker time-based
retention exactly when its triggers are old — a backfill — and during a backfill the business
outputs on the same sink carry the same old timestamps, so retention on causal topics must already
cover the backfill depth. That is
[E2's retention-sizing constraint](causal-consistency.md#environmental-assumptions),
restated, not a new one — and an undersized retention fails in the safe direction: expired null
messages below a lagging consumer's position hit `AutoOffsetReset.none()`'s loud stall rather than
silently corrupting downstream event-time results.

## Reception: the dual role of a null message

`receive` does two independent things, and the distinction is the crux of correctness here:

1. **Its own offset is genuinely delivered** on its source channel — seeded and bridged like any
   record, then delivered into the contiguous frontier, cascading releases. A null message
   occupies a real offset on its partition, so the frontier's gap-free walk must count it or it
   would stall below the message forever, stranding every later record on that channel.
2. **Its carried clock feeds the stamp only.** The clock max-merges into the channel's advertised
   view — the whole clock, never stripped — and from there into the node's outbound stamp. It
   never feeds the delivery gate: a peer's claim that a coordinate was delivered *there* is not
   proof it was delivered *here*, and gating on it would let an effect reach the delegate before
   its locally subscribed cause.

Decoding the carried clock happens upstream of `receive`, and it fails fast exactly like the
business path: a present but undecodable clock header fails the task, and a null message arriving
on a topic the node has not registered fails it too. The carried clock is the emitting node's
stamp, so treating an undecodable one as empty would permanently drop that peer's progress claims
from this node's channel fold — a later stamp here would under-claim them, and a downstream
consumer could deliver an effect before its cause. Failing the task instead leaves the offset
uncommitted, so the message is refetched and retried on restart; a successful retry delivers the
offset normally. The historical frontier-gap bug — a corrupt message permanently stalling its
channel — was a skip-and-commit, not a fail-and-retry, and does not return under this rule. Only
an *absent* header reads as an empty clock whose offset is still delivered: a producer that stamps
nothing claims nothing.

## The relay rule

A received null message is relayed onward only when its carried clock taught this node something
outside its *total knowledge* — the comparison is against the full stamp, taken before the carried
clock is folded (afterwards it is dominated by construction), with pending producer
acknowledgements folded first so the own-outputs side of the comparison is current.

"New" is never judged against a single channel's clock. A reflected own coordinate — a downstream
stamp echoing this node's own produced position around a cycle — is dominated by the own-outputs
clock and so teaches nothing; the relay settles without any special-casing of own-sink topics. A
null message's own delivery is never itself a reason to relay: only genuinely new knowledge is,
so each relay strictly shrinks the set of unknown facts and any cycle quiesces. This is the
convergence argument for emulating broadcast on a graph with cycles.

## Consuming null messages outside Parsley

Parsley's own processors handle null messages internally. A plain Kafka client consuming a topic
a Parsley topology produces folds them into its running frontier with `CausalClock.observe` while
skipping them as business records, detected with `CausalClock.isNullMessage` — see
[Concepts](../concepts.md#the-frontier).
