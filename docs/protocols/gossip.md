# The gossip module

`ParsleyGossip` is the top protocol module (see the [protocols overview](index.md)): clock
dissemination over the topology's own channels, in the epidemic sense of Demers et al. 1987[^demers] —
each node relays causal progress onward only while it is news, so knowledge spreads across every
path, cycles included, and quiesces when everyone has converged. Its records are *null messages*
in the Chandy–Misra–Bryant sense:[^cmb] a timestamp-carrying record whose value is literally null,
occupying a real offset on its channel purely to make the sender's clock observable downstream.
The module box:

```
requests:    receive(channel, offset, carried)   the null message's own offset is delivered via
               → (deliveries,                    the channels and causal-broadcast modules (it
                  advancedConsumedChannel)       advances the contiguous frontier — the only thing
                                                 that can release held records); its carried clock
                                                 feeds the channel's advertised view and the stamp
                                                 only, never the gate
indications: advertise(key, timestamp)           a stamped, ready-to-forward null message, emitted
               → null message                    when a delivery produced no business output (or a
                                                 received null message advanced a consumed channel)
relay rule:  relay iff the carried clock,        the comparison is against the node's total
             restricted to channels this node    knowledge (the stamp: frontier ∪ channel clocks ∪
             consumes, is not dominated by the   carried ancestry ∪ ownOutputs ∪ highestDelivered);
             node's total knowledge              custody folds but never obliges a relay
properties:  relay on consumed-channel advance; liveness of completeness propagation
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
[E2's retention-sizing constraint](../foundations/assumptions.md),
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

A received null message is relayed onward only when its carried clock advanced this node's *total
knowledge on a channel this node consumes* — the carried clock restricted to the node's declared
input topics at its own task partition, compared against the full stamp, taken before the carried
clock is folded (afterwards it is dominated by construction), with pending producer
acknowledgements folded first so the own-outputs side of the comparison is current.

The restriction to consumed channels is what lets every topology cycle quiesce, and it is the
discipline of the Chandy–Misra–Bryant protocol this module's null messages come from: there, a
null-message send is obliged only by an advance of the process's own input channel clocks, never
by third-party knowledge of distant links. Everything else in a carried clock is *custody* — a
claim about a channel this node neither consumes nor produces, a sibling producer's appends on a
shared sink, a foreign partition of a consumed topic. Custody still folds into the stamp
unconditionally and rides every later emission (stamps must carry custody; that chain is what
keeps unconsumed ancestry claimable downstream), but it never obliges a relay, because custody
coverage is hearsay that structurally lags its source: on a cycle of three or more nodes it is
always one gossip lap stale, so relaying on it appends the very offset that is news to the next
blind member — an idle deployment then emits null messages forever. Consumed channels have no
such lag: the contiguous frontier physically catches up to every appended offset, so a fact
there can oblige at most one relay before it is covered for good.

Withholding a relay starves nobody. The delivery gate waits only on the local frontier of
consumed channels; every stamped claim sits at or below a really-appended offset, which arrives
on its channel regardless of gossip; and a suppressed relay also suppresses the claim it would
have stamped, so nothing downstream can reference — let alone wait on — the knowledge it
withheld. Folded-but-unrelayed custody reaches downstream on the node's next emission of any
kind: in an active topology, the next business delivery flushes it; in an idle one it waits, and
an idle topology has no emission whose stamp could need it.

"New" is never judged against a single channel's clock. A reflected own coordinate — a downstream
stamp echoing this node's own produced position around a self-cycle, where the sink is also a
consumed channel and therefore inside the trigger scope — is dominated by the own-outputs
clock and so teaches nothing; the relay settles without any special-casing of own-sink topics. A
null message's own delivery is never itself a reason to relay.

The resulting quiescence guarantee is unconditional on a pure cycle (each member consumes exactly
one cycle channel: a claim about that channel arriving on the channel itself always trails the
frontier, by partition FIFO). On a chorded cycle — a member consuming two or more cycle channels —
a claim can outrun its record through a multi-hop path and oblige one extra relay, and each such
relay mints one new offset; under any fair scheduler the race class is bounded and every lap dies
the moment the raced record is delivered, so sustaining relays forever would require a consumed
partition starved by at least a full gossip lap indefinitely — an operational pathology
(broker or network degradation), not a protocol state, and one that self-quenches at the first
successful fetch.

This quiescence guarantee is about the relay itself. The null messages this layer emits stop once
knowledge has converged, and that is independent of the topology's business traffic. A topology
whose delegates forward business records back onto their own upstream cycle with a loop gain above
one amplifies without bound from any finite input, the way a microphone pointed at its own speaker
does, and no delivery-order layer can damp that without dropping records. Parsley delivers every
such record in causal order. Keeping the loop gain below one is a property of how the topology is
configured, which this layer neither creates nor prevents, and it is distinct from relay
quiescence. The relay can be perfectly quiet while the business feedback is unbounded, and the two
are told apart by whether the growing traffic is null messages or business records.

## Consuming null messages outside Parsley

Parsley's own processors handle null messages internally. A plain Kafka client consuming a topic
a Parsley topology produces folds them into its running frontier with `CausalClock.observe` while
skipping them as business records, detected with `CausalClock.isNullMessage` — see
[Getting started](../guide/getting-started.md#consuming-a-causal-topologys-output).

## Cost

This module's cost is record volume rather than computation. See the
[consolidated cost model](index.md#cost-model) for how the rows fit together.

An input record that yields no business forward emits one protocol null message to every declared
sink, so a filter-heavy or aggregating stage multiplies its sink traffic by up to its sink count
during quiet stretches. A received null message that advanced the node's knowledge of a channel it
consumes is relayed onward under the same rule, and the restriction to consumed-channel advances is
what bounds relays and lets any topology, cycles included, go quiet once knowledge has converged.

The per-message processing cost is small: a clock fold and the relay decision, both O(w), plus the
channel-state persist the [channels module](channels.md#cost) charges. But the messages are real
records on your sink topics. They occupy broker throughput and retention, and every downstream
consumer receives them. Downstream causal processors absorb them internally, and plain Kafka
consumers skip them with `CausalClock.isNullMessage`.

[^demers]: Alan Demers et al., "Epidemic Algorithms for Replicated Database Maintenance", 1987. See
    the [bibliography](../reference/bibliography.md).
[^cmb]: K. M. Chandy and J. Misra, "Distributed Simulation: A Case Study in Design and Verification
    of Distributed Programs", 1979; R. E. Bryant, "Simulation of Packet Communication Architecture
    Computer Systems", 1977. The null-message protocol for distributed simulation. See the
    [bibliography](../reference/bibliography.md).
