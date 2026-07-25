# A two-channel, one-processor, one-sink topology

The diagram in this directory traces one Parsley task through a complete example. The topology is
the smallest one that exercises all three protocol layers: a single causal stage whose processor
`p1` consumes two source topics `c1` and `c2` and forwards to one sink topic `c3`.

```
c1-0 ─┐
      ├─> p1 (task 0_0) ─> c3-0
c2-0 ─┘
```

The diagram source is [`two-channel-topology.mmd`](two-channel-topology.mmd), kept as a standalone
mermaid file rather than a fenced block so a renderer can watch it directly. See
[Rendering](#rendering) below.

Sources must be co-partitioned, so this task owns partition 0 of both inputs and produces to
partition 0 of the sink. The three layers are the package-private modules described in
[the protocols overview](../../docs/protocols/index.md): `ParsleyChannels` (L1) adapts Kafka
topic-partitions into reliable FIFO channels, `ParsleyCausalBroadcast` (L2) runs the
Birman-Schiper-Stephenson delivery gate over them, and `ParsleyGossip` (L3) keeps causal progress
observable through inputs that produce no business output.

## Reading the arrows

Every arrow in the diagram is a call, and every call returns. That holds for the ones that touch
Kafka as well: `producer.send()` returns as soon as the record is queued, and a record arriving from
a source channel is the task thread invoking `process()` with something `poll()` has already
returned. Nothing there is a fire-and-forget message.

- A **solid arrow with an activation bar** is a call. The bar spans the callee's work, including any
  nested calls it makes.
- A **dashed arrow** is the matching return. A method that returns nothing is labelled `void`, so
  the point at which control comes back is still visible.
- A **self-directed arrow** is a call a participant makes on itself, including the work it does
  inside its own state stores. These return too.

The asynchrony in this design is end to end, not per call. A send returns without the record's
offset, because the broker is what assigns it. The node learns its own coordinate later, when the
producer's network thread hands the acknowledgement to `ParsleyOwnOutputInterceptor`, long after the
`send()` call returned on the task thread. Those deferred acks are drawn as their own arrows into
`ParsleyOwnOutputRegistry`, landing between the phases rather than beneath the send that caused
them, because that is when they actually arrive. They are the entire reason
`foldAcknowledgedOutputs()` and the crossing wait exist, and the reason an outbound stamp can never
carry the coordinate of the record it is stamping.

## The scenario

The task starts with a contiguous frontier of `c1-0 @6` and `c2-0 @9`, restored from its state
store. Four things then happen in order, and each one demonstrates a different part of the stack.

1. **Init.** The stack is built bottom up, L1 first, and each layer takes the one below it as a
   collaborator.
2. **A held record.** `m1` arrives on `c2-0 @11` depending on `c1-0 @7`, an offset this node has not
   delivered. L1 bridges the transaction commit marker at `c2-0 @10`, L2's gate holds `m1` in the
   buffer, and L3 emits a heartbeat null message because L1's bridge advanced completeness with no
   business output to carry it.
3. **The cause, and the cascade.** `m2` arrives on `c1-0 @7`. Its dependency on `c4-0 @31` names a
   topic this node does not consume, so the gate ignores it. `m2` is delivered immediately, its
   delivery advances `c1-0` to `@7`, and the release cascade frees `m1` in the same pass. The
   delegate forwards a business record for `m2` and nothing for `m1`, so `m1` produces a null
   message instead.
4. **An inbound null message.** A null message arrives on `c1-0 @8` carrying claims about `c2-0`,
   which this node consumes, and about `c6-0`, which it does not. The consumed-channel claim is
   ahead of this node's knowledge, so the I6 relay rule fires. The other claim folds into the
   outbound stamp without obliging a relay.

Between phases 2 and 3 the broker acknowledges the null message phase 2 sent, which is what lets
phase 3's stamp name `c3-0 @42`. The stamp taken in phase 2 could not have.

## What each layer is responsible for

`ParsleyChannels` never decides delivery. It records what arrived, repairs the offset density Kafka
does not provide, owns the contiguous frontier, tracks acknowledged own outputs, and persists all of
it as one value. Every call into it is either a question about channel state or a report that L2 has
made a decision.

`ParsleyCausalBroadcast` owns the gate, the hold-back buffer, the cascade, and the single stamping
site. It reads the frontier from L1 but never the advertised channel clocks, which is what keeps a
peer's claim from releasing a record before local delivery of its cause. Every outbound record,
business or protocol, takes its clock from the one `broadcast` call, so the two kinds of stamp
cannot diverge.

`ParsleyGossip` adds nothing to the gate. It delivers a null message's own offset through L2's
cascade, folds the carried clock into L1 for the outbound stamp only, and decides whether to relay.
Steps 2 and 3 show it standing in for a business output that did not happen, and step 4 shows it
carrying a peer's progress onward.

`ParsleyOwnOutputRegistry` is not a protocol layer. It is the seam through which the node learns
what the broker did with its own sends, and it is the only participant written from a thread other
than the task thread.

## Rendering

A standalone `.mmd` file does not preview inline on GitHub the way a fenced block does. That is the
deliberate trade: the file is watchable by a renderer, so it can be reviewed live while it is being
edited rather than by copying it out.

To watch it live, serve this directory with any static server and open a page that polls the file
and re-renders on change. To produce a one-off image instead:

```
npx -y @mermaid-js/mermaid-cli -i two-channel-topology.mmd -o two-channel-topology.svg
```

A successful render is only half a check on the call and return pairing. Mermaid rejects a return
with no matching call, failing with `Trying to inactivate an inactive participant`, but it tolerates
a call whose activation is never closed and simply draws the bar to the end of the diagram. If you
edit the diagram, count the two directions: every `->>+` needs a later `-->>-` on the same
participant, and the running depth of each participant must end at zero.
