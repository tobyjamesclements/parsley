# Topology shapes

A Parsley topology is stages wired through topics, and the wiring rules are few. A topic may
be the sink of any number of stages but the source of at most one stage per application
(Kafka Streams allows one source node per topic). Every topic must exist before the
application starts, and there are no automatic repartition topics, so the co-partitioning
precondition stays visible at each hop. Beyond that, shapes compose freely, because every hop
is the same thing: a causal channel, stamped on write and gated on read.

This page catalogues the common shapes and states exactly what ordering each one gets.
Topics are named `c1..cN`, stages `p1..pN`, and messages `m1..mN`, with a message's number
giving its send order where that matters. The recurring theme: causal delivery orders a
record after its causes, and only after its causes. Where a shape needs an order between
records that are not causally related, the fix is always to make the relation causal — send
them from one stamping site, or consume the record you need to come first.

## One application or several

Stages compose into one application, or run as separate applications, and the guarantee does
not change: ordering is carried by the stamps on the wire, not by co-deployment.

```java
try (CausalStreams app = Parsley.named("pipeline-app", p1, p2).streams(props)) {
    app.start();
}
```

`Parsley.named("pipeline-app", p1, p2)` assembles both stages into one Streams topology,
under the application id that names every topic and store the application owns. Each stage
keeps its own state stores, keyed by its stage name. Splitting the same stages across two
applications changes deployment granularity and nothing else. A new application joins a
running topology by simply starting to consume: it self-gates into causal order during
replay, and its outputs are correctly gated everywhere from its first emission.

## Linear

```text
c1 --> p1 --> c2 --> p2 --> c3
```

The pipeline. Within one channel, per-partition FIFO already orders everything; what the
stamps add is ordering *across* the pipeline's topics. Every record p1 emits claims the
input that caused it, and claims carry transitively through p2, so any stage that consumes
several of the pipeline's topics — c1 and c3, say, for reconciliation — delivers effects
after their causes, however far apart the hops run.

```java
Stage p1 = Stage.named("p1")
        .on(c1, m -> List.of(c2.send(m.key(), transform(m.value()))))
        .into(c2)
        .build();

Stage p2 = Stage.named("p2")
        .on(c2, m -> List.of(c3.send(m.key(), summarise(m.value()))))
        .into(c3)
        .build();
```

Stamping is synchronous (sequence claims, see [the stamp](../design/architecture.md#the-stamp)),
so a hop costs one broker round trip and nothing waits on acknowledgements.

## Fan-out

One stage, several sinks:

```text
            +--> c2
c1 --> p1 --|
            +--> c3
```

```java
Stage p1 = Stage.named("p1")
        .on(c1, m -> List.of(
                c2.send(m.key(), summary(m.value())),
                c3.send(m.key(), audit(m.value()))))
        .into(c2, c3)
        .build();
```

The emissions of one delivery are applied in list order, leave in one transaction, and are
claimed in that order: if the handler above emits m2 to c2 and then m3 to c3, then m3 claims
m2, and any stage that consumes both c2 and c3 delivers m2 before m3. A stage's sends are
ordered across *all* its sinks this way, across deliveries too — downstream observers see
one stage-task's output stream in send order, whichever topics it is spread over.

The other fan-out is one topic with several consumers. Kafka Streams allows one source node
per topic, so two stages of the same application cannot both consume c1; run the second
consumer as its own application. Each consuming application gates independently and needs no
awareness of the others.

## Fan-in

```text
c1 --+
     |--> p3 --> c4
c2 --+
```

```java
Stage p3 = Stage.named("p3")
        .state(pairCodec, Pair::empty)
        .on(c1, (s, m) -> joined(s.withLeft(m.value()), m))
        .on(c2, (s, m) -> joined(s.withRight(m.value()), m))
        .into(c4)
        .build();
```

Here `joined` is the pure join step: it returns the updated pair as the next state, plus an
emission to c4 when both sides are present.

A multi-source stage is where the gate earns its keep: records causally related across c1
and c2 are delivered in causal order, so the fold's state never sees an effect before the
cause it consumed. Two records with no causal relation are delivered in arrival order —
the delivered history is one valid extension of the causal partial order, not a total order
anyone chose. If the outcome must not depend on which concurrent record lands first, make
the logic commutative for concurrent inputs; causal order fixes everything else.

Two expectations come with the shape. Related keys must land on the same partition number of
every source topic — in practice, the same key bytes and the same partition count — because
the guarantee is per task and a task consumes one partition slice of each source
([preconditions](../foundations/causal-model.md#preconditions)). And per-key fold state is
keyed by the message key's encoded bytes, so records from c1 and c2 that agree on key bytes
fold into the same state, which is exactly the agreement co-partitioning already requires.

## Diamond

```text
            +--> c2 --> p2 --> c4 --+
c1 --> p1 --|                       |--> p4 --> c6
            +--> c3 --> p3 --> c5 --+
```

Two branches derive from a common source and a join consumes both. Within each branch the
linear rules apply. Across branches, be precise about what is and is not ordered: the
records p2 and p3 derive from the same input are causally *concurrent* — each claims the
common input, neither claims the other — so p4 may receive either side first, and a join
holds its partial result in fold state until the other side arrives, exactly as in fan-in.
Wanting "the c4 record before the c5 record" at the join is not a causal property, and no
sound protocol can grant it without inventing an order.

What the diamond does guarantee is that no branch output outruns anything it *depends on*
that the join consumes. The sharpest version is the base-and-derived triangle:

```text
c1 --+--------------> p4
     |                ^
     +--> p2 --> c2 --+
```

p4 consumes both the base topic c1 and the derived topic c2. Every record p2 emits claims
the base record it was derived from, so p4 always delivers the base before the derivation —
whatever the branch's lag. This is the shape that removes the classic race where an
enrichment, an index entry, or a projection arrives ahead of the record it describes.

## Request and reply

```text
p1 --> c1 --> p2 --> c2 --> p1
```

p1 emits requests to c1; p2 consumes c1 and emits replies to c2; p1 consumes c2. A reply
claims the request it answers, so any stage consuming both c1 and c2 sees each request
before its reply. Correlation — which request a reply answers — rides in the key or payload
as usual; Parsley orders the flow and adds no envelope.

The property that makes the shape worth naming is effect-before-reply. Have the responder
emit its effects before its reply, in one handler:

```java
Stage p2 = Stage.named("p2")
        .on(c1, m -> List.of(
                c3.send(m.key(), effect(m.value())),
                c2.send(m.key(), reply(m.value()))))
        .into(c2, c3)
        .build();
```

The reply m3 claims the effect m2 (fan-out emission order), so a requester that consumes
both c3 and c2 delivers the effect before the reply: by the time p1 processes an
acknowledgement, everything it acknowledges is already visible locally. Read-your-writes
across services, expressed as a topology shape.

## Event flows

An event notification is a record announcing state that lives elsewhere, and its classic
failure is the dangling notification: the announcement arrives before the data it points
at. The shape that fixes it is the fan-out order again, this time between a data topic and
an event topic. Write the data m1 to c1, then the notification m2 to c2, from one stamping
site — two emissions of one delivery, or a plain producer that folds its acknowledgements
([plain clients](clients.md)). The notification claims the data record, so every stage
consuming both topics delivers the data first, and a notification handler can always read
what it was notified about.

Event topics also cross the adoption boundary well: a producer that stamps nothing claims
nothing, so its events deliver immediately and unordered, and stamping can be added one
producer at a time as ordering starts to matter.

## Cycles

Topics may wire stages into a cycle:

```text
c1 --> p1 --> c2 --> p2 --> c1
```

The protocol does not deadlock on cycles: every wait a gate can hold resolves strictly
backward in append time, so the wait graph is acyclic whatever shape the topology takes
([liveness](../foundations/liveness.md#why-over-claims-cannot-deadlock)). What a cycle does
demand is a termination condition in the logic itself — a fold that re-emits on every
delivery circulates forever, at one broker round trip per lap. Converge on a fixpoint,
count rounds in the state, or emit only on change.
