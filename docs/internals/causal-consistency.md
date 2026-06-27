# Causal consistency model

This page explains what causal consistency means in Parsley's context, why traditional causal
consistency algorithms do not translate directly to Kafka stream processing, and how Parsley's
algorithm is designed around the constraints that Kafka imposes.

## Lamport's happened-before relation

Leslie Lamport's 1978 paper "Time, Clocks, and the Ordering of Events in a Distributed System"
defines a partial order on events called the happened-before relation, written A → B. It holds
when:

- A and B occur in the same process, and A occurs before B, or
- A is the sending of a message and B is its receipt, or
- there exists a C such that A → C and C → B (transitivity).

Causal consistency, applied to a storage or streaming system, says: if a write W1 happened before
a write W2, then any process that observes W2 must have previously observed W1. The transitive
case is the important one in practice — a consumer need not have subscribed to every intermediate
step to be bound by the ordering that runs through them.

Vector clocks implement the happened-before relation for distributed systems. Each process
maintains a clock: a map from process identifiers to the highest event number it has delivered
from that process. When a process delivers an event, it advances its own entry. When it sends a
message, it attaches a snapshot of its clock. A receiving process holds the message until its
local clock dominates the attached snapshot — until it has delivered everything the sender had
observed.

## How traditional algorithms work

Systems like COPS (Consistency, Availability, and Partition-tolerance of Scalable reads) and
causal broadcast protocols build on vector clocks under one shared assumption: every node that
must enforce causal ordering receives every write.

In COPS, writes propagate to all datacenters. When a datacenter receives a write carrying
dependencies, it can check each one because it already holds, or will receive, the writes those
dependencies name. The delivery predicate is always checkable: the coordinates in the dependency
clock are coordinates this datacenter will eventually observe.

In causal broadcast protocols the same assumption holds in a different form. Every process in the
group receives every message. The delivery predicate (local clock dominates the message's attached
clock) always converges because every coordinate in the attached clock is a message that will
eventually arrive.

This is the total visibility assumption: a node enforcing causal order has, or will have,
visibility into every event the dependency clock references.

## Why Kafka stream processing is different

A Kafka stream processor subscribes to a fixed set of input topics. It receives messages on
those topics only. It has no visibility into messages on any other topic.

A producer in a Kafka pipeline stamps the causal dependencies header with its own frontier — the
highest offset it had observed on each topic and partition it consumed. That frontier can, and
routinely does, span topics a downstream consumer never reads. A processor subscribing to topics T1
and T3 will receive records whose dependency clocks include coordinates for T2, even if T2 is an
intermediate topic that this processor has no connection to.

Under the total visibility assumption this coordinate is not a problem: the processor will
eventually receive the messages that advance it. Under Kafka's subscription model it is
unanswerable: the processor will never receive those messages, so it could never satisfy the
dependency through normal delivery. Holding the record until T2's coordinate is met means holding
it forever.

The total visibility assumption does not hold in Kafka stream processing. Algorithms that depend
on it cannot be applied directly.

## Parsley's algorithm

Parsley's algorithm is built around a different principle:

> A node enforces the causal order pertinent to the state it holds, and propagates causal
> metadata so that other nodes can do the same.

The two halves are handled separately.

### Enforcing causal order for state you hold

A processor owns the state associated with the input topics and partitions it is assigned. Its
delivery predicate is scoped to exactly those coordinates. A dependency on a coordinate the
processor does not own — a topic it does not subscribe to, or a partition belonging to a different
task — is vacuously satisfied. The processor cannot advance that coordinate, so it does not wait
on it.

This is implemented in `ParsleyEngine.effectiveDependencies()`. Before the delivery predicate is
evaluated, the full dependency clock is filtered to `inScope`: only coordinates on the processor's
registered input topics, on the partition the task owns, remain. The filtered clock is what gates
admission. Coordinates removed by this filter are not dropped from the stamp; they are dropped
only from the gating decision.

Scoping the delivery predicate this way reflects what it means to hold state. A processor running
a join against T1 and T2 has taken a consistency obligation over T1 and T2. A record it
forwards carries that obligation. It has no obligation over T3 because it holds no T3 state.
Waiting on T3 coordinates would impose a consistency obligation the processor is not equipped to
meet.

### Propagating causal metadata for downstream nodes

Scoping the delivery predicate solves the liveness problem — records are not held on
unanswerable dependencies — but it creates a correctness risk. Consider a pipeline:

```
T1 ---> Node A ---> T2 ---> Node B ---> T3
```

Node A subscribes to T1 and produces T2. Node B subscribes to T2 and produces T3. When Node B
forwards a T3 record, it stamps its own frontier, which covers T2. If a downstream Node C
subscribes to T1 and T3, it needs to know that T3 depends on T1. But if Node B stamps only its
T2 frontier, the T1 coordinate is lost. Node C cannot enforce the T1 → T3 ordering and admits T3
records before the corresponding T1 records are delivered — a Lamport violation.

The fix is to carry causal metadata through intermediate nodes. When Node B forwards a T3 record,
it stamps the full transitive ancestry: its own frontier merged with the dependency clock of the
T2 record it consumed. The T2 record's dependency clock includes T1 coordinates from Node A.
Node B did not act on those coordinates — they were out of scope for Node B — but it carries
them forward.

Node C receives a T3 record stamped with both T2 and T1 coordinates. T2 is out of Node C's scope
(vacuously satisfied). T1 is in scope. Node C's delivery predicate waits on T1's coordinate and
holds T3 until T1 is met.

This is implemented in `ParsleyProcessor.deliver()`:

```java
stampFrontier = snapshots.get(i).merge(message.dependencies());
```

The outgoing stamp is the node's own frontier snapshot (what this node enforced) merged with the
inbound record's original dependencies (what upstream nodes enforced and carried through). Each
node adds its own causal evidence and forwards everything it received, regardless of whether it
could act on it.

### The frontier is a contiguous watermark

One further difference from traditional vector clocks: Parsley's frontier for a given coordinate
is a contiguous watermark, not a running maximum. It represents the highest offset delivered
without a gap. A later record on a partition can be forwarded before an earlier record that is
still held on the same partition. The frontier only advances past the held record's position once
that record is itself delivered, whether by normal release or by eviction.

This matters because the delivery predicate checks `frontier >= required`. If the frontier jumped
over a gap, a downstream record waiting on an offset inside the gap would be released on
bookkeeping alone, without the record at that offset ever having been delivered. The contiguous
watermark ensures the predicate reflects actual delivery history, not observed-but-undelivered
offsets.

## Violations

Eviction is the only path that breaks Lamport's happened-before guarantee. Under the default
policy (`parsley.buffer.eviction.failure.policy = fail`), the engine throws rather than deliver
a record whose dependencies are not met, and the task restarts. No violation reaches downstream.

Under the `continue` policy, the record is delivered out of causal order. Its outgoing stamp
carries whatever frontier snapshot existed at eviction time, which may not dominate the record's
declared dependencies. Downstream nodes receiving that stamp may release records they would
otherwise have held. The violation is transitive. The `continue` policy is an explicit trade of
Lamport's guarantee for availability under a full buffer; it is logged, metered, and documented
as a deliberate choice.

The distinction between `fail` and `continue` maps directly onto the consistency-availability
trade-off described in the CAP theorem and in the causal consistency literature. The default
takes the consistency side.
