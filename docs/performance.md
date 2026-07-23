# Performance

Parsley adds overhead to your Kafka processing in five distinct places. This page states how each
cost scales and which part of the implementation it comes from, so you can reason about capacity
and spot the levers that matter for your topology.

!!! note "Asserted, not measured"
    The complexities on this page are asserted from the structure of the implementation, and each
    claim names the class that carries it. Parsley does not ship a benchmark suite: absolute
    latency figures depend on hardware, storage class, and workload shape, and isolated
    micro-curves proved to be a poor proxy for the costs that actually dominate (state
    persistence, protocol record volume, and producer acknowledgement waits). To size a
    deployment, measure your own topology end to end and watch the metrics described in
    [Configuration](guide/configuration.md#metrics).

Throughout this page, `w` is the width of a clock (the number of `(topic, partition)` entries it
carries), `C` is the number of channels the node tracks, `n` is the number of records held in the
causal buffer, `k` and `r` are drain parameters defined below, and `N` is the number of records a
delegate forwards per input.

---

## 1. Per-record overhead

Every record a causal processor handles pays four costs.

**Header parse, O(w).** The dependency clock is deserialised from the incoming record's header by
`ParsleyVectorClock.fromBytes`, a single linear walk over the clock's entries.

**Gate evaluation, O(w).** The parsed dependencies are normalised against the channel state and
compared against the node's contiguous frontier (`ParsleyVectorClock.dominates`). Both are linear
in the clock's width.

**Outbound stamp, O(w).** Each forwarded record is stamped with the node's outbound clock,
serialised by `ParsleyVectorClock.toBytes`. Note that the outbound clock is wider than the
incoming one, as described under [clock width](#clock-width) below.

**State persistence, O(C · w).** This is usually the dominant per-record term. All of the node's
causal metadata (the frontier, the per-channel advertised clocks, carried ancestry, own-output
positions, and highest-received offsets) persists as one value in the frontier state store, and
`ParsleyChannels` rewrites that whole value on every advance: every delivered record, every
producer acknowledgement, and every gossip fold. Each rewrite serialises the full channel state
and issues one state-store put, so the cost scales with the total channel state, not with the
incoming header alone, and a store write costs far more than any of the clock walks above.

### Clock width

The outbound stamp is the union of the node's frontier, its per-channel advertised clocks, its
carried ancestry, and its own acknowledged outputs (`ParsleyChannels.stamp()`). Carried ancestry
never shrinks: a coordinate that entered the node's causal past stays claimed on every later
stamp. Clock width therefore grows with the number of distinct `(topic, partition)` coordinates
in the node's causal history, which for a processor in the middle of a topology approaches the
number of channels in its transitive upstream, not just the partitions the task itself consumes.

!!! tip "Controlling clock width at the edges"
    Inside a Streams topology the width is a consequence of the topology's shape and history and
    is not directly tunable. At the edges you control it: `CausalClock.using(props).observe(trigger)`
    carries only the trigger's coordinate, whereas merging a full upstream clock via
    `CausalClock.fromRecord` carries that record's entire ancestry. Prefer observing the specific
    records a downstream consumer must wait for.

---

## 2. Causal buffering latency: O(log n + k + r)

This cost is paid when a causally ready record arrives and Parsley releases held records from the
buffer. The drain in `ParsleyCausalBroadcast` has three independent components.

**Candidate lookup, O(log n).** Finding which held records were waiting for the arriving
coordinate is a RocksDB range scan over the candidate index (`StoreBackedCandidateIndex`), whose
keys order by coordinate and required offset. The logarithmic seek is a property of RocksDB's
storage structure rather than of Parsley's code, and it holds in the buffer depth `n`.

**Per-released record, O(k).** If `k` held records all depend on the arriving coordinate, one
drain pass releases all of them. Each release is one buffer read, one frontier advance (with its
state persist, see above), and one forward call, so the pass is linear in `k`. Heavy fan-in at a
single dependency offset is what drives `k` up.

**Cascade, O(r).** A released record can itself satisfy another held record's dependency, and the
release propagates through the chain. The cascade visits each newly satisfied record once, so it
is linear in the chain depth `r`. Only long runs of strictly sequential dependencies that become
ready simultaneously produce a deep cascade.

---

## 3. Recovery latency

When Parsley restarts, or a Kafka Streams task is reassigned, `ParsleyProcessor.init()` restores
state before processing begins.

**Channel-state restore, O(C · w).** One point read of the persisted channel-state value,
followed by a parse that is linear in the total channel state (`ParsleyChannels`). Independent of
buffer depth.

**Buffer restore, O(n).** Every held record is scanned once to rebuild the buffer's counters
(`StoreBackedBufferStore`) and the candidate index (`ParsleyCausalBroadcast`). A buffer twice as
deep takes roughly twice as long to restore.

**Broker round trips.** Init also resolves topic identities, reconciles the declared input and
sink sets against the restored state, and seeds sink end offsets. These are a fixed number of
admin and metadata calls per task, but each is a network round trip, so in wall-clock terms they
usually dominate a restart on any healthily sized buffer.

!!! tip "Buffer restore cost tracks buffer depth"
    The causal buffer is unbounded, so buffer restore cost is entirely a function of how much lag
    accumulates before a restart, not a configurable limit. Keep an eye on the `buffer-depth`
    gauge (see [Configuration](guide/configuration.md#metrics)) in production. A lagging or
    co-partitioning-broken topology drives up both restart cost and steady-state buffer footprint.

---

## 4. Crossing-wait produce serialization

Within one task invocation, a second forward is causally after the first even when the two go to
different sink topics or partitions, so before stamping each business forward the task waits for
every pending own-sink send to be acknowledged (the crossing wait, see the
[own outputs section](protocols/channels.md#own-outputs) of the channels internals). The waits
serialize a multi-forward invocation on producer acknowledgement latency: forward k+1 does not
stamp until forward k's batch is acknowledged, so a delegate forwarding N records per input pays
roughly

> **N × (linger + replication round trip)**

per invocation. Kafka Streams' default `producer.linger.ms` is 100 ms, so an unconfigured
multi-forward delegate can spend ~N × 100 ms per input record in crossing waits alone, which is
usually the dominant cost for such delegates, far above every category on this page.

A single-forward delegate pays at most one wait per invocation and is usually unaffected (the
previous invocation's send has normally been acknowledged by the time the next record arrives).
For multi-forward delegates, lower `producer.linger.ms` (a few milliseconds, or 0) so each batch
ships immediately; the replication round trip then bounds the wait. The cost cannot be exempted
per partition: a business forward's destination partition is unknowable at stamp time (the sink's
partitioner runs downstream of `forward()`), so the wait must conservatively cover every own-sink
coordinate. Protocol null messages are exempt for their own exact destinations and do not pay it.

---

## 5. Gossip record volume

The gossip layer (see [the gossip module](protocols/gossip.md)) keeps causal progress observable
through processors that produce no business output, and its cost is record volume rather than
computation. An input record that yields no business forward emits one protocol null message to
every declared sink, so a filter-heavy or aggregating stage multiplies its sink traffic by up to
its sink count during quiet stretches. A received null message that advanced the node's knowledge
of a channel it consumes is relayed onward under the same rule; the restriction to consumed-channel
advances is what bounds relays and lets any topology, cycles included, go quiet once knowledge has
converged.

The per-message processing cost is small (a clock fold and the relay decision, both O(w), plus
the state persist from category 1), but the messages are real records on your sink topics: they
occupy broker throughput and retention, and every downstream consumer receives them. Downstream
causal processors absorb them internally; plain Kafka consumers skip them with
`CausalClock.isNullMessage`.

---

## Summary

| Category | Complexity | Scales with |
|---|---|---|
| Header parse | O(w) | Incoming clock width |
| Gate evaluation | O(w) | Incoming clock width |
| Outbound stamp | O(w) | Outbound clock width (grows with causal history) |
| State persistence | O(C · w) | Total channel state, once per advance |
| Buffer drain (candidate lookup) | O(log n) | Records in buffer |
| Buffer drain (per-released record) | O(k) | Records sharing the trigger coordinate |
| Cascade propagation | O(r) | Chained release depth |
| Channel-state restore on restart | O(C · w) | Total channel state |
| Buffer restore on restart | O(n) | Records held at time of restart |
| Crossing-wait produce serialization | O(N) waits | Forwards per invocation × (linger + replication RTT) |
| Gossip volume | Up to one null message per silent input per sink | Sink count, share of inputs with no business output |
