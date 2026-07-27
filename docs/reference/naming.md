# Naming

Parsley's naming follows two systems: a visibility convention separating the public API from
the internals, and an academic-grounding test applied to every identifier a change touches.
This page is the living register of naming decisions; new decisions are recorded here. The works
each decision cites are collected in the [bibliography](bibliography.md).

## Visibility convention

`Causal*` names the public API; `Parsley*` names package-private internals. Implementations of
a narrow seam are named for their backing rather than for the seam (`KafkaTopics*`,
`StoreBacked*`). The convention is codified in the package Javadoc (`package-info.java`).

## Module presentation

Each of the three protocol layers is one package-private class presented in the module style
of Cachin, Guerraoui, and Rodrigues (*Introduction to Reliable and Secure Distributed
Programming*): requests in, indications out, and the properties the module guarantees (its
[named invariants](../foundations/invariants.md)). Two package-wide deviations from the textbook style are
stated once in the package Javadoc so an academic reader is oriented immediately:

- **Indications are pulled, not pushed.** Deliveries come back as ordered return values rather
  than through an upcall, because Kafka Streams' threading is synchronous. Same semantics,
  pull style.
- **The sender's clock increment is performed by the broker.** In Birman–Schiper–Stephenson
  the sender increments its own vector entry at send; here the increment is the broker's
  offset assignment, learned asynchronously via producer acknowledgements. This is why
  `broadcast()` attaches a stamp but cannot include the record's own coordinate.

## The academic naming test

At every class, method, or variable a change touches:

1. **Exact match.** If the name is our own invention but the literature already names the
   concept, and the concepts match exactly, adopt the academic term and cite the source in
   Javadoc.
2. **Near miss.** If the concept only approximately matches the academic term, do not borrow
   it. A misappropriated term imports a specification the code does not honour (Dataflow
   watermarks are heuristic where Parsley's null messages are exact; partitions are not
   point-to-point links).
   Keep our name and document the nearest term and the delta.
3. **Kafka-specific.** If the mechanism is genuinely Kafka-specific with no literature analog,
   keep the coinage and say so explicitly in Javadoc ("no literature analog; Kafka-specific"),
   so a reader knows not to go looking.

## Decision register

Decisions already made under the test. A "pending" verdict means the adoption is sanctioned
but waits for a change that touches the name.

| Name | Verdict | Literature |
|---|---|---|
| `vectorTime()` | Rule 1, exact match, adopted: it is VT(p), the process's vector time, and `broadcast()` attaches it as VT(m) := VT(p) at send. One read serves both the outbound stamp and the relay comparison because those are the two literature roles of the same value | Mattern 1988, "Virtual Time and Global States"; Fidge 1988 |
| `completeness()` | Dissolved rather than renamed. It named `frontier ⊔ carriedAncestry ⊔ advertised`, a value with one caller besides the stamp, and no term may be borrowed for it: "causal past" (Schwarz–Mattern 1994) includes all preceding events where this excludes own outputs, and EIT / "channel time" is a **minimum** over input channels where this is a max-merge, the opposite lattice operation. Its body folded into `vectorTime()`, and its one caller — the heartbeat — now reads the `advancedConsumedChannel` signal, the rule it was expressing by diffing a clock | Near misses refused: Schwarz–Mattern 1994; Bryant 1977, Chandy–Misra 1979 |
| `ParsleyBufferStore` / "held records" | Coinage with an exact match; adopt "hold-back queue" when next touched | The ISIS/CBCAST papers' term for exactly this structure |
| `merge()` | Grounded; kept with citation | CRDT merge / lattice join (component-wise max) |
| channel clocks (the `channels` map) | Name kept; nearest term cited with the delta (per-channel, not per-process) | Row of a matrix clock (Sarin–Lynch; Wuu–Bernstein) |
| `frontier` | Grounded; kept with citation | Frontier of a consistent cut (Mattern; also Naiad/Timely Dataflow) |
| `dominates()` | Grounded; kept with citation | Vector-clock dominance (component-wise comparison) |
| `bridge()`, `seedIfFirstSeen()` | Rule 3: Kafka-specific, no analog; marked in Javadoc | — |
| `ownOutputs` | Coinage kept; cites the BSS own-entry it reconstructs | Nearest is VT(m)'s own-slot semantics (Birman–Schiper–Stephenson) |
| `highestDelivered` | Coinage kept, symmetric with `highestReceived`: the max projection of this node's delivered set, a component `vectorTime()` merges rather than VT(p) itself. The split from the contiguous frontier is Kafka-specific (rule 3), stated on the field | — |
| `ParsleyTopologySim` ground truth | Adopted with citation, restricted to delegate-visible information flow | Causal histories (Schwarz–Mattern 1994) |
| `ParsleyOwnOutputRegistry`, `ParsleyOwnOutputInterceptor` | Rule 3: Kafka-specific coinage (exists only because the broker performs the sender's clock increment and reports it via producer acks); marked in Javadoc | — |
| crossing wait, `awaitQuiescentExcept` | Coinage; "quiescence" in its plain concurrent-programming sense, no specific-algorithm borrowing | — |
| `CausalClock` (wire header `parsley-causal-clock`) | Grounded: the type plays both vector-clock roles (attached = VT(m), accumulated = VT(p)), so a "dependencies" name misdescribes the accumulating role | Vector clock (Fidge 1988; Mattern 1988); the Javadoc states the VT(m)/VT(p) duality and the indexed-by-channel variant |
| `ParsleyGossip.Reception`, `ParsleyCausalBroadcast.Outcome` | Coinage: the (deliveries, advancedConsumedChannel) pair each receive returns; no single academic term. The deliveries list is the module-style deliver indication in pull form, stated in Javadoc | — |
| `advancedConsumedChannel` | Descriptive coinage for the CMB input-channel-clock advance, carried by both receives: a null message's carried clock on L3, a seed or bridge on the business path. Only an advance on a consumed channel obliges an advertisement, so a name claiming any new knowledge over-claims: custody advances knowledge without obliging anything. Near-miss borrowings ("EIT advance", "channel time") refused | CMB trigger discipline (Bryant 1977; Chandy–Misra 1979: null-message sends obliged by input channel clock advances); null-message reduction precedent DeVries 1990; cycle-echo precedent Cai–Turner 1990 and Wood–Turner PADS '94 — CMB cited on the class |
| `advertise` | Plain-English epidemic vocabulary (make the clock observable), consistent with "advertised clocks" | Demers et al. 1987 (epidemic dissemination), cited on the class |
| null message (header `_parsley_null_message`, `isNullMessage()`) | Grounded; the "watermark" vocabulary is a rule 2 near miss and is not used | Chandy–Misra–Bryant null messages (timestamp-only, value null) |
| Test and docs sample identifiers `c1..cN`, `m1..mN`, `p1..pN` | Channels (topics) are `c<n>` — sound for tests because a single-partition test topic is the channel; messages are `m<n>`; processor nodes are `p<n>`. Scenario-encoding names stay in complex broker ITs (funnel, diamond, prereq, and similar). Role-bearing record variables (`stamped`, `nullMessage`, `consumed`) keep their roles: the literature writes m′ for a transformed m rather than renumbering. `p<n>` is deliberately not clock vocabulary — Parsley clocks are channel-indexed, so process names never appear in clock positions | Messages m, m′, m₁..mₖ (Lamport 1978; BSS 1991; Schwarz–Mattern 1994 VT(m)); channels c (CSP; Chandy–Misra–Bryant); processes p₁..pₙ (Fidge; Mattern) |
| `ParsleyFrontierState` (frontier store key `"frontier"`) | Rule 3: Kafka-specific coinage for the `{ns}-frontier` store's durable value, the serialised union of the node's persisted causal metadata. No literature term names a state-store envelope; the name leads with the grounded `frontier` and stays consistent with the store name. The key spells out the same word rather than an opaque single letter, and the value carries a wire-version byte | — |
| `ParsleyVectorClock` | Adopted with citation: it is a vector clock, indexed by channel rather than by process, and the variant is stated on the class | Vector clock (Fidge 1988; Mattern 1988) |
| `ParsleyCausalBroadcast` | Adopted with citation: the class is the causal-broadcast module of the stack, named for the abstraction it implements rather than for its role in the process | Causal broadcast (Birman–Schiper–Stephenson 1991; Cachin–Guerraoui–Rodrigues module style) |
| `ParsleyMessage` | Grounded; kept with citation: BSS delivers messages | Message (Birman–Schiper–Stephenson 1991) |
