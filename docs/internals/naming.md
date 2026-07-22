# Naming

Parsley's naming follows two systems: a visibility convention separating the public API from
the internals, and an academic-grounding test applied to every identifier a change touches.
This page is the living register of naming decisions; new decisions are recorded here.

## Visibility convention

`Causal*` names the public API; `Parsley*` names package-private internals. Implementations of
a narrow seam are named for their backing rather than for the seam (`KafkaTopics*`,
`StoreBacked*`). The convention is codified in the package Javadoc (`package-info.java`).

## Module presentation

Each of the three protocol layers is one package-private class presented in the module style
of Cachin, Guerraoui, and Rodrigues (*Introduction to Reliable and Secure Distributed
Programming*): requests in, indications out, and the properties the module guarantees (its
[named invariants](invariants.md)). Two package-wide deviations from the textbook style are
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
   watermarks are heuristic where ours were exact; partitions are not point-to-point links).
   Keep our name and document the nearest term and the delta.
3. **Kafka-specific.** If the mechanism is genuinely Kafka-specific with no literature analog,
   keep the coinage and say so explicitly in Javadoc ("no literature analog; Kafka-specific"),
   so a reader knows not to go looking.

## Decision register

Decisions already made under the test. A "pending" verdict means the adoption is sanctioned
but waits for a change that touches the name.

| Name | Verdict | Literature |
|---|---|---|
| `completeness()` | Coinage with an exact match; adopt `vectorTime()` when next touched. It is VT(p) computed under partial visibility | Mattern 1988, "Virtual Time and Global States" |
| `ParsleyBufferStore` / "held records" | Coinage with an exact match; adopt "hold-back queue" when next touched | The ISIS/CBCAST papers' term for exactly this structure |
| `merge()` | Grounded; kept with citation | CRDT merge / lattice join (component-wise max) |
| channel clocks (the `channels` map) | Name kept; nearest term cited with the delta (per-channel, not per-process) | Row of a matrix clock (Sarin–Lynch; Wuu–Bernstein) |
| `frontier` | Grounded; kept with citation | Frontier of a consistent cut (Mattern; also Naiad/Timely Dataflow) |
| `dominates()` | Grounded; kept with citation | Vector-clock dominance (component-wise comparison) |
| `bridge()`, `seedIfFirstSeen()` | Rule 3: Kafka-specific, no analog; marked in Javadoc | — |
| `ownOutputs` | Coinage kept; cites the BSS own-entry it reconstructs | Nearest is VT(m)'s own-slot semantics (Birman–Schiper–Stephenson) |
| `highestDelivered` | Coinage kept, symmetric with `highestReceived`: the max projection of the delivered vector VT(p). The split from the contiguous frontier is Kafka-specific (rule 3), stated on the field | — |
| `ParsleyTopologySim` ground truth | Adopted with citation, restricted to delegate-visible information flow | Causal histories (Schwarz–Mattern 1994) |
| `ParsleyOwnOutputRegistry`, `ParsleyOwnOutputInterceptor` | Rule 3: Kafka-specific coinage (exists only because the broker performs the sender's clock increment and reports it via producer acks); marked in Javadoc | — |
| crossing wait, `awaitQuiescentExcept` | Coinage; "quiescence" in its plain concurrent-programming sense, no specific-algorithm borrowing | — |
| `CausalClock` (renamed from `CausalDependencies`) | Renamed: the type plays both vector-clock roles (attached = VT(m), accumulated = VT(p)) and "dependencies" misdescribed the accumulating role. Wire header `parsley-causal-dependencies` → `parsley-causal-clock` | Vector clock (Fidge 1988; Mattern 1988); the Javadoc states the VT(m)/VT(p) duality and the indexed-by-channel variant |
| `ParsleyGossip.Reception` | Coinage: the (deliveries, advancedConsumedChannel) pair the gossip receive returns; no single academic term. The deliveries list is the module-style deliver indication in pull form, stated in Javadoc | — |
| `advancedConsumedChannel` | Descriptive coinage for the CMB input-channel-clock advance; replaced `learnedSomethingNew`, which over-claimed once custody stopped obliging relays. Near-miss borrowings ("EIT advance", "channel time") refused | CMB trigger discipline (Bryant 1977; Chandy–Misra 1979: null-message sends obliged by input channel clock advances); null-message reduction precedent DeVries 1990; cycle-echo precedent Cai–Turner 1990 and Wood–Turner PADS '94 — CMB cited on the class |
| `advertise` | Plain-English epidemic vocabulary (make the clock observable), consistent with "advertised clocks" | Demers et al. 1987 (epidemic dissemination), cited on the class |
| null message (header `_parsley_null_message`, `isNullMessage()`) | Grounded; renamed from the "watermark" vocabulary, which was a rule 2 near miss | Chandy–Misra–Bryant null messages (timestamp-only, value null) |
| Test and docs sample identifiers `c1..cN`, `m1..mN`, `p1..pN` | Channels (topics) are `c<n>` — sound for tests because a single-partition test topic is the channel; messages are `m<n>`; processor nodes are `p<n>`. Scenario-encoding names stay in complex broker ITs (funnel, diamond, prereq, and similar). Role-bearing record variables (`stamped`, `nullMessage`, `consumed`) keep their roles: the literature writes m′ for a transformed m rather than renumbering. `p<n>` is deliberately not clock vocabulary — Parsley clocks are channel-indexed, so process names never appear in clock positions | Messages m, m′, m₁..mₖ (Lamport 1978; BSS 1991; Schwarz–Mattern 1994 VT(m)); channels c (CSP; Chandy–Misra–Bryant); processes p₁..pₙ (Fidge; Mattern) |

## Renames of record

Carried by the decisions above, pre-release with no deprecation aliases: `ParsleyClock` →
`ParsleyVectorClock` (it is one, indexed by channel rather than process — a stated variant);
`ParsleyEngine` → `ParsleyCausalBroadcast`; `ParsleyFrontier` folded into `ParsleyChannels`;
`CausalDependencies` → `CausalClock`; the "watermark" vocabulary → "null message".
`ParsleyMessage` keeps its name: BSS delivers messages.
