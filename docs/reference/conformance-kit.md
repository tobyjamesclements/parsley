# The conformance kit

Parsley's guarantees rest on [verification](../design/verification.md), not on trust in the
one shipped runtime: a deterministic simulator hosts the real protocol under seeded random
interleavings, and a ground-truth oracle — which never reads a vector clock — judges every
delivery against real happened-before ancestry. The conformance kit is that machinery,
packaged so it runs outside this repository. It exists for two consumers:

- **A vendored copy.** Copying the source into another codebase is permitted and workable,
  but a copy without the verification is unverified — vendoring rule 2. The kit travels with
  the test tree; this page says what must keep passing.
- **An alternative host.** The protocol is only sound under a host contract no API can
  enforce. The shipped Kafka Streams adapter upholds it by construction; any other host can
  claim conformance only by verification — port the host duties below, run the kit, and the
  same obligations hold or fail loudly.

## Getting the kit

The kit is published as the `test-jar` artifact of the library's own coordinates:

```xml
<dependency>
    <groupId>io.github.tobyjamesclements</groupId>
    <artifactId>parsley</artifactId>
    <version>0.2.0-SNAPSHOT</version>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
```

The broker smoke tests (`*IT`) are excluded from the artifact: the kit needs no Docker, no
Testcontainers, and no broker. Its only test-time dependencies are JUnit 5 and (for the
Streams adapter's own tests) `kafka-streams-test-utils`; driving `SimWorld` alone needs
neither Kafka Streams nor a test framework beyond your own.

The kit is deliberately package-private, like the internals it verifies. Tests that drive it
are declared in the `io.github.tobyjamesclements.parsley` package (or the renamed package of
a vendored copy) — the same convention the kit's own suites use.

## What is in it

| Piece | Role |
|---|---|
| `Oracle` | Ground truth. Tracks real happened-before ancestry entirely outside the protocol; checks causal order, per-channel FIFO, and duplicates on every delivery, completeness at drain. |
| `SimWorld` | The seeded scheduler: interleaves fetches, position advances, producer ops, crashes, and restarts until the world drains or the step budget fails it. |
| `SimBroker` | The broker model: real offset occupancy for business records, transaction markers, and aborted records, as a `read_committed` consumer observes them. |
| `SimNode` | The reference host. Upholds every host duty the way the Streams adapter does; the executable form of the host contract below. |
| `SimBehavior`, `EdgeProducer` | Scripting: stage logic (forward, filter, damped feedback) and plain clients at the topology's edge. |
| `CausalNodeSimTest` | The scenario suite discharging obligations V1–V7 of [verification](../design/verification.md#obligations). |
| `OracleSelfTest` | V8: a deliberately broken protocol (`EagerProtocol`) must be flagged. A harness that cannot fail is not a harness. |
| `HostContractSelfTest` | V9: a deliberately broken host must be flagged — the host-side analogue of V8. |

## The host contract

The protocol surface a host drives is `DeliveryProtocol`; the duties the host owes it are
modelled, one for one, by `SimNode`. A host that upholds all of them inherits the simulator's
verdict; a host that shirks one is the thing the kit must catch — and `HostContractSelfTest`
proves it does for the two duties whose violation is silent in production until data is wrong.

| # | Duty | Where `SimNode` models it | What catches a violation |
|---|---|---|---|
| H1 | Hand records to `onRecord` in strictly increasing per-channel offset order (Kafka's order through a `read_committed` consumer). | `step` fetches `nextFetchable` from the committed position, in order. | Oracle FIFO and causal checks on delivery. |
| H2 | Report consumer position advances past offsets that never arrive as records (`positionAdvance`) — the liveness signal that bridges trailing markers. | `stepPositionAdvance`, offered whenever the log continues past the position with nothing fetchable. | Completeness at drain: stranded held records are flagged as never delivered ([position-advance bridging](../foundations/liveness.md#position-advance-bridging)). |
| H3 | Commit store mutations, consumer positions, and sends step-atomically; a crash aborts all three together (EOS). | `transactionalStep`: staged store and positions, appended records aborted on crash, oracle knowledge rolled back in mirror. | Duplicate, causal, and completeness checks after crash-injecting runs. |
| H4 | Partition before stamping, stamp at the single site (`prepareSend` with the concrete destination channel), and append what was stamped. | `sendBusiness`, which also asserts every offset claim names a really-appended offset. | Inline harness assertion; downstream, wedged or violated gates. |
| H5 | Restart from committed state through the full init path — restore, end-offset seed, rescope, `resumePositions`. | `start`, which reconstructs the protocol and resolves positions from committed state. | Crash-recovery scenarios: completeness and causal checks across restarts. |
| H6 | Respect the log start: a position below it resets to the first surviving offset, and retention never outruns an active subscriber's lag. | `start` clamps positions to the log start; `advanceLogStart` refuses to delete unfetched history. | The guard itself, plus baseline accounting in the oracle. |

## Running it against a copy

For a vendored copy, conformance means: the kit's suites pass in **your** CI, against **your**
copy, unmodified in substance. Concretely:

- Copy the test tree with the source tree; renaming the package and classes is fine (no
  runtime string carries the library name), re-synthesizing the logic is not — the sim and
  oracle encode invariants that fail silently when paraphrased.
- `OracleSelfTest` and `HostContractSelfTest` must keep failing their broken subjects. If a
  rename or adaptation makes them pass vacuously, the copy is unverified regardless of how
  green the rest looks.
- Never change the [wire format](wire-format.md): it is what keeps a copy interoperable with
  every other Parsley application.
- Record the upstream commit the copy was taken from, so it can be diffed when fixes land
  upstream.

For an alternative host, the port target is `SimNode`: it is small, single-threaded, and each
duty above names the lines that uphold it. Verify the host's decision logic through the same
seam the simulator uses (`DeliveryProtocol` in, `Delivery` out), and keep the kit's step
budgets and anti-vacuity assertions — a suite that passes because nothing interesting
happened proves nothing.
