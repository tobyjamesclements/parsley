# Worked examples

Every shape in this guide has a runnable, asserted counterpart in the test tree. They are
ordinary JUnit tests over `Parsley.testTopology()` and Kafka Streams'
`TopologyTestDriver` — no broker, no Docker — so each one runs in seconds, and they ship in
the [conformance kit](../reference/conformance-kit.md) artifact, which makes them executable
documentation rather than prose that can rot.

Each example asserts the guarantee, not just the happy path: where a shape exists because of
causal ordering, the example feeds records in the adversarial order and shows the gate
holding a record until its cause arrives.

| Example | Shape | What it demonstrates |
|---|---|---|
| `ExampleCommanderTest` | Commander / CQRS | Commands to events to projections; rejections as facts; read models rebuilt by replay; a projection held behind its command at a shared consumer |
| `ExampleRequestReplyTest` | [Request and reply](topologies.md#request-and-reply) | Effect-before-reply — the reply's sequence claim on the effect, and the reply held until the effect is delivered |
| `ExampleTickPolicyTest` | [Ticks](ticks.md) | A channel-scoped policy on its own cadence; the tick-state slot distinct from per-key state; a tick as a stamped record claiming a consistent cut |
| `ExamplePlainClientEdgeTest` | [Plain clients](clients.md) | `CausalClock.observe` making what a client read a cause of what it writes; sequential sends ordered across topics; stamps readable by any consumer |
| `ExampleConcurrentInputsTest` | [Fan-in](topologies.md#fan-in) | What causal order does *not* promise: commutative logic is interleaving-independent, non-commutative logic is not |

## The Commander example

The centrepiece is the **Commander** architecture (Bobby Calderwood, "Commander: Better
Distributed Applications through CQRS and Event Sourcing", 2015), because it exercises most
of the guide at once and because its known weak spot is exactly what causal delivery
removes.

The pattern: clients write immutable **commands**; a commander service validates each command
against its write-side state and emits **events** — facts, including rejections — to an event
log; **projectors** fold that log into read models. The write side never mutates a read model
directly, which is what makes read models disposable and rebuildable.

```text
commands --> commander --> events --> projector --> views
                                                      |
commands ---------------------------------------------+--> reconciler
```

What the pattern leaves to luck is the CQRS split itself. A projection is derived from an
event, which is derived from a command, so a service consuming both the command log and a read
model can be handed the projection before the command that produced it — the classic
read-your-writes failure, usually patched with polling, version checks, or a sticky read.

Under Parsley the projection's stamp transitively claims the command two hops upstream, so any
consumer of both topics delivers the command first. The example asserts this the hard way: it
feeds the reconciler the projection *first*, shows that nothing is emitted at all while the
gate holds it, and then feeds the command and observes both released in causal order.

Two more properties of the pattern come out of the same example. A rejection is an event, not
an exception: the write-side state is untouched, the projector ignores it, and nothing is
retried — the [error-handling](error-handling.md) rule that domain failures belong in the
domain. And because a read model is a pure fold of the event log, a fresh projector replaying
that log rebuilds the identical view, which is also the late-joiner shape: a new consumer
needs no coordination, it simply starts consuming.

## Running them

```
mvn test -Dtest='Example*Test'
```

To adapt one, copy the class and change the domain: the topics, codecs, and folds are the
only parts specific to the example, and the driver wiring is the same four lines everywhere.
