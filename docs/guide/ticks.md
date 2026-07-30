# Ticks

A stage may declare ticks: at the declared interval, the stage runtime emits one stamped
record per task to the stage's own tick topic, consumes it back through the causal gate like
any other record, and delivers it to your logic as a `Tick`. Wall-clock time reaches pure
logic only as data on a delivered record, so tick logic stays a pure function, testable with
plain equality, and the tick itself is durable: a tick whose processing fails is redelivered
in order by the aborted transaction's retry, never lost.

```java
Stage balances = Stage.named("balances")
        .state(balanceCodec, Balance::zero)
        .on(orders, (bal, m) -> Step.of(bal.minus(m.value().total())))
        .ticks(Duration.ofMinutes(1),
                tick -> List.of(audits.send(null, "cut@" + tick.timestamp())))
        .into(audits)
        .build();
```

## What a tick means

A tick's stamp is written at the same stamping site as every emission, so it claims the
task's consumed history at the moment of emission — a consistent cut of everything the task
had delivered when the interval fired. Two consequences follow.

First, any consumer of an emission a tick causes is gated behind that cut: a downstream
stage sees the tick's effects only after everything the ticking task had seen. A
channel-scoped policy event needs no enumeration — "everything in this channel so far" is
exactly what the stamp claims, entry by entry.

Second, the scope is the channel. A task consumes one partition of each source topic, so the
history a tick can speak for is its own slice's, the same per-task scope as the delivery
guarantee itself. A policy that must react per entity at a chosen time is a topology shape,
not a tick: emit a timer request and let a scheduler produce the keyed reply — the
[request and reply](topologies.md#request-and-reply) shape, with the fire record stamped by
a [`CausalClock`](clients.md) so it is gated behind the request's causes.

## Stateless and stateful ticks

On a stateless stage, tick logic is a `TickHandler`: a pure function from the tick to its
emissions. On a stateful stage, `ticks` may instead take a `TickFold`, which steps the
stage's tick state: one reserved per-partition slot of the stage's declared state type,
distinct from every per-key slot including the null key's. It is resolved to the stage's
initial value when unseen, persisted transactionally with the delivery, and deleted by
returning `Step.of(null, ...)` — the same discipline as a key fold.

A tick carries no entity key, so tick logic never sees per-key state. The slot holds
tick-to-tick memory — the last time a policy fired, counters, rate limits. A stage whose
state type is `Long` can, for example, emit a cut marker every fifth tick:

```java
.ticks(Duration.ofMinutes(1), (Long quiet, Tick tick) -> quiet + 1 >= 5
        ? Step.of(0L, audits.send(null, "cut@" + tick.timestamp()))
        : Step.of(quiet + 1))
```

A stateful stage may also declare ticks with a plain `TickHandler`, which leaves the slot
untouched.

## Delivery timing

Ticks ride the ordinary pipeline, so two delays are inherent. A tick becomes visible to the
task's consumer only when the transaction that emitted it commits, so tick delivery lags
emission by up to the commit interval. And a tick's stamp merges the claims advertised by
records currently held at the task, so a tick emitted while a record is held on an
unresolved dependency is itself held until that dependency delivers — delay-only, and
liveness is unaffected: every claim names a really-appended offset. The tick interval is a
cadence floor, not an exact schedule.

## The tick topic

The tick topic is named `vc-<stage-name>-ticks` and is the stage's own: it appears as
one of the stage's sources, so composition rules prevent any other stage from consuming it.
Like every declared topic it must exist before the application starts, and assembly fails
loudly when it is missing or its partition count differs from the stage's widest source
topic — each task emits ticks to its own partition, so the counts must agree exactly.
Ticks are transient by design; a short retention on the topic is enough, provided it covers
consumer lag like any causal topic.

## Testing

Tick logic is pure, so the primary tests need no runtime:

```java
assertEquals(Step.of(1L, audits.send(null, "fired")),
        policy.apply(0L, new Tick(1000L)));
```

Under `TopologyTestDriver`, `advanceWallClockTime(interval)` emits the tick, and the driver
feeds the tick topic back into the stage; construct the driver with a fixed initial wall
clock to make tick timestamps deterministic. Each emitted tick counts on the
`ticks-emitted-total` metric ([metrics](../reference/metrics.md)).

Runnable: `ExampleTickPolicyTest` ([worked examples](examples.md)) — a policy on its own
cadence, the tick slot shown distinct from per-key state, and the tick's stamp inspected.
