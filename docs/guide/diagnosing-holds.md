# Diagnosing held records

When records stop coming out of a stage, the gate is usually doing its job: a cause is
missing, lagging, or unstamped, and Parsley is refusing to deliver an effect before it. The
rule that overrides everything is that this is never fixed by skipping, reordering, or adding
a timeout — the fix is to deliver the missing cause. This page is how you find out which cause
that is.

Three surfaces answer three different questions:

| Surface | Answers |
|---|---|
| [Metrics](../reference/metrics.md) — `records-held`, `records-held-age-max-ms` | *Is* anything held, and for how long? Alert on these. |
| `CausalStreams.explainHolds()` | *Why* is it held — which cause, on which channel, how far behind? |
| The `vc-hold-*` WARN lines | The same answer, pushed, when a hold outlives its threshold. |

The committed consumer position advances past held records, so a stage's own consumer lag
never shows holding. The metrics are the only signal that something is held at all, which is
why they are what you alert on.

## Asking the runtime why

`explainHolds()` returns one entry per gating head across the instance's local tasks,
longest-held first:

```java
for (HeldRecord held : app.explainHolds()) {
    log.info(held.summary());
}
```

Each entry names the held record's coordinate, how long it has gated its channel, how many
records wait behind it, and every cause it is still waiting for — with the local watermarks
that show the gap:

```text
stage 'reconciler' task 0_3 has held views:3@8814 for 47000ms (queue depth 219):
vc-hold-not-fetched waiting on commands:3 offset 51204 (local frontier 51199, position 51200)
— the cause has not reached this consumer yet; check lag on that topic
— see docs/guide/diagnosing-holds.md#the-cause-has-not-arrived
```

Only queue heads appear. Holding is head-of-line blocking by design, so the head is the whole
story: the 219 records behind it wait on it, not on causes of their own.

Two properties are worth knowing before you build on this. It is **payload-free** — coordinates,
claims, and watermarks, never key or value bytes — which is what keeps it from becoming a back
door to state that [interactive queries are withheld](getting-started.md#what-the-runtime-wires-for-you)
to protect. And it reads a snapshot each task publishes from its own stream thread on its
wall-clock punctuator, so it takes no locks on the processing path, and a reading is at most
one punctuator interval stale and describes an in-flight view an aborted transaction may roll
back. Diagnose with it; do not treat it as a source of truth about application state.

## The warning

A hold that outlives its stage's threshold is logged once, at WARN, with the same summary:

```java
Stage.named("reconciler")
        .on(views, ...)
        .holdWarningAfter(Duration.ofMinutes(2))   // default: 30 seconds
        .into(...)
```

One hold warns once — a line per punctuator tick would bury the diagnosis it exists to
surface — and a new head on the same channel is a new hold, so it warns again. Zero or a
negative duration disables the warning and leaves the accessor and metrics as the interface.

It is WARN and not ERROR deliberately: nothing has failed. The line says a wait has lasted
long enough to be worth a look, and names what to look at. Match on the `vc-hold-*` code if you
route logs by pattern; the codes are stable.

## The diagnoses

### The cause has not arrived

`vc-hold-not-fetched` — the claimed record has not been fetched by this consumer yet. The
local position is at or below the claimed offset, so the record is genuinely still in flight.

This is the ordinary case and it usually resolves itself. Look at the *cause* topic: consumer
lag on it, whether its producer is healthy, and whether the producing stage is itself stalled.
A cause channel that is merely slow makes a hold that clears; a cause channel whose producer
has stopped makes one that does not.

### The cause is itself held

`vc-hold-held-upstream` — the claimed record has been fetched here but not delivered, so it
is sitting in its own channel's hold queue behind that channel's head.

Follow the chain: that channel's head appears in the same report, with its own unmet causes.
Holds compose, and the useful end of the chain is the one record whose cause has not arrived
at all. Fix that one and the whole chain drains.

### No record from that sender

`vc-hold-sender-unseen` — a sequence claim naming a sender this task has never delivered on
that channel. Sequence claims are how a stamp names the sender's own just-issued sends, whose
offsets the broker has not yet assigned.

Usually this is transient: the sender's record is on its way. If it persists and the sender
never writes that partition again, this is the documented
[late-joiner window](../foundations/liveness.md#the-sequence-claim-caveat-late-joiners) — a
consumer that baselined at the log end cannot deliver a record that sits below its baseline.
Baseline late joiners at the last stable offset rather than the log end.

### The sender's later records are behind

`vc-hold-sender-behind` — a sequence claim ahead of what this task has delivered from that
sender. The sender's later records are in flight, or their transaction has not committed.

Under `read_committed`, records of an open transaction are invisible until it commits, so a
long-running or stuck producer transaction shows up exactly here. Check the sender's commit
interval and whether it is making progress.

## What none of them mean

None of these diagnoses is ever resolved by skipping the held record. Every claim names a
really-appended offset, so every wait terminates once its cause is delivered
([liveness](../foundations/liveness.md)); a hold that never clears means a cause that never
arrives, and that is a broken upstream, a retired sender, or retention that outran a consumer
— all of which are fixed upstream.

If holding is costing more than the ordering is worth for a particular topic, the answer is to
stop stamping that producer, not to weaken the gate: an unstamped record claims nothing and
delivers immediately, which is a per-topic decision made at the producer
([plain clients](clients.md)).
