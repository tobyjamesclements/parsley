# Error handling

Handlers and folds carry no error channel in their signatures: a handler returns emissions,
a fold returns a step, and there is no `Either` or result wrapper around either. This page
states what the runtime does when logic throws, why that is the default, and how to model
failures that are part of your domain.

## What happens when logic throws

An uncaught `RuntimeException` from a handler, a fold, or a codec fails the stream task. The
task's transaction aborts atomically: the delivery, the fold state it wrote, the emissions
it caused, and the sequence claims stamped on them all roll back together, and the
committed state remains exactly what the last successful delivery left. What happens to the
application next is the host runtime's policy, decided by the
[uncaught-exception handler](#the-stream-thread-policy).

On restart the task refetches from its committed position and redelivers the same records
in the same causal order. The two failure classes therefore behave differently:

- A **transient failure** — a schema-registry outage, a broker hiccup — retries with
  exactly-once semantics. The aborted attempt leaves no trace, and the retry delivers the
  record as if the failure never happened.
- A **deterministic failure** — a bug, a message the logic cannot handle — recurs on every
  redelivery and fails the task each time. The record blocks its channel until the logic
  changes.

## Why abort-and-retry is the default

The runtime cannot tell the two classes apart: a throw carries no evidence of whether the
same inputs produce it again. Aborting is the only response that is safe under both
readings. Committing a failure outcome instead — routing the record to an error topic, for
example — is irrevocable under exactly-once semantics, and performed on a transient throw it
misroutes a record that a retry delivers normally. A blocked channel is loud, visible in the
[hold metrics](../reference/metrics.md), and fully recoverable by deploying fixed logic; a
misrouted record is none of those.

For the same reason there is no retry budget and no skip-after-N-attempts policy. An
attempt count is a function of crash timing, not of the delivered history, so any outcome
keyed to it makes fold state and emissions depend on how often a task happened to restart —
the same class of guess as delivering on a timeout, which the
[contract](expectations.md#what-parsley-promises) excludes.

## Domain failures are data

A failure that belongs to your domain — a validation reject, a request the business logic
refuses — is not an exception to the model above, because it is not a task failure at all.
Model it as an emission: declare an error sink, and return the failure through it. An error
emission is stamped, claimed, and causally ordered downstream like any other, so a consumer
of the error topic sees rejects in causal order too.

`Handler.recovering` and `Fold.recovering` package this pattern for logic that signals
failure by throwing. The decorator catches a `RuntimeException` and converts it into the
emissions a recovery function yields, typically to a declared error sink:

```java
Topic<String, Order> orders = Topic.of("orders", Codec.utf8(), orderCodec);
Topic<String, Shipment> shipments = Topic.of("shipments", Codec.utf8(), shipmentCodec);
Topic<String, String> rejects = Topic.of("rejects", Codec.utf8(), Codec.utf8());

Stage stage = Stage.named("shipping")
        .on(orders, Handler.recovering(
                m -> List.of(shipments.send(m.key(), ship(m.value()))),
                (m, e) -> List.of(rejects.send(m.key(), e.getMessage()))))
        .into(shipments, rejects)
        .build();
```

The fold form keeps the key's prior state unchanged and carries the recovery's emissions in
its step. In both forms the recovery commits in the same transaction as the delivery — it
is exactly-once and never retried — which is what makes the caveat above load-bearing:

- **Wrap only logic whose throws are deterministic** in the message (and, for a fold, the
  state). A transient failure caught by the decorator routes a good record to the error
  sink irrevocably, where the undecorated form's abort-and-retry delivers it.
- An `Error` propagates through the decorator and fails the task. Only `RuntimeException`
  is recoverable, because only it can plausibly be a domain outcome.

## Malformed records

Codec failures sit before user logic — inbound bytes decode at delivery, with no handler
frame on the stack — so the decorators cannot reach them, and a codec exception fails the
task as [the codec contract](codecs.md#the-codec-contract) requires. For a source topic
whose producers you do not control, where malformed bytes are a fact of the domain rather
than a wiring bug, make the codec total over a sum type instead:
[decoding failure you can route](codecs.md#decoding-failure-you-can-route). The failure
then arrives in the handler as a value, and the handler routes it like any other domain
failure.

## The stream-thread policy

The division of labour: the transaction decides what a failure does to the data (nothing —
it aborts), and the `StreamsUncaughtExceptionHandler` registered on
[`CausalStreams`](getting-started.md#what-the-runtime-wires-for-you) decides what it does
to the application — replace the thread, shut down the client, or shut down the
application. `CausalStreams` registers the handler totalized: a handler that itself throws,
or returns null, resolves to `SHUTDOWN_CLIENT` with both failures logged, so a handler bug
shuts the client down loudly instead of silently losing the stream thread.
