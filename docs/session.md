# Session consistency

Parsley's guarantee ends at the last consumer's seam. A client behind an HTTP gateway sees
projections of the pipeline's output with no ordering promise at all: it can write an order,
query a read model, and be told the order does not exist.

The `session` package extends the causal frontier past that edge. A client holds its
frontier as a token the server mints; the server stamps a validated token onto writes as
causes, refuses to serve reads from data whose recorded past does not cover the token, and
returns the token refreshed with what was served. That buys **session consistency** —
read-your-writes, monotonic reads, writes-follow-reads — for a participant that runs no
protocol at all.

This is an application-layer pattern built *on* Parsley, not a change to the guarantee
Parsley provides. Nothing in `core`, `api` or `kafka` reads a token, and holding one grants
no delivery guarantee. The design and its open questions are recorded in issue
[#96](https://github.com/tobyjamesclements/parsley/issues/96); the layout decision is D99.

## The client as a process

The pattern works because the protocol's rules already describe a client, with channel
indexing doing the heavy lifting: coordinates are `(topicId, partition, offset)` — durable,
globally meaningful, and meaningful to a party in no group membership.

| Engine rule ([Model](model.md#the-causal-frontier)) | Client equivalent |
|---|---|
| Receipt merges every carried cause | Read merges the served data's recorded past |
| Delivery merges `(channel, position)` | — the client delivers nothing |
| Sends are stamped with the frontier | Write stamps the validated token as causes |
| Sends do not enter the frontier | A write enters the token only once the broker's acknowledgement confirms its coordinate |

## One type, every role

`CausalPast` is the companion: a frontier used as a client's token, as the past recorded
beside a projected row, and as either side of the comparison between them.

**Write path.** Decrypt and bound the inbound token → produce with the token's pairs merged
into the record's `parsley.causes` header (`encode()` is a valid header value) → merge the
acknowledged `(topic, partition, offset)` coordinate → re-mint → return.

**Read path.** Decrypt the inbound token → query → `recordedPast.coverageOf(token)` → if it
does not cover, wait bounded, fail with retry-after, or route to a caught-up replica; never
serve → merge the served past into the token → re-mint → return.

The wire form is the [frozen grammar](wire-format.md) itself, and `decode` is exactly as
strict as the engine's: a damaged token is refused, never salvaged into a weaker one.

Coordinates at the edge arrive as topic names — the produce acknowledgement and the handler
seam alike — while `ChannelId` carries the topic id. Resolve names once at startup, from
the admin client; a recreated topic resolves to a different id, which is the point.

## Coverage fails closed

The delivery gate skips a cause on a channel outside the process's received set — mandatory,
since a gate cannot wait for what it will never see. A read tier wants the opposite
disposition: a channel its recorded past cannot verify must mean *do not serve*, because
serving anyway is a silent read-your-writes violation. `coverageOf` therefore checks every
channel a token names and reports the unverifiable ones as gaps. The error is always in the
conservative direction — a refusal to serve, never a stale serve.

The consequence to plan for: a read model that records only its own delivered coordinates
can never cover a token naming a channel it does not receive. A client that writes to
`orders` and reads a model fed only by a downstream `events` topic fails coverage forever,
not transiently. Scope the pattern to read models that receive the channels clients write
to, or carry upstream coordinates forward in message payloads at the application level.

## What the token does not replace

The token protects the client's session; only causal delivery protects the projection's
value. A projector applying updates out of causal order can leave a row whose recorded past
covers a token while its value is wrong. Read models therefore want to be Parsley
processes — which also puts row and recorded past in one `Store`, committing atomically in
the Kafka transaction.

At the handler seam, record the delivery's own coordinate (`delivery.partition()`,
`delivery.position()`), merged per row or into a model-wide clock. Do not derive a
recorded past from `frontierSnapshot()`: the frontier advances on receipt, before
delivery, so it can name coordinates whose effects are still held back — telling a client
its write is visible when it is not.

## Operating it

Build service-to-service first. Between Kafka participants a producer stamps
`parsley.causes` directly and the ceiling is Parsley's own metadata budget, so the token
size question never binds; at the HTTP boundary the same frontier must fit a header or a
cookie, which is what makes the browser-facing form the constrained one.

A token is untrusted input even when this application minted it. Bound the entry count and
encoded width, reject channels the tier does not recognise, and bound positions against a
known-live upper bound: a validated token becomes causes on a produced record, and every
downstream receiver of those channels holds messages until they settle
([failing closed](failing-closed.md)), so an over-broad token affects liveness for everyone
behind the topic.

Client-facing tokens should be encrypted, not merely signed — offsets leak throughput and
topic ids leak topology — but the encrypted form stops at the gateway: the engine decodes
only the frozen grammar, so the gateway decrypts, bounds, and stamps a plain header.

Give tokens a TTL well short of retention. A pruned coordinate can otherwise never be
covered again, and on mirrored estates no log-start evidence ever arrives for foreign
topics, so expiry is the mitigation that works everywhere. The token also only ever grows;
expiry is what stops a long session converging on the size of the topology.
