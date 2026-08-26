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

The consequence to plan for: a recorder that sees only its own delivered coordinates can
never cover a token naming a channel it does not receive. That is the situation behind the
handler seam, which filters the causes header before application code sees it: a client
that writes to `orders` and reads a model fed only by a downstream `events` topic fails
coverage forever, not transiently. A projector consuming raw records does not have this
problem — the header carries the transitive closure — which is one reason the
database-hosted shape below is the right one for a serving tier.

## The token does not replace delivery

The token protects the client's session; only causal delivery protects the projection's
value. A projector applying updates out of causal order can leave a row whose recorded past
covers a token while its value is wrong. The projector is therefore a Parsley process — and
because clients query a database, not a Streams store, it wants to be a process hosted on
the database's technology rather than on Kafka Streams.

## A projector on the database's transaction

Let the database be the host: projection rows, clock rows and consumed positions all commit
in one database transaction, and the projector resumes from the positions the database
holds. That is the Kafka Streams host's shape with the atomicity domain moved — one commit
domain, so the dual-write discipline an external database usually demands never arises. The
core was built to permit exactly this: the engine is host-independent, runs over an
`OrderingStore`, and the delivery decision is a pure function. The library ships no
database host today; the simulation harness is the proof that a non-Streams host honouring
the specification's Host obligations runs real engines.

Consuming raw records, the projector sees the `parsley.causes` header the seam withholds,
so its clock is the transitively closed delivered past: per delivered message, merge the
own coordinate and every carried pair — the same fold the engine's `markDelivered`
performs. In exchange the host owes what Kafka Streams was providing: `read_committed`
consumption, fencing of zombie writers (a partition epoch plus monotone guards), and
position facts for pruning. Do not substitute `frontierSnapshot()` from behind a seam: the
frontier advances on receipt, before delivery, so it can name coordinates whose effects
are still held back — telling a client its write is visible when it is not.

## Clocks are per partition; collections gate on the meet

Keep one clock row per consumed partition, written in the same transaction as that
partition's rows. Each partition's projector is the single writer of its own clock — no
shared hot row, and ownership moves with partition assignment.

A single-entity read gates against the owning partition's clock. A collection spanning
partitions cannot gate on rows at all: its failure mode is absence — the row a write
should have created is not there yet, and a missing row has no past to check — so it gates
on what the whole model has applied. The sound aggregate is the pointwise **minimum**
across partition clocks, not the maximum. A max-merged superset clock reports the most
advanced partition while a collection's correctness is bounded by the least advanced: one
caught-up partition hides every laggard, and the gate serves rows that do not yet reflect
the write — a stale serve, the failure the pattern exists to prevent.

One query computes the meet, because a terminal partition's own coordinates have exactly
one contributor, where minimum and maximum coincide:

```sql
SELECT channel, MIN(position), COUNT(*) FROM partition_clock GROUP BY channel
```

treating an upstream channel counted on fewer than all partitions as absent, which fails
closed. The meet also has the property the join lacks: stale is safe. A lagging meet only
blocks a little longer, never serves stale data, so it may be materialised asynchronously
where read volume demands it; a join must never be materialised as a gate.

The meet's liveness cost is the idle partition. Inherited entries advance only when a
partition applies a message; every emission carries the emitter's whole frontier, so active
partitions converge on their next message, but a silent partition pins the meet at its
last-seen frontier. Bound the wait and fail with retry-after, or have the application fan
a periodic no-op event across every terminal partition — the pipeline stays clockless, and
the heartbeat carries real causal evidence rather than a wall-clock guess.

Refresh a collection read's token from the meet it was gated on, plus the own coordinates
of the partitions that contributed rows — never from the superset, which inflates the
client's future demands with coverage the read did not prove.

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
