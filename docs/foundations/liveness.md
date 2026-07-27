# Liveness without gossip

Parsley appends no protocol records to any topic. This page develops why that is possible: why
every held record is eventually released, why nothing deadlocks, and what closed the one
genuine gap that in-band protocol traffic covers in other designs.

## What a gate can wait on

The delivery gate waits on exactly one thing: the local frontier of channels the node itself
consumes. It never consults another node's advertised knowledge — a peer's claim that a record
was delivered *there* is not proof it was delivered *here*. So the liveness question reduces
to: does every frontier eventually cover every offset a claim can name?

Three facts answer it:

**Claims name really-appended offsets.** Every source of a claim — the frontier (delivered
records), acknowledgement folds (own appended sends), end-offset seeds (append-time
snapshots), and clocks folded from received records (inductively the same) — is an
append-time fact. There is no such thing as a claim on an offset that does not exist.

**Every appended offset reaches every consumer of its channel, as a record or as a skip.** A
business record arrives and is delivered in queue order. A transaction marker or aborted
record is consumer-skipped, and the skip is observable: the next fetched record's offset jumps
(bridged at receive), or — when nothing follows — the consumer's position advances past it
anyway.

**Custody needs no side channel.** Knowledge dissemination beyond business paths would matter
only if some gate waited on it, and no gate does. Stamps stay sound along exactly the paths
causality travels (the induction in
[the delivery gate](delivery-gate.md#why-the-ignore-branch-is-sound)).

## Position-advance bridging

The one genuine wedge in the receive-side story is the trailing skip. Every EOS commit writes
a marker; an end-offset seed can mint a claim naming a marker's offset; and if the producer
never appends again, no later record arrives to reveal the skip. A downstream frontier parks
one short of the claim forever.

The information that unwedges it already exists at every consumer: a `read_committed`
consumer's **position** advances past trailing markers and aborted batches even when the poll
returns no records. A position of `p` is proof that everything below `p` was fetched or
consumer-skipped. The host reports it through one method — `positionAdvance(channel, p)` —
and when the channel's hold queue is empty the frontier rises to `p − 1` and the cascade runs.

That method is the entire liveness mechanism. What it replaces in a gossip-based design: null
messages occupying sink offsets, an emission rule for quiet deliveries, a relay rule and its
cycle-quiescence argument, protocol-record filtering at every plain consumer, trigger
timestamps to protect downstream stream time, and retention sizing coupled to protocol
traffic. None of that has a remaining consumer once gates wait only on local frontiers and
skips are observable from position.

## Why over-claims cannot deadlock

Claims can exceed real causality: the init-time end-offset seed claims every offset appended
to a sink before the node started, including sibling producers' records, and folded clocks
propagate such claims onward. Over-claims are individually harmless — they name real offsets,
so they delay rather than reorder. The remaining worry is a cycle of waits: record X held
waiting on Y while Y is held waiting on X, each wait justified by some claim.

No such cycle can exist, because **every wait edge points from a later-appended record to an
earlier-appended one**:

- A head-of-line wait: the head was appended (and received) before its successors on the same
  channel.
- A real-causality dependency wait: a cause is appended before anything emitted after
  delivering it — append precedes delivery precedes emission precedes the effect's append.
- An over-claim dependency wait: an end-offset snapshot taken at time T covers only offsets
  appended before T, while every record whose stamp carries the resulting claim is appended
  after T — the claim entered the system at T and propagated causally, and causal propagation
  moves strictly forward in append time.

A wait cycle would therefore require some record to be appended strictly before itself. The
wait graph is acyclic; a blocked record always has a wait chain ending at a record that is
deliverable now, or at a skip the next position advance reveals.

## The sequence-claim caveat: late joiners

Sequence claims (a sender's synchronous claims over its own sends — see
[the stamp](../design/architecture.md#the-stamp)) add one wait kind the append-order argument
does not cover on its own: a sequence claim is resolved by *delivering* the claimed sender's
record, and a consumer whose baseline sits **above** that record can never deliver it. For a
from-the-start or committed-position consumer this cannot happen — the claimed record is
always at or above its position. The exposed case is a **late joiner** baselining at the log
end while a sequence-form claim still circulates in some custody clock and the claiming
sender never writes that partition again. The verification suite demonstrates the wedge
directly (an at-latest joiner against a never-normalised claim) and its absence for a
from-the-start joiner.

Operationally: baseline late joiners at the last stable offset (which
`read_committed` offset listing provides) rather than the log end — every claim minted by an
open transaction sits above the LSO, so it resolves — and treat a held record whose sequence
claim names a sender with no post-baseline presence as the detectable signature of the
remaining window (a retired sender's claim frozen in custody). Offset claims have no such
window, which is why acknowledged sends upgrade to them.

## What enforces this empirically

The argument above is prose; the simulator makes it load-bearing. Every simulated world must
**drain** — reach a state with no fetchable record unfetched, no acknowledgement undelivered,
no node down — within its step budget, and at drain the oracle checks **completeness**: every
business record above a node's baseline on a consumed channel was delivered there. A wedged
frontier, a missed bridge, or a livelock fails one of the two on some seed. The suite asserts
that position advances actually fire (anti-vacuity), so the mechanism is exercised, not
merely present. See [verification](../design/verification.md).
