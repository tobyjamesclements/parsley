# Design

This document records the architecture of Parsley and the reasoning that connects it
to `SPEC.md`. It is written for a reader who knows Kafka and Kafka Streams but has not read this codebase. Decisions the
specification left open are logged in `DECISIONS.md`; this document explains how the pieces fit.

Terms of art used here name the following (Structural 6): *happened-before* is Lamport's causal order over process
events (Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System", 1978); *causal delivery* is
delivery respecting that order, as defined in `SPEC.md` — if A is a cause of B, no process that delivers both delivers
B first; the per-channel dependency summary used here plays the role a *vector clock* (Fidge/Mattern) plays in
process-indexed causal broadcast, but is indexed by channel, not by process, because Structural 11 forbids process
identity in metadata.

## 1. The model

`SPEC.md` maps onto Kafka as the Assumptions permit (each taken assumption is logged in `DECISIONS.md`):

| Spec term | Kafka realisation |
|---|---|
| Channel | topic-partition, identified by (topic ID, partition) so a recreated topic is a different channel |
| Position | record offset |
| Message | record: key bytes, value bytes, headers, offset |
| Process | a Kafka Streams *task* of the Streams application built for one declared process |
| Step | one Kafka Streams EOS transaction (commit interval) |
| Earliest retained position | log-start offset |
| Host read-position report | the record feed itself (in-order per partition) plus the group's committed offsets |

**One declared process = one Kafka Streams application** (its own `application.id`, hence its own consumer group).
This is what lets two declared processes receive the same channel — a single Streams application cannot register the
same topic in two source nodes. Each process's application contains one subtopology: byte-level sources for its
received topics, one processor node, byte-level sinks for its send topics. Kafka Streams then induces one task per
partition group; that task is the spec's *process*, and its received-channel set is partition *i* of each received
topic (Assumption 14).

## 2. Causality: what the metadata says

Per Structural 11, a message's metadata expresses its causes solely as (channel, position) pairs. The engine keeps, per
process, a **causal frontier**: a map `channel → highest position known to be a cause of this process's subsequent
sends`. One pair per channel is enough because replacing two pairs on one channel by the greater position is
compression, not discarding (Structural 13), and expressing a position *at or above* the cause's is expressing the
cause (Terminology: *Express*).

The frontier grows from exactly two events:

1. **Delivery**: when the process delivers message M from channel c at position p, merge `(c, p)`. Delivery of M
   happened-before every later send, so M is now a cause of them.
2. **Receipt**: when the process *receives* M (even if M is held undelivered), merge every pair in M's metadata.
   Happened-before passes through receipt, so M's causes are already causes of this process's subsequent sends
   (Terminology: *Cause*; Structural 15 requires exactly this).

Sends do **not** enter the frontier: under the spec's definition, causality flows only through *delivery-before-send*,
so a process's own send is not a cause of its later sends until someone (possibly itself, via a self-channel) delivers
it and that delivery flows back. This also discharges Structural 14 by construction: every position in the frontier was
assigned before the moment of sending, and offsets are assigned in append order, so a message sent to a channel the
process also receives from carries only dependencies strictly below its own offset.

Pruning (Structural 13): a frontier pair `(c, p)` is dropped exactly when `p` is below c's log-start offset, or c's
topic no longer exists (its topic ID no longer resolves). Log-start facts can be stale; stale facts are lower bounds,
so staleness only delays pruning, never over-prunes.

**Growth law (Operational 5).** The frontier's size follows the causal graph, not this process's declaration:
receipt merges every metadata pair, including channels the process never receives — necessarily, for downstream
re-expression (Structural 15) — and every emission carries the whole frontier. Steady-state size approaches the sum
of partition counts over the transitive upstream closure: each re-keying hop multiplies exposure by a partition
count, and output topics carry metadata their external consumers have no use for. An entry is released only below
its channel's log start or at channel death, and it holds the channel's *highest* causal position — which outruns
retention while the channel is in use — so pruning collects idle channels rather than bounding active ones. Anyone
who can produce to a received channel can grow the frontier (a coordinate naming an unreceived, retained channel
lodges and propagates). At 28 bytes per entry the encoding reaches Kafka's default 1 MiB record ceiling near 37,000
entries, where the failure would be a permanent stop inside the producer with no parsley diagnosis; the metadata
budget (`ParsleyConfig.metadataBudgetBytes`, default 256 KiB) fails closed attributably first, and each process's
current frontier size and encoded bytes are logged every facts round and surfaced at 80% of budget.

### Wire format

Causes travel in a single Kafka record header, `parsley.causes` (documented and frozen in `docs/METADATA.md`):
a version byte, an entry count, then per entry topic ID (16 bytes), partition (int32), position (int64), big-endian.
The reserved key plus version byte make the metadata distinguishable by construction from application headers
(Structural 5); emissions attaching application headers with the reserved prefix are refused. The record's key and
value are exactly the bytes the application's serdes produced — nothing wrapped or prefixed (Safety 4), so any
consumer with the application's codecs alone, including Confluent Schema Registry serdes, decodes them (Safety 5).

* Absent header ⇒ no causes ⇒ deliverable immediately (Safety 6).
* Present but undecodable (bad version, truncation, trailing bytes) ⇒ fail closed (Safety 7): the process throws and
  stops; see §6.

## 3. Deliverability: the settled frontier

Per received channel c the engine tracks:

* `fedUpTo(c)` — highest position such that every position ≤ it has either been fed to this process as a message or
  will never arrive as one. Advanced by two inputs, both reports under Host obligation 2:
  * **receipt** of a record at offset o on c ⇒ `fedUpTo(c) := max(fedUpTo(c), o)` — within an execution the host feeds
    each partition in offset order, and Kafka's per-partition order means every earlier offset was either already fed
    or will never yield a record (aborted transactions, control records, compaction);
  * **read-position facts**: the group's committed offset n for c ⇒ `fedUpTo(c) := max(fedUpTo(c), n − 1)` — a
    committed position asserts everything below it was fed or never will be. This is what advances past a *trailing*
    run of positions that never arrive (Liveness 3), with no reliance on elapsed time: it is the substrate's report,
    not a timeout.
* `held(c)` — the hold-back buffer: received-but-undelivered messages of c, in position order, persisted (§5).

From these, **settled(c)** is derived, not stored:

```
settled(c) = held(c) empty ? fedUpTo(c) : head(held(c)).position − 1
```

Every position ≤ settled(c) is *delivered or will never arrive*: positions below the buffer head that were fed are
delivered (the head is the lowest undelivered), and gaps are covered by the fed-or-never meaning of `fedUpTo`.

**The decision** (Structural 7) is the pure static function `Deliverability.decide(causes, receivedChannels,
settled)`: message M is deliverable iff for every pair `(c, p)` in M's causes, either `c ∉ receivedChannels`
(Liveness 4: order with messages this process will never deliver is vacuous — this also covers dead incarnations of
recreated topics, whose topic ID no longer matches any received channel), or `settled(c)` is defined and `p ≤
settled(c)`. It reads no clock, no timestamps, nothing outside its arguments; the engine calls this same unit for
every delivery, and it is public and callable without any host.

Safety 3 (FIFO per channel) is enforced structurally: only the *head* of each channel's buffer is ever offered to the
decision. Delivery order across channels is a deterministic drain: scan received channels in a fixed total order,
deliver every deliverable head, repeat until a full pass delivers nothing. Determinism makes replay after restart
reproduce the same order from the same state (2-safety 2).

First receipt baseline (Structural 12): before anything is known of c, `settled(c)` is undefined and any dependency on
c (if c is received) holds the message; the initial committed offset established at first start (§7) initialises
`fedUpTo(c)`, so positions below the first receipt are treated as already satisfied.

Duplicate feeds: a record at offset ≤ `fedUpTo(c)` and not in the buffer was already delivered in a committed step —
the host re-feeds exactly the records whose read positions were not committed (Host obligation 5) — so it is dropped,
not redelivered (Safety 2). An aborted step rolls `fedUpTo` back with everything else, so a redelivery after abort is
not a duplicate.

## 4. The engine and its purity boundary

Everything above lives in `ProcessEngine`, which is host-independent: its inputs are `onReceive(channel, position,
headers, payload)`, `onFacts(committedOffsets, logStarts, deadChannels)`, and `deliveryCompleted(...)`; its outputs
are deliveries to perform and the causes header to stamp on each emission. It talks to durable state only through the
`OrderingStore` interface (get/put/delete/range-scan over bytes). The Kafka Streams adapter implements `OrderingStore`
over a Streams `KeyValueStore`; the test simulator implements it over a rollbackable map and drives the *same engine*
through crashes, aborts, gaps, trailing runs and interleavings, checking every run against a happened-before oracle
built outside the engine. Structural 1 holds by construction: the engine sees only received messages, its own state,
and position facts the substrate reports (Assumption 15 makes such queries not-exchange).

## 5. Ordering state

One reserved Streams `KeyValueStore<Bytes, byte[]>` per process, named with the reserved prefix `__parsley.`, keys:

| Key | Value |
|---|---|
| `f` + channelId | `fedUpTo` (int64) |
| `c` + channelId | frontier position (int64) |
| `h` + channelId + position | held message blob: key, value, headers, decoded causes, timestamp |

`channelId` is 20 bytes (topic ID + partition, big-endian), so a prefix range scan yields one channel's held messages
in position order. The store is persistent and changelogged; under EOS its content is committed atomically with the
read positions consumed and the messages sent (Host obligation 3), which is what makes held messages survive restarts
(Liveness 5) even though Streams commits read positions past records we have buffered rather than delivered.
Application state cannot alter ordering state (Structural 8): application declarations may not use the reserved
prefix, and the seam (§8) exposes only declared application stores.

## 6. Failing closed

Fail-closed events throw `ParsleyFailClosedException` out of `process()`, which fails the task's step: the EOS
transaction aborts, nothing is delivered past the failure, and the process stops and will re-fail on restart until an
operator intervenes. The blast radius is the whole process, not just the affected channel, deliberately: after
receiving a message whose metadata cannot be decoded, *every* subsequent send of the process would risk under-
expressing causes (Structural 15 — causes known only from received-undelivered metadata must be re-expressed, and an
undecodable header hides exactly those), which would let a downstream process violate Safety 1. Stopping the process
entirely is "at minimum on the affected channel" (Terminology: *Fail closed*) and never weakens the guarantee.

Triggers:

* metadata present but undecodable (Safety 7);
* read position at or resuming below the log-start offset — discarded positions cannot be assumed empty (Safety 8).
  Enforced twice: the main consumer runs with `auto.offset.reset=none` so an out-of-range position kills the task
  rather than silently jumping (initial positions are pre-committed at first start, §7), and the engine independently
  fails when a log-start fact exceeds `fedUpTo + 1`;
* an execution whose declaration removes a channel with held messages, including a held channel whose topic ID no
  longer exists (Structural 16);
* an emission naming a channel outside the declared send set (Structural 19) or carrying a reserved header;
* application payload that its own serde cannot decode at delivery (delivering past it would violate Safety 3).

## 7. Runtime: wiring into Kafka Streams

`Parsley.start()` for each declared process:

1. Resolves topic IDs for the declared topics (AdminClient) and fails if a received topic is missing.
2. Reads the group's committed offsets; for received partitions with none, pre-commits the declared initial position
   (earliest = log-start, latest = end) — through a generation-fenced group membership, never an admin alter, so a
   stale paused bootstrap can never overwrite a newer lifetime's offsets (D48). This gives `auto.offset.reset=none`
   a defined starting point and realises the first-receipt baseline; on later starts, existing commits win (a
   channel that rejoins the set resumes, never re-enters delivered past — Structural 16, backed by the `fedUpTo`
   dedupe even if group offsets expired; a missing offset with prior state re-establishes earliest, never the
   declared LATEST — D36). Prior state also pins the task width: the ordering store's changelog cannot change
   partition count, so a width-changing declaration refuses with its remedy rather than dying in the host's
   internal-topic validation (D49).
3. Builds the topology (byte sources → processor → byte sinks) and starts `KafkaStreams` with
   `processing.guarantee=exactly_once_v2`; `isolation.level=read_committed` follows, and user configuration may not
   override either — overrides are rejected, and neither `Topology` nor raw configuration is exposed, so no documented
   operation can run the topology without EOS (Structural 9, Substrate 3).

Inside the processor, a wall-clock punctuator periodically ingests position facts — committed offsets, log-start
offsets, topic existence and identity (a name resolving to a different id is affirmative recreation evidence; an
unknown id is corroborated against its last-known name and time-debounced, and an authorization denial is never
death — D44) — and feeds them to `engine.onFacts(...)`, then drains. The gathering itself runs on one background
thread per runtime; the punctuator snapshots the inputs, applies each completed round exactly once on the stream
thread, and never blocks on the cluster (every fact is a per-position lower bound, so a round applied one interval
late releases and prunes exactly what a fresh one would — D54). This punctuator is internal plumbing
for ingesting the host's read-position reports (the only transport Kafka Streams offers for them); it is not an
application-facing timer, delivers nothing that was not received from a channel, and the deliverability decision it
triggers remains the pure unit of §3 — time never appears among its inputs. The public API offers no timers or
scheduled callbacks (Structural 10). Zombie safety: facts are monotone, and any delivery a superseded execution
performs on stale state sits in a transaction the host will fence and abort (Host obligation 6), so it "has not
occurred".

## 8. Public API and the seam

Declaration (Structural 17/18): `Channel.of(topic, keySerde, valueSerde)` gives a typed `Channel<K, V>`;
`ProcessDefinition.named(name).receives(channel, handler).sends(channel).store(storeDef)` declares a process; several
processes form an application. Any arrangement is expressible, including cycles and self-channels (Structural 2).

The seam (Structural 3) is per received channel:

```java
Effects handle(Delivery<K, V> delivery, StateReader state)
```

`Delivery` carries the delivered message (key, value, channel, position, headers). `StateReader` is a read-only,
typed view over the process's declared application stores (ordinary Streams RocksDB stores — host state facilities,
not exclusively owned; Structural 8). All effects return through `Effects`: typed sends (`send(channel, key, value)`,
statically typed per channel — Structural 4) and state writes (`put(store, k, v)` / `delete(store, k)`), which the
implementation applies within the step. The logic receives no other capability — no context, no producer, no clock.
Sending never blocks on deliverability of the message sent (Liveness 2): emissions are stamped with the current
frontier and forwarded immediately within the step.

## 9. Testing strategy

1. **Pure core**: codec fuzz/round-trip tests; decision-unit table tests; engine unit tests.
2. **Simulation**: a simulated substrate + host honouring the Host obligations drives many engines over randomised
   topologies, interleavings, gaps, aborted-transaction runs, crashes, restarts and read-position reports, seeded and
   deterministic. An oracle tracks real happened-before *outside* the engine and asserts causal order, no duplicates,
   FIFO per channel, and quiescent liveness (everything received is eventually delivered).
3. **Sabotage meta-tests**: the same suite is run against deliberately broken engines (dependency check disabled,
   FIFO hold disabled, duplicate delivery on refeed, undecodable-metadata-treated-as-absent, ...) via a test-only
   hook, asserting the oracle *fails*. This is the evidence that the tests would catch a violation, per
   `EVIDENCE.md`'s standard.
4. **Streams wiring**: TopologyTestDriver tests for header format on the wire, byte-exact key/value pass-through,
   Schema-Registry-format serdes, punctuator fact ingestion via an injected facts source, store persistence across
   driver restarts.
5. **Integration**: embedded KRaft broker tests for EOS commit/abort behaviour, restart with state restore,
   aborted-transaction gaps and trailing runs, log truncation (Safety 8), and a plain read_committed consumer
   decoding output with application serdes alone.
