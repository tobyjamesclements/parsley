# Assessment of the candidate implementation

Findings to resolve, written for the implementing session. Every entry here is established: its premise was
re-verified on this tree, and its demonstration — a mutation, a broker probe, or a traced scenario — was run
here. Read this alongside `SPEC.md`; nothing else is required context.

**Baseline.** The suite on this tree is 380 tests, green, 2:59 under plain `mvn test`, at commit 18bf78b.
Every mutation result below was judged against that baseline by a full `mvn test` run.

**Grading.** Two tiers, marked on every entry.
- **[MUST]** — the entry asserts a violation of a MUST clause of `SPEC.md`, or of the suite's obligation to
  catch one (`EVIDENCE.md`'s own standard: a test that stays green when the behaviour breaks is worse than an
  empty cell). These are binding: every one ends resolved.
- **[SHOULD]** — a desired characteristic, most citing the specification's Operational section. Follow it, or
  record the deviation and rationale in `DECISIONS.md`; that record is then the deliverable.

Where an entry requires a *decision* rather than a patch, it says so; the decision plus rationale in
`DECISIONS.md` is the deliverable there.

## 1. Production-code defects

### 1.1 [MUST — Assumption 2, Safety 8, Liveness 1, 2-safety 2] Mid-run topic recreation feeds the new incarnation under the old channel identity

Channel identity is topic-id-based everywhere except the feed path, which is name-based and immediate:
sources subscribe by name (`ProcessTopology.build`), `ParsleyProcessor.init` maps name → `ChannelId` from the
partition snapshot taken at `start()` (`ParsleyProcessor.java:74–81`), and every identity guard is
startup-time or rounds-delayed — the name-binding refusal runs only at engine construction
(`ProcessEngine.java:104–116`), the dead verdict needs three facts rounds (`AdminFactsSource.java:47`), and
`resolveTopics` runs once. Delete and recreate a received topic while the process runs, and the new
incarnation's records are fed to the engine as the old channel.

Demonstrated on a real broker (Kafka 3.9.1, embedded KRaft): echo process on `rr-in`, `factsInterval` 60 s;
deliver offsets 0–4 (committed offset 5); without stopping the app, delete `rr-in` (gone in ~40 ms),
recreate it, produce v0..v7. Observed: the stream thread never dies and `healthy()` stays true throughout —
no `NoOffsetForPartitionException` (the consumer's in-memory position survives), no epoch/truncation error
(both incarnations at leader epoch 0). The coordinator briefly expunges the group's offsets, then the live
app re-commits offset 8 against the new topic. **v0..v4 — new messages at new-incarnation offsets 0..4 — are
silently dropped as duplicates of the old channel's session floor and are never delivered by anything**;
v5..v7 are delivered, and their emissions carry `parsley.causes` naming the **old, dead topic id** at
positions 5..7, propagating the wrong identity downstream.

Resolution must confront the window between deletion and whatever guard fires: the feed path must not adopt
records for a channel whose identity it has not confirmed (the fetch-by-topic-id era makes the record's real
topic id knowable), or the process must fail closed when identity cannot be confirmed. Design together with
1.2 and 1.3 — all three are the same identity machinery.

### 1.2 [MUST — Structural 13 → Safety 1] The dead-channel verdict rests on evidence that cannot distinguish death from denial or staleness, and one shared streak serves every task

A wrong death verdict prunes frontier causes — against Structural 13's "MUST NOT discard any other cause" —
and surfaces as under-expression at downstream processes with no local failure. Three legs, one fix:

- **The evidence is ambiguous, and provably so on a real broker.** `AdminFactsSource.describeByIds`
  (`AdminFactsSource.java:221–246`) treats `UnknownTopicIdException` as "definitive: the topic no longer
  exists". Demonstrated with a real `StandardAuthorizer`: a DENY ACL on Describe for one frontier topic makes
  the broker answer describe-by-id with `UnknownTopicIdException` (it masks authorization denial to avoid
  leaking existence), while describe-by-**name** answers `TopicAuthorizationException`. After three rounds
  (~750 ms at the probe's 250 ms interval) the denied topic was declared dead and its frontier pair pruned;
  emissions after the confirmation no longer carry it, **and removing the ACL does not resurrect it** — a
  transient permission change permanently discards a live cause, silently, while `healthy()` stays true.
  Probe recipe: `KafkaClusterTestKit` with `authorizer.class.name=org.apache.kafka.metadata.authorizer.
  StandardAuthorizer`, `allow.everyone.if.no.acl.found=true`; inject a frontier coordinate for topic X via a
  hand-built `parsley.causes` header (`CausesCodec.encode`/`Causes.of` are public); add DENY DESCRIBE on X
  for `User:ANONYMOUS`; decode emission headers before and after.
- **No corroboration against the topic's last-known name.** The same cluster still resolving the name — or
  answering `TopicAuthorizationException` for it — is evidence of staleness or denial rather than death. The
  by-name path distinguishes exactly the case the by-id path cannot. (`topicNamesById` already exists in the
  class and is currently write-only dead state.)
- **The debounce is pooled across tasks.** `unknownStreak` is keyed by topic id alone
  (`AdminFactsSource.java:53`) while one `AdminFactsSource` serves every task of the application
  (`ParsleyRuntime.java:68`): at N tasks the three-round window shrinks to ~`interval × 3/N`, and one task's
  live round resets a streak another built.

Also fold in: `unknownStreak` entries for dead ids are never removed (a dead id can never describe live
again), and `topicNamesById` has no removal path at all — both grow without bound under topic churn or
injection; and whether the same masking on a *received* topic drives `fedUpTo` to the end-of-channel
sentinel (releasing dependents against positions that may still arrive) is an **open question the fix must
settle** — the probe above extends to it directly.

Note for `DECISIONS.md`: D40's claim that a live id describing unknown for three rounds is
"substrate-breach territory" is refuted — an ACL change is routine operations.

### 1.3 [SHOULD — Operational 6] Recreation across a restart with held messages is misdiagnosed as a declaration change

`refuseStrandedHeldMessages` builds its "declared" channel set from the **current** resolution's topic ids
(`ParsleyRuntime.java:180–189`). Stop with held messages on topic T, delete and recreate T under the same
name, restart with an unchanged declaration: the held entries carry the old id, "declared" contains only the
new id, and start() refuses with `CHANNEL_REMOVED_WITH_HELD_MESSAGES` — "which the new declaration no longer
receives" — though nothing was removed. The accurate diagnosis and remedy exist (`CHANNEL_IDENTITY_CHANGED`,
"reset the process's state and group offsets deliberately", `ProcessEngine.java:104–116`) but the runtime
scan throws before any engine constructs. Without held messages the accurate check fires. Safety is
preserved either way (both refuse); the defect is the wrong reason and remedy.

### 1.4 [MUST — Safety 9] A dead channel's causes are discarded while a process can still hold an undelivered message from it

`ProcessEngine.onFacts` prunes a frontier pair the moment its channel is reported dead
(`ProcessEngine.java:287–298`), so subsequent emissions express nothing about that channel. A process still
holding a received-but-undelivered message from it then delivers the effect before the cause. Safety 9 makes
the detected case binding: a process retaining a received-but-undelivered message from a channel that no
longer exists must preserve that message's place in causal order or fail closed — today it does neither
(`fedUpTo` jumps to the end-of-channel sentinel and later messages on other channels sail past the held one).

Engine-level demonstration (no broker; ran green as four probe tests this session):
1. Engine A receives X: deliver X@5 → `causesHeaderForEmission()` decodes to {X→5}; then
   `onFacts(dead={X})` → header decodes empty.
2. Engine B receives X and Y, holds X@7 (give it a cause at X@8 or above — note the settled clamp is
   head−1, so a cause at X@6 against a held head X@7 is already satisfied); feed Y@0 with an empty header
   (what A now emits): `nextDeliverable()` offers Y@0 while X@7 stays held — effect before cause.
3. Counterfactual: feed Y@0 expressing {X→7} instead: blocked before **and after** `onFacts(dead={X})` —
   the hold-back clamp keeps settled(X) at 6 even with `fedUpTo=+∞`. Retention of the pair preserves order
   even at a dead channel.

The undetectable half — the effect arriving where the dead channel's message was never received — is excused
by Assumption 17 (deletion hygiene is the operator's duty), mirroring how D26 treats Assumption 13. Choose
the mechanism freely (fail closed on dead-verdict-with-non-empty-holdback is the minimal one; expression
retention is sound but grows metadata at the origin — see constraint 4.1) and record it.

### 1.5 [MUST — Fault model 2, 2-safety 2, Safety 8] The bootstrap can overwrite a newer lifetime's committed offsets

`commitInitialPositions` lists the group's offsets, computes what is missing, and alters unconditionally —
no re-validation between list and alter (`ParsleyRuntime.java:206–238`). A bootstrap paused between the two
(the arbitrary-duration pause Fault model 2 requires tolerating) resumes after a newer lifetime has
bootstrapped, processed, committed and stopped; the group is empty again, so the alter succeeds and
overwrites. With a LATEST channel the stale bootstrap commits the current log end, silently skipping
retained unread messages — a restart observable in what is delivered.

Broker premise demonstrated this session (plain clients, embedded KRaft): group commits offset 10 and
empties; `alterConsumerGroupOffsets` to 2 **succeeds** and sticks. Counter-leg: with a subscribed, polling
member in the group, the same alter fails with `UnknownMemberIdException` and the offset is untouched. That
is the fencing direction Kafka offers: commits made through group membership are generation-fenced;
admin alters are not. Kafka has no conditional alter, so some window is irreducible by that route — the fix
must change the mechanism, not shrink the window. This window recurs at **every** site that ever pre-commits
offsets (today there is exactly one); any new pre-commit site must inherit the fix.

### 1.6 [MUST — Structural 16, Structural 2] A legal shrink of the received-channel set bricks the application with advice that would destroy the ordering store

Structural 16 permits a declaration change that shrinks the task set (when nothing is held on the removed
channels), but nothing on this tree reconciles the ordering-store changelog's historical partition count
with the new task-set width. Demonstrated end-to-end this session: process receiving `mp-in` (3 partitions)
+ `mp-single` (1); all messages delivered; restart declaring only `mp-single`. `start()` **succeeds** in
~0.6 s; the app reports `healthy()==true` for ~42 seconds while delivering nothing; then the streams client
dies in the partition assignor with `StreamsException: Existing internal topic
mp-mp1-__parsley.ordering-changelog has invalid partitions: expected: 1; actual: 3. Use
'org.apache.kafka.tools.StreamsResetter' tool to clean up invalid topics before processing.` No parsley
diagnosis at any point; a supervisor restart recurs identically; and following the advised remedy deletes
the ordering store — the state Structural 16 exists to protect.

**Constraint any fix must respect:** today the one-shot, snapshot-bounded stranded-holds scan
(`refuseStrandedHeldMessages`, ends snapshotted once at `ParsleyRuntime.java:171`) is masked by this
crash-loop: a held entry committed after the snapshot on a shard the shrunken task set does not own is
missed by the scan, no producer of the new execution ever fences that shard's writer, and the engine-level
refusal cannot run there (no task) — but the width error stops the app anyway, and the *next* restart's
fresh snapshot catches the entry. A width fix that lets the shrunken app run **re-opens that window as a
silent Structural 16 violation**: it must re-establish the guard (fence or re-scan after the point where
late commits can no longer land). Design 1.6 and that guard together. Constraint 4.3 (changelog re-reader
failure modes) applies to any re-scan.

### 1.7 [SHOULD — Operational 6, Operational 2] Mid-run partition expansion stops the application with a foreign diagnosis

Partition counts are snapshotted once (`resolveTopics`) and baked into the topology closure. A partition
added mid-run either fails the rebalance in Streams' width validation (task count grew) or, for an expanded
narrower topic with the task count unchanged, dies with the consumer's `NoOffsetForPartitionException`
(`auto.offset.reset=none`; the new partition was never pre-committed) before any record reaches `process()`.
Neither failure names a parsley concept or remedy. A full restart handles expansion correctly
(re-resolution + EARLIEST pre-commit for the new partitions) — that boundary is currently undocumented.
Give the failure a parsley diagnosis naming the restart remedy, or support expansion; either way record the
choice.

### 1.8 [MUST — Structural 9 (→ Safety 1/3/4/7); Structural 20] Client interceptors and the timestamp extractor are configurable

`ParsleyConfig`'s refusal sets (`ParsleyConfig.java:16–34`) cover neither `interceptor.classes` (any prefix)
nor `default.timestamp.extractor`; `extraProperties` flow unfiltered into the Streams properties, the admin
client, and the changelog-reader clients. Two documented public paths to safety violations:

- A producer interceptor's documented purpose is mutating records in `onSend`: one that strips or replaces
  the `parsley.causes` header makes every emission read as cause-free downstream (Safety 6 then *mandates*
  immediate delivery), yielding effect-before-cause at a correctly-behaving downstream process, undetectable
  on either side — below every safety criterion that rides on the header.
- A timestamp extractor in the `LogAndSkipOnInvalidTimestamp` style converts a received message into a
  documented log-and-skip drop whose read position still advances — a silent never-delivered skip, the same
  shape as the continue-style handlers D37 already refuses.

D37's own rule — "the owned set is the configs that bear on the guarantees" — makes these owned. Add both to
the refusal set and record the addition in `DECISIONS.md` (the completeness counterargument — extraProperties
can inject SASL modules, metric reporters, etc. — fails Structural 9's "as documented" qualifier: those
configs' documented use never touches the record path; an interceptor's does).

### 1.9 [SHOULD — Operational 2; constraint 4.2] Position facts are gathered synchronously on the stream thread, behind one lock, at a cost set by frontier size

`AdminFactsSource.gather` is `synchronized`, performs describes and offset queries with 10 s timeouts plus
up to 1 s of probing per held channel, and runs inside the punctuator — counting against
`max.poll.interval.ms`, shared by every task of the application. Its per-round cost scales with the frontier
(`union(receivedChannels, frontierChannels)` drives both describes and `listOffsets`), a quantity an
external producer can inflate (see 2.3). Measured on this tree: 1.5 ms/round empty vs 9.6 ms/round with
1,000 frontier coordinates (6.4×), same call counts, fatter calls — plus network latency per round on a real
cluster. Off-thread gathering is sound — every fact is a per-position lower bound (constraint 4.2) — and
removes both the punctuator stall and the shared-lock coupling.

### 1.10 [SHOULD — Operational 1] No fail-closed reason is readable programmatically, and `healthy()` lies during rebalance limbo

The public surface is `start`/`healthy`/`close`; all ten `ParsleyFailClosedException.Reason` values die in a
log line behind the uncaught handler. A supervisor cannot distinguish a deliberate refusal (which recurs
identically — e.g. `CHANNEL_IDENTITY_CHANGED`) from a transient outage. Observed consequence of the
`isRunningOrRebalancing` definition: the doomed shrink in 1.6 reported healthy for ~42 s while delivering
nothing. Expose per-process state and the stop reason (Operational 1).

### 1.11 [SHOULD — Operational 2] The bootstrap's stranded-message scan can block `start()` indefinitely

`refuseStrandedHeldMessages` drains the changelog with `while (position < end) poll(500ms)` and no deadline
(`ParsleyRuntime.java:172–175`) while every admin call around it has a 30 s timeout. A broker becoming
unreachable mid-scan leaves startup blocked without diagnosis.

### 1.12 [SHOULD — Operational 3] `close()` has no exception isolation

`ParsleyRuntime.close` (`ParsleyRuntime.java:260–269`) runs streams closes, facts-source closes, then the
admin close with no try around any step; one throw leaks everything after it. `start()`'s failure path calls
this same `close()`, so a failed startup can leak the resources it was releasing.

### 1.13 [SHOULD] Forwarding received headers on emission is a deterministic crash-loop trap

`Delivery.headers()` includes the `parsley.causes` header (documented, `Delivery.java:63–66`);
`Effects.Emission` refuses any `parsley.`-prefixed header at construction (`Effects.java:18–26`), inside the
handler's own frame. An application forwarding its received headers — `send(ch, k, v, delivery.headers())`,
a natural pattern — therefore throws, the step aborts, and every restart re-fails identically until the code
changes. Fail-closed and safe, but a sharp edge: filter the reserved prefix from `Delivery.headers()`, or
provide an application-headers accessor, or accept-and-strip on emission — choose and record (this read-side
exposure is itself an unrecorded choice; see 1.15).

### 1.14 [SHOULD] Reserved names are refused for stores and headers but not for topics

A declared topic name colliding with a runtime-internal topic name (the ordering-store changelog,
`<appId>-__parsley.ordering-changelog`) is not refused at declaration or start — the only topic-name
validation anywhere is non-blankness (`Channel.java:22–24`). Partly self-defending today (duplicate source
names fail the topology build), but the namespace grows with every runtime-owned topic.

### 1.15 [MUST — Structural 20] Unrecorded choices: write the missing decision records

Each of these is an implemented open choice with live alternatives, absent from `DECISIONS.md` (the log's
own bar: D39 records exactly this shape). Recording them — or changing the code so the choice disappears —
resolves the entry:

- **Send-set matching semantics.** Emissions are validated by topic name only
  (`ParsleyProcessor.java:205`); the emission's own `Channel` object supplies the serdes (:212–215); the
  declared send channel's serdes are never consulted; `sends()` silently keeps the first `Channel` per topic
  (`ProcessDefinition.java:87–92`) while `receives()`/`stores()` throw and store access enforces reference
  identity. (Verified not to be a safety defect — the emission serde *is* the application's own codec, and a
  downstream mismatch fails closed per D13 — but identity-matching emissions were an implementable
  alternative that this code declined, unrecorded.)
- **Ordering-store changelog provenance.** D17's "the store is changelogged" and D38's "(compacted)
  changelog" both rest on documented Streams *defaults* (builder logging-on; compact cleanup for
  non-windowed store changelogs) that the code never requests (`ProcessTopology.java:50–55`). Were either
  ever different, restart restores an empty store (Safety 2/Liveness 5 collapse) or held entries age out of
  the changelog. Record the reliance under the D19 pattern — or harden by requesting
  logging-with-compaction explicitly.
- **The read-side of the reserved namespace.** Applications may read `parsley.causes` at the seam but not
  write it — a deliberate asymmetry, javadoc'd, never recorded (D18 covers the write side only).
- When 1.8 is fixed, its D37 additions are recorded there; when 1.2/1.4 are fixed, correct D40 and D21
  accordingly.

## 2. Causal-frontier size

Established mechanics any resolution must take as given (all verified on this tree, on a real broker):

### 2.1 [context for 2.4/2.5] Frontier size is set by the causal graph, not the declaration; growth is effectively monotone

Receipt merges *every* metadata pair, including channels the process never receives
(`ProcessEngine.java:183–189`) — necessarily, for downstream re-expression (Structural 15). Every emission
carries the whole frontier. Steady-state size approaches Σ(partition count) over the transitive upstream
closure: each re-keying hop multiplies exposure by a partition count, and output topics carry metadata their
external consumers have no use for. An entry is released only below its channel's log start or at channel
death; the entry holds the channel's *highest* causal position, which outruns retention while the channel is
in use — so the prune is a collector for idle channels, not a bound.

### 2.2 [context] An inflated frontier is durable and carried by unrelated traffic

Re-demonstrated this session at 50-channel scale: one hand-built header naming 50 idle channels at position
0 → the next emission carries 1,433 bytes (= 5 + 28×51); a second, headerless record's emission carries the
same 1,433 bytes; unchanged after ~10 facts rounds (position 0 on an empty partition tests `0 < 0` — never
released); still 1,433 bytes after close-and-restart (persisted ordering store). Arithmetic scales at
28 bytes/entry: 1,001 entries → 28,033-byte headers on every emission regardless of payload.

### 2.3 [context — Assumption 13] Anyone who can produce to a received channel sets the frontier's size

`CausesCodec.encode`/`Causes.of` are public by design (D3 froze the wire format for non-parsley senders); a
coordinate naming an unreceived channel is vacuously satisfied, delivers normally, lodges in the frontier,
and propagates. Within a closed graph the maximum frontier is statically calculable from the declarations;
with external injection it is every topic-partition the injector can name. Forged ids self-clear via the
dead-channel path (~3 rounds); coordinates on real, retained topics persist.

### 2.4 [SHOULD — Operational 4] Growth has no ceiling short of the record-size wall, where the process stops permanently

Headers count toward record size; at default 1 MB limits the ceiling is ≈ (1,048,576 − ~107)/28 ≈ 37,400
entries. At the wall: `RecordTooLargeException` → production exception handler FAIL (unoverridable, in
`FORBIDDEN_SUFFIXES`) → `SHUTDOWN_CLIENT` → zero commits, permanent stop. Safety holds to the wall; nothing
in-band can legally shed load (Structural 13 forbids discarding live causes; cross-channel compression is
unsound — constraint 4.4). **Compression settled this session:** the producer's `max.request.size` check
uses the *uncompressed* serialized size (64 KB of zeros with a 16 KB limit fails client-side), so
compression never moves the producer-side ceiling; only the broker-side `max.message.bytes` judges the
compressed batch. Implement Operational 4: a configurable metadata budget that fails closed with a parsley
diagnosis before the substrate's wall.

### 2.5 [SHOULD — Operational 5] Size is invisible in operation

No surface reports a process's frontier size; the first observable symptom of growth is 2.4's terminal stop
(or 1.9's slow facts rounds). Implement Operational 5: expose frontier size, and document the growth law of
2.1 for application designers.

## 3. Suite blind spots

All **[MUST]**: each names a violation the suite is obliged to catch and today cannot — with the mutation or
probe, run on this tree against the green 380-test baseline, that proves it. Resolution = the check exists
and the listed mutation/probe is now caught. Sequence these first: several production fixes (1.1, 1.2, 1.4,
1.6) can only be regression-pinned by the widened harness these entries demand. Constraints 4.4–4.6 shape
the rework; do not build the checks piecemeal against them.

### 3.1 The suite cannot see omission

The oracle learns of a receipt only when the engine returns ACCEPTED (`SimProcess.java:170–173`), and the
sim's read position advances before `onReceive` — an engine that consumes a position and discards it erases
the evidence of its own loss. Mutation: after cause extraction in `ProcessEngine.onReceive`, add
`if (message.position() == 3) return ReceiveOutcome.DUPLICATE_DROPPED;` → **all 380 green** while every
position-3 message on every channel is discarded. Fix shape: the oracle must observe the *feed* (and treat
engine-dropped-as-duplicate positions as deliveries owed unless the oracle itself knows them delivered).
Note the same acceptance-gating narrows the Structural 15 check: causes of dropped-but-received messages are
policed by exactly one unit test (`causesOfAJoinClampDroppedMessageStillBindSends` — verified the sole
red test under the reorder mutation); keep it, but the oracle should own the property.

### 3.2 The random generator cannot reach truncation, channel death, declaration change, or fail-closed outcomes

The generator's event vocabulary (`Scenario.java:77–118`) is feed/drain/facts/produce/appendDead/commit/
abort/crash/stop/rewind — no truncation, no topic deletion, no redeclaration, though `SimWorld.truncate`,
`SimWorld.killChannel` and `SimProcess.redeclare` all exist for targeted tests. Consequently across all 300
property seeds: log starts stay 0, dead sets stay empty, and `onFacts`' truncation check, both prune arms,
and the dead-channel settling are dead code; `SabotageMetaTest`'s sweep must omit exactly the three modes
(`UNDECODABLE_AS_ABSENT`, `IGNORE_TRUNCATION`, `IGNORE_REMOVED_CHANNELS`) the generator cannot produce
violations for. Worse: a mid-run fail-closed aborts the entire run — `Scenario.run` catches `Throwable` and
returns without `finalChecks`/`checkAllReceivedDelivered` — so once the generator *can* produce refusals,
every check is waived for exactly the runs that exercise them unless the harness learns to continue past a
failed-closed process. Margin is bounded by generator shape, not seed count — on this tree three of the
eight sabotage modes provably have margin zero at any seed count — so measure per-mode catch margins on the
suite you build; `caught > 0` says nothing about margin.

### 3.3 Boundary coverage is absent exactly where the off-by-ones live

- Truncation guard: `logStart > base + 1` → `> base + 2` in `ProcessEngine.onFacts` (accepting a truncation
  that discarded exactly one unread position) → **380 green**.
- Delivered-past prune: `entry.getValue() < logStart` → `<=` in the *deliveredPast* loop
  (`ProcessEngine.java:307`) — over-pruning the Structural 16 join clamp by one position → **380 green**.
  (The same mutation in the *frontier* loop is killed by 225 seeds — but only via the degenerate
  logStart=0/position-0 case, not by any genuine truncation scenario; do not mistake it for coverage.)
- Restart fidelity: persisting zero headers (`headers = List.of()` in `StoreCodec.encodeHeld`) → **380
  green**; persisting timestamp `0L` → **380 green**. Both reach application logic through the seam after a
  restart. (Persisting zero *causes* is caught by 214 tests — the suite can see restart corruption when it
  breaks ordering; it is blind to content fidelity beyond key/value.)

Add: truncation scenarios at exact boundaries (gap of one, both directions), join-clamp coverage after
retention, and restart assertions on delivered headers and timestamp.

### 3.4 Over-expression is policed by one incidental assertion

`Oracle.checkExpression` (`Oracle.java:82–103`) checks under-expression, unassigned positions
(commit-time — see below), and self-dependency; it never compares expressed pairs against true causes from
above. Mutation: merge `fedUpTo` (assigned non-causes) into every emission stamp → **all 300 property seeds
pass**; the sole kill is one exact-equality unit assertion (`frontierMergesReceiptDeliveryAndStampsEmissions`).
Any over-expression variant preserving that one fixture survives. Add an oracle-level upper-bound check —
derived from the oracle's own bookkeeping, never from expressed metadata (constraint 4.5: an
expression-derived checker inherits the very lies it must catch). Also: the Structural 12 check compares
against `lastAssigned` *at commit time*, so appends interleaving between send and commit excuse
genuinely-future expressions — evaluate at send time.

### 3.5 The facts source has no tests at all, and the bootstrap path has no seam

Zero references to `AdminFactsSource` in `src/test`. Mutations proving the blind spot: delete the confirming
describe (`confirmedNames = liveNames`) → **380 green**; `DEAD_CONFIRMATION_ROUNDS = 3 → 1` → **380 green**.
Likewise the runtime bootstrap: disable `refuseStrandedHeldMessages` entirely → **380 green** (D38's scan
has no behavioural coverage; only the pure key-interpreter is pinned); make `commitInitialPositions` ignore
`priorState` and honour a declared LATEST after offset expiry (reverting D36 to the behaviour it exists to
prevent) → **380 green**. The bootstrap methods are private and reachable only through `start()` against a
live broker — decide the seam (a package-private hook, or accept broker-only coverage) and record it.
Constraint 4.6: fabricating `AdminFactsSource`'s inputs without a broker pulls toward Kafka-internals
constructs (`DescribeTopicsResult`'s non-public factories, `KafkaFutureImpl`) — the probe recipes in 1.1,
1.2 and 1.5 exercise the real paths against `KafkaClusterTestKit` in seconds and are the cleaner base.

### 3.6 No test creates more than one partition

Sole `NewTopic` call in the tree: `new NewTopic(name, 1, (short) 1)` (`EndToEndIntegrationTest.java:101`).
Everything in the partition→task→channel dimension was unexercised until this session's probes: multi-task
operation itself works (3-partition probe: all tasks live, unequal partition counts fine, shared facts
source uncontended at small scale), and the shrink probe found 1.6. Add multi-partition coverage to the
integration suite — the two probe recipes (1.6's and the multi-task sweep) are the template — plus the
1.2-pooled-streak and 1.9-contention dimensions none of this session's probes pushed to failure.

### 3.7 Half the fail-closed reasons have no test, two are unreachable

`APPLICATION_PAYLOAD_UNDECODABLE`, `OUT_OF_ORDER_FEED`, `RESERVED_HEADER_USED`, `SUBSTRATE_MISCONFIGURED`,
`UNKNOWN_ORDERING_STATE_FORMAT`: zero references in `src/test`. `RESERVED_HEADER_USED` and
`SUBSTRATE_MISCONFIGURED` are never thrown in `src/main` either — the reserved-header refusal actually
fires as `IllegalArgumentException` from `Effects.Emission` (1.13). Cover the real branches; delete or wire
the dead constants.

## 4. Cross-cutting constraints

Verified properties of the problem. None prescribes a design; each bounds one.

- **4.1 (for 1.4)** Retaining dead-channel pairs is only unbounded at the origin. Downstream copies
  self-clean: every process's facts source dead-verdicts frontier ids after three rounds and prunes (that is
  D40's deliberate design, and measured injection headers settle back within ~the confirmation window). But
  the *retaining* process re-expresses its pairs on every emission forever, re-seeding downstream copies —
  so expression-retention grows the origin's frontier and every emission with every dead channel it ever
  delivered from. A fail-closed resolution of 1.4 avoids the cost entirely.
- **4.2 (for 1.9)** Facts tolerate off-thread gathering and late application: every fact is a per-position
  lower bound (committed reads and log starts only advance; a read_committed probe's never-yield verdict is
  immutable once true; dead verdicts are terminal by id). The argument does not extend to sharing
  engine-owned collections across threads without copying on the stream thread, nor to reusing a completed
  round twice. Note `onFacts` applies the read-position report *before* the truncation check deliberately —
  a retention pass over an already-covered run must not fail spuriously; keep that order.
- **4.3 (for 1.6, 3.5)** Changelog re-readers have two known failure shapes: judging single poll batches
  (reports a message received in one batch and delivered in the next as stranded — stops a healthy app
  during exactly the rolling redeclaration the guard exists for) and retaining tombstoned keys (grows with
  every message ever held; tombstone-as-remove is the correct reading — `OrderingStateInspector` already
  does this, and the one-shot scan accumulates fully before judging; preserve both properties in any
  replacement). Cost floor: the ordering store writes a fed-up-to entry per received message, so any
  changelog tail-reader pays one record per inbound record.
- **4.4 (for 2.4)** Per-channel max is the tightest sound summary. Dropping (c₁,p₁) as covered by a retained
  (c₂,p₂) is sound only when readers(c₁) ⊆ readers(c₂) across the reachable graph — at a process reading c₁
  but not c₂ the surviving pair is vacuous and the constraint is lost. Arbitrary-pair soundness needs equal
  reader sets (a full mesh), which partial channel consumption precludes. (D4 records the receiver-side
  ground; the sender-side impossibility — computing coverage needs the cause message's own metadata — is
  established here, not in D4.)
- **4.5 (for 3.4)** A join-clamp or expression checker fed from expressed metadata is circular: an
  over-expressing engine inflates the clamp and the exemption in lockstep and silently drops messages that
  were never in anyone's causal past — and the acceptance-gated oracle cannot see the drop (3.1). The
  checker's cause-truth must come from its own bookkeeping.
- **4.6 (for 3.5)** `src/test` currently touches no Kafka `internals` package except
  `RecordHeader`/`RecordHeaders` (two files; the only `Header`/`Headers` implementations shipped in 3.9.1)
  and never calls `setAccessible`. That freedom is under structural pressure exactly where 3.5 wants
  coverage; prefer real-broker probes over internals-coupled doubles.
- **4.7 (recovery facts)** A held message's read position commits *before* its delivery — that is the point
  of hold persistence — so moving group offsets cannot clear a failure that fires at delivery-from-store
  (e.g. `APPLICATION_PAYLOAD_UNDECODABLE`); it recurs from the changelog on every restart. Committed offsets
  at the log end prove nothing about the hold-back buffer.
- **4.8 (documentation)** Of the unspecified-Streams-behaviour dependencies, three are named and
  test-pinned in `DECISIONS.md` (D6/D9/D19/D35); the internal-topic **width validation** that 1.6 rides on
  is named nowhere and covered by nothing. When fixing 1.6, name it and pin it; D19's "a Streams upgrade
  re-runs the integration suite" is worth exactly what suite coverage is worth (§3).

## 5. Desired characteristics

### 5.1 [SHOULD] Enumeration on `StateReader`

`StateReader` exposes exactly `<K,V> V get(StoreDef<K,V>, K)`; no `range`/`prefixScan`/`all`, though the
stores beneath are `KeyValueStore<Bytes, byte[]>` with all three. A deliberate v1 scope decision (D12 records
it under Cost), not a safety concern: reads of local committed state cannot reorder deliveries. If adding
enumeration, settle: iterator visibility vs the current-invocation `Effects` buffer (undefined today);
determinism over serialized-key order (byte order ≠ deserialized order); and scan locality (a "range" is
one shard, not the key space). Multi-store point-lookup patterns already work; only enumeration is missing.

### 5.2 [SHOULD] One name per concept, mechanically enforced

Nothing in the build enforces naming (`pom.xml`: no checkstyle/spotless/PMD/Error Prone), and the semantic
layer drifts where it costs most — the public API. Verified on this tree: `StoreDef` beside
`ProcessDefinition`; `receivedTopics()`/`sendTopics()` and `input(topic)`/`sendChannel(topic)` as
disagreeing mirrors; `stores()` as both accessor and builder-mutator; `named(...)` vs `builder(...)`;
`Input` as the sole "input" in a receive/deliver/send vocabulary; `Deliverability.Deliverable` vs
`DeliverableMessage`; `fedUpTo` as field and method; empty singletons as `NONE`/`EMPTY`/`INSTANCE`;
`HeaderKV` the lone initialism; `StoreCodec.longValue`/`longOfValue` adjacent near-inverses (the one item
with a route to a wrong edit); codec verb triples (`encode`/`decode`, `encodeHeld`/`decodeHeld`,
`writeTo`/`readFrom`/`toBytes`); `onReceive`/`onFacts` vs `onReceived`/`onDelivered` vs `ingestFacts`;
`heldCount`/`heldCountTotal`; `receivedChannelSet()`; boolean styles split three ways across the codebase
(`healthy()`, `isDeliverable()`/`isEmpty()`, `changelogExists()` — the last private, not public API);
`xByY` vs bare-noun map names (sharpest: `logStartByPartition` beside `logStart`, `committed` beside
`committedNextRead` in one method); test doubles as `Sim*`/`Memory*`/`Fake*` (the last nested-class-scoped)
plus unmarked `Oracle`/`Scenario`/`Instance`; four test channel-constant schemes. What already holds
uniformly (all-record accessors, `get` only on the two genuine map lookups, `LOG`, `*Exception`, `*Test`,
SCREAMING_SNAKE, sentence-style test names) is worth pinning with tooling before it erodes. Public-API
renames are breaking later; that asymmetry is the reason to decide now. Enforce mechanically or record the
deviation.
