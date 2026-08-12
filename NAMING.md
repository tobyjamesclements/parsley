# Naming recommendations

Names that earned a recommendation during the pre-vendoring audit, its remediation
(PR #89), and the two adversarial reviews of that work. The test is not taste: each entry
below is a name that, in practice, forced an explanation — a reviewer misread it, a fix
went wrong partly because of what it implied, or every discussion of it had to start by
restating what it actually means. None of these renames are applied here; internal names
can be renamed freely, and the one public-surface item is flagged as a fork decision.

## Recommended renames

### 1. `AdminFactsSource.unknownSince` → `affirmedGoneSince`

The strongest recommendation, because the name contributed to a defect. The field anchors
the dead-channel confirmation window, which D44 defines as an unbroken run of
*affirmative* name-gone answers. "Unknown since" instead suggests it tracks any period of
non-observation — and that is exactly the bug the audit found (M4: the window survived
rounds with no answer) and the bug's first fix over-corrected (clearing it on rounds whose
answers had landed). Both review rounds spent their sharpest findings re-deriving this
field's intended semantics. A name that says what opens and extends the window —
an affirmed gone answer — makes the continuity rule self-evident at every use site.

### 2. `ParsleyProcessor`: `incarnation` field vs `epoch` locals — one term

The revival machinery uses `incarnation` for the counter field but `epoch` for the
captured value, the `GatheredRound` record component, and every comparison. They are the
same concept; two words imply two. The docs and commit history consistently say
"incarnation", so rename the locals and the record component to match.

### 3. `ParsleyProcessor.gathered` → `pendingRound`

An `AtomicReference` holding a completed facts round awaiting application on the stream
thread. "Gathered" (a bare past participle) says how it got there, not what it is; every
review discussion independently reached for "the pending-round slot", and the revival test
even names its reflection accessor `pendingRound()`. Let the field carry the name everyone
already uses for it.

### 4. `AdminFactsSource.gatherRound` / `completeRound` — collapse to one method

After the second review round moved the abort-handling inside, `gatherRound` is a
closed-check plus a delegation, and `completeRound` is the body. The pair reads as if
"complete" contrasts with some other kind of round, which was true for one commit and is
not anymore. Merge them (or `runRound`), keeping the observation-stage comment where the
try/catch lives.

### 5. `AdminFactsSource.rounds` (the `ReentrantLock`) → `roundLock`

A lock named after the thing it serialises reads as a collection of rounds at use sites
(`rounds.lockInterruptibly()` survives scrutiny; `rounds` in a debugger does not).

### 6. `AdminFactsSource.earliestOffsets` → `earliestOffsetFutures`

The F4 seam returns one future per partition — that per-partition granularity is the
entire point of the fix — but the name promises offsets. The review had to annotate the
call site to explain that failures are per-element. Naming the future-ness keeps the
fix's contract visible in the signature.

### 7. `StoreCodec.channelOfChannelKey` → `channelOfEntryKey`

Introduced during F3. The stutter comes from `channelKey(tag, channel)` naming the
*builder*; the accessor reads "channel of channel key". The keys it accepts are the
per-channel entry keys (`f`/`c`/`p` tags), so `channelOfEntryKey` — or renaming the
builder to `entryKey` and this to `channelOfKey` — removes the stutter without losing the
exact-length contract in the javadoc.

### 8. Test fakes: one convention for scripted `FactsSource` implementations

Four hand-rolled fakes now exist with four naming styles: `FakeFacts`
(`TopologyWiringTest`), `ControllableFacts` (`ProcessorRevivalTest`), `DegradedFacts`
(`AdminFactsSourceDegradationTest`), `ScriptedFacts` (`AdminFactsSourceDebounceTest`).
They are all the same idea — a `FactsSource` whose answers the test scripts — at different
levels of control. Converging on one convention (`Scripted*` fits the most capable of
them) and, where practical, one shared fixture would make the next test author extend
rather than invent a fifth.

### 9. `Sabotage.Mode` vs `EngineTestFactory.SabotageMode` — mirrored enums

Two enums with identical constants, bridged by `Sabotage.Mode.valueOf(mode.name())`. The
duplication exists to keep the production enum package-private, which is sound; the cost
is that adding a mode requires touching both and nothing checks they stay aligned. If both
must exist, name them so the mirroring is explicit (e.g. keep the names but add the
alignment assertion the bridge silently assumes), or expose one through the factory.

## Flagged, not recommended: the public surface

### `ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT`

The remediation stretched this reason well past its name: it now classifies an unknown
format version, state present without its version entry (changelog-head loss), corrupt
entries of every shape, and holds restored out of scan order — the doc row honestly reads
"stored state cannot be trusted", not "unknown format". A truer name would be
`ORDERING_STATE_UNTRUSTED`. But `Reason` is public API and D55 establishes that
supervisors key on `refusalReason`, so renaming it breaks the exact integrations the
fail-closed contract exists to serve. Recommendation: keep the name, lean on the
`docs/failing-closed.md` row, and revisit only at a moment the fork is already breaking
its supervision interface.

## Non-issues, for the record

`gatherForSeed` names its purpose rather than its mechanism (a bounded wait), which is the
right emphasis for a seam. `awaitFedAndHeld` and the behaviour-sentence test names match
the repository's established style. And as `KNOWN-ISSUES.md` notes for members generally:
this is a library, so surface that looks unused from inside the repository —
framework-invoked lifecycle methods, injection seams, operator accessors — is load-bearing
and correctly named for its external callers.
