# Audit remediation work state

Working notes for the pre-vendoring fixes on this branch. Delete before vendoring.
Findings and fix directions: VENDORING-AUDIT.md. Session context: fixes are applied one
finding per commit, each with pinning tests, full suite run before each push.

## Done

- **F1 (critical)** — stale facts round survives EOS task revival. Fixed in
  `ParsleyProcessor` (incarnation epoch + apply-time check + epoch-owned gather slot +
  punctuator cancel + close()). Pinned by `ProcessorRevivalTest` (9 tests; each guard
  individually mutation-tested). Commits a20d521, 896c803. This also discharges test gap
  §3.1 (T1).
- **F3 (major)** — StoreCodec decode paths trust wire length/count fields. Fixed:
  every length/count validated against remaining bytes before allocation; cause-count
  check requires exact remaining (also rejects trailing bytes); NegativeArraySizeException
  added to the corruption catch; `decodeLong` requires exactly 8 bytes;
  `channelOfChannelKey`/`channelOfHeldKey`/`positionOfHeldKey` validate exact key
  lengths with the classified refusal (repointed in ProcessEngine + inspector). Pinned
  by `StoreCodecCorruptionTest` (19 tests incl. two engine-restore refusal tests).
  Discharges test gap §3.3.
- **F2 (major)** — unversioned store silently re-stamped v1. Fixed in `ProcessEngine`:
  when the version entry is null, every tag prefix is probed and any surviving record
  raises `UNKNOWN_ORDERING_STATE_FORMAT` (changelog-head loss diagnosis) before any
  stamp or name re-bind. Pinned by 4 tests in `StoreCodecCorruptionTest` incl. the
  audit's real-engine strip-the-version probe.
- **F4 (major)** — one unavailable partition blacked out the whole facts round. Fixed
  in `AdminFactsSource`: per-partition `partitionResult()` futures behind a
  package-private `earliestOffsets` seam, shared 10s deadline, per-partition catch so
  loss confines to the affected channel and dead/recreated verdicts still ride;
  `committedOffsets`/`describeByNames` extracted as seams. Pinned by
  `AdminFactsSourceDegradationTest` (2 tests, mutation-checked: removing the
  per-partition catch fails both). Discharges the F4 half of test gap §3.6.
- **T2 (test gap §3.2)** — explicit changelog-recovery evidence. Added
  `EndToEndIntegrationTest#heldMessageSurvivesAStateDirWipeByChangelogRestore` (state
  dir deleted between starts; hold and order rebuilt from the changelog) and
  `BootstrapIntegrationTest#orderingChangelogIsCreatedCompacted` (cleanup.policy
  asserted). Corrected docs/verification.md (driver-restart claim removed, wipe test
  added) and EVIDENCE.md Liveness 5 (changelog leg now cites the wipe test).
- **C1** — scanPrefix javadoc now promises unsigned-lex order; TAG_HELD restore verifies
  per-channel position monotonicity → refusal. Pinned by reverse-scan wrapper test in
  `StoreCodecCorruptionTest` (writer engine + flushHolds to persist two holds).
- **C2** — `ParsleyProcessor.send()` re-checks headers after serializers run, before the
  genuine stamp → sender-side RESERVED_HEADER_USED. Pinned by smuggling-serializer test
  in `TopologyWiringTest`.
- **M3** — `ParsleyRuntime` monitoring collections now synchronizedMap/COW lists.
- **M4 + §3.6b** — debounce continuity: an UNAVAILABLE answer for a known name now
  clears `unknownSince`, so death is confirmed only by an unbroken run of affirmative
  name-gone answers. Pinned by `AdminFactsSourceDebounceTest` (mutation-checked;
  NameVerdict made package-private as the scripting seam). Closes test gap §3.6 fully.

All four confirmed audit findings (F1-F4) and both P1 test gaps (T1 §3.1, T2 §3.2,
corruption pins §3.3) are now closed. Remaining triage candidates: C1/C2 hardenings,
M1-M4 minors, P2/P3 gaps §3.4-§3.9.

## To do (in order) — hardening pass

- **§3.7** — CorePurityTest: recursive walk + widened ban list.
- **M2** — GroupMembershipCommitter: strip group.instance.id, cap session timeout.
  Audit asks for a broker repro before fixing.
- **§3.5** — SabotageMetaTest: oracle-violation assertions for the three assertDoesNotThrow
  modes + DELIVER_PAST_DEAD_HOLDS random-sweep floor; align docs/verification.md.
- **§3.8** — Oracle delivery-time legality check.
- **§3.4** — two-instance task-migration smoke test.
- **M1 / §3.9** — init-gather blocking cost; broker-bounce test. Lowest priority.

## Verification protocol per fix

1. Write failing test first where cheap (F2/F3 pins), or fix+pin together.
2. `./mvnw test -Dtest=<class>` targeted, then full `./mvnw test` (427+ tests) before push.
3. Commit one finding per commit, push immediately.
