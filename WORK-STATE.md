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

## To do (in order)

- **F2 (major)** — store missing its version entry but containing state is silently
  re-stamped v1 (`ProcessEngine.java:127`). Fix: if version entry is null AND any
  parsley-tagged record exists (bounded prefix probe over the store), throw
  `UNKNOWN_ORDERING_STATE_FORMAT`. Pin: build held state, delete only the `v` key,
  restart engine → refusal (audit found this passes silently today).
- **F4 (major)** — one unavailable partition blacks out the whole facts round
  (`AdminFactsSource.java:200`, `ListOffsetsResult.all()`). Fix: per-partition
  `partitionResult()` so loss confines to the affected channel; ensure dead/recreated
  verdicts still ride the round when position queries partially fail. Pin per §3.6:
  one failing partition must not suppress other channels' facts nor the verdicts
  (inject via the overridable admin seams D50 established).
- **T2 (test gap §3.2)** — explicit changelog-recovery test: EndToEndIntegrationTest
  variant wiping the state dir between two Parsley.start calls, assert held message
  still delivers; BootstrapIntegrationTest assertion that
  `<appId>-__parsley.ordering-changelog` has `cleanup.policy=compact`. Correct
  docs/verification.md:31 and EVIDENCE.md Liveness 5 while there.

## Not in scope this pass (triage later)

C1/C2 cheap hardenings, M1-M4 minors, P2/P3 test gaps §3.4-§3.9.

## Verification protocol per fix

1. Write failing test first where cheap (F2/F3 pins), or fix+pin together.
2. `./mvnw test -Dtest=<class>` targeted, then full `./mvnw test` (427+ tests) before push.
3. Commit one finding per commit, push immediately.
