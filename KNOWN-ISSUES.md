# Known issues and residual risks

What remains open after the pre-vendoring audit and its remediation (PR #89). Every
confirmed defect the audit found was fixed there; this records what was deliberately not
fixed — risks that are structural, areas the audit did not cover, and accepted
observations — so the adopting fork inherits the knowledge and not just the code.

## Residual risks, recorded and accepted

These were confirmed by the audit and are not closable by a code change in this library.
They belong in the fork's operator documentation.

1. **Wholesale changelog record loss is undetectable at the store seam.** If the ordering
   changelog loses records while the topic itself and the group offsets survive, the
   restored store is internally consistent and passes every check. The unversioned-store
   refusal narrows this — losing the head segment, where the version entry lives, is
   detected — but durable provenance outside the store is unavailable by design (D33,
   D36), so a loss confined to later segments cannot be distinguished from compaction.
   Operator guidance: never weaken `cleanup.policy=compact` on `*-__parsley.ordering-changelog`
   topics (the bootstrap asserts it; see `BootstrapIntegrationTest#orderingChangelogIsCreatedCompacted`),
   and treat changelog retention incidents as state-reset events.

2. **An operator forward-resetting group offsets forges the read-position report.** The
   committed group offset is the host's canonical statement of what was read (D6). Setting
   it forward while a process is stopped asserts that unread positions were read; the
   library cannot distinguish this from truth. Operator guidance: reset a process's state
   and offsets together, deliberately, never offsets alone.

3. **Concurrent multi-instance cold start can collide (D48 residual).** Several instances
   of one application cold-starting at once can interleave one instance's Streams join
   with another's still-open bootstrap membership, failing that Streams client fatally.
   The window is the few seconds of a bootstrap with missing offsets; a supervisor restart
   recovers. Sequential first-start of a new application avoids it entirely. Ordinary
   scale-out — a second instance joining after the first has bootstrapped — takes the
   read-only fast path and is unaffected (see
   `EndToEndIntegrationTest#heldMessageSurvivesTaskMigrationBetweenInstances`).

## Areas the audit did not cover

4. **The `api/` package's validation surface.** The audit's dimensions touched `Effects`,
   `Delivery` and `ParsleyConfig` incidentally but performed no dedicated hunt over
   declaration validation, the serde surface, or `Parsley.start`'s public contract. The
   config deny-list is load-bearing (two findings turned on it), so a focused pass over
   `api/` validation is the most valuable unexamined ground for a follow-up review.

5. **Build, packaging and dependency supply chain.** No audit dimension reviewed
   `pom.xml`, the wrapper, CI, or dependency provenance. The adopting organisation should
   run its own supply-chain pass. Note that the revival-handling fixes were verified
   against the pinned Kafka Streams version's bytecode specifically; D19's mandate — 
   re-validate the lifecycle assumptions on any Kafka upgrade — carries into the fork's
   process.

## Accepted observations from the merge reviews

Raised during the pre-merge reviews, judged not worth a change, and recorded so they are
not rediscovered as surprises.

6. **`AdminFactsSource.close()` can wait for an in-flight round.** Shutdown may block for
   up to one gather (bounded by its per-query timeouts) while the round holds the source's
   lock. This matches the pre-existing synchronized behaviour and is bounded, not a hang.

7. **A bootstrap configured with a long session timeout holds the group after an
   ungraceful exit.** An explicitly configured `session.timeout.ms` (any spelling,
   including the Streams-prefixed forms) is respected so brokers with a raised
   `group.min.session.timeout.ms` can start at all; the cost, logged at startup, is that a
   killed bootstrap holds the group for that timeout before a successor can join. The
   default remains ten seconds. Graceful close is unaffected — the member always leaves
   the group dynamically.

8. **The revival guard's authoritative check is pinned through reflection.**
   `ProcessorRevivalTest` plants stale facts rounds via reflection on private members of
   `ParsleyProcessor` (the interleaving the check guards is narrower than any schedule a
   test can construct). Renaming those members requires updating the test; the test's
   layout assertion fails loudly rather than passing vacuously if the shape drifts.

9. **The reserved-header re-check surfaces at send time.** A serializer that writes into
   the `parsley.` namespace fails the step with `RESERVED_HEADER_USED` at the sender
   (documented in `docs/failing-closed.md`). Serializer authors see the refusal on their
   own emission rather than a poisoned topic downstream — intended, but worth knowing when
   wrapping header-writing serializers.

## Naming items still open

The naming work the audit and reviews recommended was applied and recorded as
`DECISIONS.md` D72 (the public `StoreDef` → `Store` rename and rule, and seven internal
renames). Three items were deliberately not applied and remain open.

10. **Four scripted `FactsSource` fakes, four naming styles.** `FakeFacts`,
    `ControllableFacts`, `DegradedFacts` and `ScriptedFacts` are the same idea — a
    `FactsSource` whose answers the test scripts — at different levels of control.
    Converging on one convention (`Scripted*` fits the most capable) and, where practical,
    one shared fixture would let the next test author extend rather than invent a fifth.

11. **`Sabotage.Mode` and `EngineTestFactory.SabotageMode` are mirrored enums bridged by
    `valueOf(mode.name())`.** The duplication keeps the production enum package-private,
    which is sound; the cost is that adding a mode touches both and nothing checks they
    stay aligned. A constant renamed in one fails only at runtime. Before touching either,
    add the alignment assertion the bridge silently assumes — or expose one enum through
    the factory.

12. **`ParsleyFailClosedException.Reason.UNKNOWN_ORDERING_STATE_FORMAT` now classifies
    more than its name says** — unknown format version, changelog-head loss, corrupt
    entries, holds restored out of scan order; the doc row honestly reads "stored state
    cannot be trusted". A truer name would be `ORDERING_STATE_UNTRUSTED`, but `Reason` is
    public API and D55 establishes that supervisors key on `refusalReason`, so renaming
    breaks the integrations the fail-closed contract exists to serve. Keep the name, lean
    on the `docs/failing-closed.md` row, and revisit only at a moment the fork is already
    breaking its supervision interface.

## A note on unused members

This is a library: parts of its surface exist for hosts, integrators and the test harness
rather than for callers inside this repository. Static analysis may flag members as unused
that are in fact load-bearing — `ParsleyProcessor.close()` is invoked by the Kafka Streams
task lifecycle, the `FactsSource` seam methods exist for injection, and inspector and
status accessors exist for operators. An unused-member warning here is not, by itself, a
defect; remove members only on evidence that no supported caller class needs them.
