package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The deterministic fold over the {@code epoch-events} log — the heart of the leaderless epoch protocol.
 * Every node applies the log's events in total order through an instance of this class and, because the
 * fold is a pure function of the ordered events, they all agree on round ownership, membership, round
 * completeness, the committed {@code lowerBounds}, and the committed member-app roster without any leader
 * or lock.
 *
 * <p><strong>Genesis cohort barrier.</strong> The first commit (genesis) establishes a new logical
 * time-0: its floor is empty by construction (an empty running set folds to an empty {@link
 * ParsleyVectorClock}), so a founder admitted at genesis strips nothing and sees full history in causal order.
 * Genesis does not commit until the whole founding cohort has declared — every app in the agreed roster
 * present with its full task set — so no founder is left behind and later mis-admitted at a non-empty
 * floor (which would strip a fan-out coordinate's genesis-era records: silent effect-before-cause).
 *
 * <p><strong>Roster is authoritative membership.</strong> Each {@link ParsleyEpochEvent.JoinRequested}
 * carries the declaring node's view of the complete member-app roster and its app's total task count. A
 * round commits only when the <em>voters</em> — all declarers before genesis, the committed-roster apps
 * after — unanimously agree on one roster {@code R_new}, every app in {@code R_new} not already committed
 * is cohort-complete (all its tasks declared, count matched), every running member has published, and the
 * mesh holds over {@code R_new}. It then promotes only pending joiners whose app is in {@code R_new}. A
 * newcomer the committed members have not named simply waits, un-admitted, without disturbing the
 * committed epoch (which keeps delivering under its floor). This single predicate — {@link
 * #evaluateCommit()} — is the source of truth for both proposing a commit and validating one on apply, so
 * a conflicting event that folds in just before a commit makes every node reject that commit identically.
 *
 * <p>Not thread-safe: a single consumer thread applies the log in order.
 */
final class ParsleyEpochLog {

    private long committedEpochId;                             // last committed epoch; 0 = pre-genesis
    private ParsleyVectorClock committedLowerBounds = ParsleyVectorClock.empty();
    // The app-id membership the last commit established — authoritative membership after genesis. Empty
    // before genesis. Voters (post-genesis) and the admission gate are computed against this.
    private Set<String> committedRoster = Set.of();
    private final Set<String> runningMembers = new HashSet<>();
    private final Set<String> pendingJoiners = new HashSet<>();
    private @Nullable String roundOwner;                       // non-null iff a round is open
    private final Map<String, ParsleyVectorClock> publications = new HashMap<>();  // current round only
    // Every declared member's declaration, keyed by memberId, LWW on re-declaration. Folded
    // UNCONDITIONALLY (a non-roster app's declaration is kept, then filtered at each use against the
    // roster relevant to that use) — folding-time exclusion would discard a legitimately-joining app's
    // declaration before its roster is agreed and wedge every add. Removed on Leave.
    private final Map<String, Declaration> declarations = new HashMap<>();
    // Each member's admission epoch+floor, written at promotion — the cut it was admitted at. A member
    // that restarts with no local state (e.g. an EOS abort before its first commit) re-seeds from this,
    // not from a constant floor, so an aborted fresh joiner re-adopts its own non-empty cut rather than
    // acting on pre-cut history at an empty floor. Removed on Leave.
    private final Map<String, ParsleyEpochRuntime.CommittedEpoch> admissionFloors = new HashMap<>();
    // Registry snapshot shift register (see externalSourceTopicsAsOfPreviousCommit).
    private Set<String> previousCommitExternalSourceTopics = Set.of();
    private Set<String> currentCommitExternalSourceTopics = Set.of();
    // The epoch id of the most recent commit event this fold REJECTED on apply (invalid against the
    // current state). Read once by the runtime to clear its append-once latch so the round can be
    // re-proposed; -1 when the last commit applied cleanly.
    private long lastRejectedCommitEpoch = -1;

    /** A member's declaration: its app id, the channels it consumes, the topics it produces, its view of
     * the complete member-app roster, and its app's total task count. */
    record Declaration(String appId, Set<String> inputTopics, Set<String> sinkTopics,
                       Set<String> rosterView, int taskTotal) {}

    /** Applies one event in log order, updating the folded state. */
    void apply(ParsleyEpochEvent event) {
        switch (event) {
            case ParsleyEpochEvent.JoinRequested e -> {
                if (!runningMembers.contains(e.memberId())) {
                    pendingJoiners.add(e.memberId());
                }
                declarations.put(e.memberId(), new Declaration(
                        e.appId(), e.inputTopics(), e.sinkTopics(), e.rosterView(), e.taskTotal()));
            }
            case ParsleyEpochEvent.SnapshotRequested e -> {
                if (roundOwner == null) {
                    roundOwner = e.memberId();
                }
            }
            case ParsleyEpochEvent.FrontierPublished e -> {
                if (roundOwner != null && runningMembers.contains(e.memberId())) {
                    publications.put(e.memberId(), e.completeness());
                }
            }
            case ParsleyEpochEvent.Leave e -> {
                runningMembers.remove(e.memberId());
                pendingJoiners.remove(e.memberId());
                publications.remove(e.memberId());
                declarations.remove(e.memberId());
                admissionFloors.remove(e.memberId());
            }
            case ParsleyEpochEvent.EpochCommitted e -> applyCommit(e);
        }
    }

    /**
     * Adopts an {@link ParsleyEpochEvent.EpochCommitted} only if it is valid against the fold state at
     * this exact log position — {@link #evaluateCommit()} must produce the identical decision (same epoch
     * id and roster). A commit that a conflicting event (folded in after its proposer read the state)
     * has invalidated is <em>rejected</em>: the epoch does not advance, no member is promoted, and the
     * round stays open. The floor is taken from the event, not recomputed: a floor built from published
     * frontiers and clamped monotonically is always a safe (possibly conservative) cut, so only the
     * roster/cohort decision needs re-validation, not the exact floor value.
     */
    private void applyCommit(ParsleyEpochEvent.EpochCommitted e) {
        if (e.epochId() <= committedEpochId) {
            return;                                            // dedup: stale or duplicate commit
        }
        Optional<ParsleyEpochEvent.EpochCommitted> decision = evaluateCommit();
        boolean valid = decision.isPresent()
                && decision.get().epochId() == e.epochId()
                && decision.get().committedRoster().equals(e.committedRoster());
        if (!valid) {
            lastRejectedCommitEpoch = e.epochId();             // signal the runtime to clear its latch
            return;
        }
        lastRejectedCommitEpoch = -1;
        previousCommitExternalSourceTopics = currentCommitExternalSourceTopics;
        currentCommitExternalSourceTopics = externalSourceTopics();
        committedEpochId = e.epochId();
        committedLowerBounds = e.lowerBounds();
        committedRoster = Set.copyOf(e.committedRoster());
        // Promote only pending joiners whose app is in the newly-committed roster; record each promoted
        // member's admission cut so a later stateless restart re-adopts it (see admissionFloors).
        ParsleyEpochRuntime.CommittedEpoch admittedAt =
                new ParsleyEpochRuntime.CommittedEpoch(e.epochId(), e.lowerBounds());
        Set<String> promoted = new HashSet<>();
        for (String member : pendingJoiners) {
            Declaration declaration = declarations.get(member);
            if (declaration != null && committedRoster.contains(declaration.appId())) {
                promoted.add(member);
                admissionFloors.put(member, admittedAt);
            }
        }
        runningMembers.addAll(promoted);
        pendingJoiners.removeAll(promoted);
        publications.clear();
        roundOwner = null;
    }

    // ---- derived membership / roster views -------------------------------------------------------

    /** Every app id that currently has at least one declared member. */
    private Set<String> declaredApps() {
        Set<String> apps = new HashSet<>();
        for (Declaration declaration : declarations.values()) {
            apps.add(declaration.appId());
        }
        return apps;
    }

    /** The apps whose roster views count toward agreement: the committed-roster apps that still have a
     * live declaration (a departed member's vote drops when its last Leave folds), or all declared apps
     * before genesis / when every committed member has left (the zero-voter fallback). */
    private Set<String> votingApps() {
        Set<String> declared = declaredApps();
        if (committedEpochId == 0) {
            return declared;
        }
        Set<String> voters = new HashSet<>(committedRoster);
        voters.retainAll(declared);
        return voters.isEmpty() ? declared : voters;
    }

    /** The roster the voters unanimously declared, or {@code null} if they do not agree (a change in
     * flight, or a genuine misconfig — both refuse to commit; the distinction is only diagnostic). */
    private @Nullable Set<String> unanimousRoster() {
        Set<String> voters = votingApps();
        Set<String> common = null;
        for (Declaration declaration : declarations.values()) {
            if (!voters.contains(declaration.appId())) {
                continue;
            }
            if (common == null) {
                common = declaration.rosterView();
            } else if (!common.equals(declaration.rosterView())) {
                return null;
            }
        }
        return common;
    }

    /** Whether {@code app} has declared its full task set — every one of its {@code taskTotal} tasks. */
    private boolean isAppCohortComplete(String app) {
        int declared = 0;
        int total = -1;
        for (Declaration declaration : declarations.values()) {
            if (declaration.appId().equals(app)) {
                declared++;
                total = declaration.taskTotal();
            }
        }
        return total >= 0 && declared == total;
    }

    /**
     * Whether the declarations of the apps in {@code scope} carry a cohort contradiction that makes a
     * commit unsafe: two tasks of one app disagreeing on the task total (partition-count instability
     * mid-roll), or a task whose partition index is {@code >=} its app's declared total (proof of an
     * undercount — a task that could never legitimately exist under that total). Either means a
     * genesis/growth commit could seal a roster while a real task is still missing, so it refuses to
     * commit. <strong>Scoped</strong> to the apps a commit actually depends on (the new roster plus the
     * committed members): an inconsistent declaration from a rogue app no committed member has named must
     * not freeze the domain — it just waits. Recomputed from current state, so it self-heals once a
     * rolling re-declaration completes.
     */
    private boolean hasCohortConflict(Set<String> scope) {
        Map<String, Integer> totalByApp = new HashMap<>();
        for (Map.Entry<String, Declaration> entry : declarations.entrySet()) {
            Declaration declaration = entry.getValue();
            if (!scope.contains(declaration.appId())) {
                continue;
            }
            Integer seen = totalByApp.put(declaration.appId(), declaration.taskTotal());
            if (seen != null && seen != declaration.taskTotal()) {
                return true;                                   // same-app task totals disagree
            }
            int partition = partitionIndex(entry.getKey());
            if (partition >= 0 && partition >= declaration.taskTotal()) {
                return true;                                   // task index beyond the declared total
            }
        }
        return false;
    }

    /** The apps a commit depends on: the proposed new roster plus the current committed members. */
    private Set<String> cohortScope(Set<String> rNew) {
        Set<String> scope = new HashSet<>(rNew);
        scope.addAll(committedRoster);
        return scope;
    }

    /** The partition index encoded in a {@code appId/subtopology_partition} member id, or {@code -1} if
     * it cannot be parsed (a malformed or app-id-less id — ignored for the index invariant, never a throw
     * that would break fold determinism). */
    private static int partitionIndex(String memberId) {
        int slash = memberId.lastIndexOf('/');
        String taskId = slash >= 0 ? memberId.substring(slash + 1) : memberId;
        int underscore = taskId.lastIndexOf('_');
        if (underscore < 0) {
            return -1;
        }
        try {
            return Integer.parseInt(taskId.substring(underscore + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // ---- the unified commit predicate ------------------------------------------------------------

    /**
     * The commit to append for the currently-open round, or empty if it is not (yet) committable — the
     * single source of truth for both proposing a commit ({@link ParsleyEpochRuntime#driveCommit}) and
     * validating one on apply ({@link #applyCommit}). A round is committable iff: a round is open; the
     * voters unanimously agree on one roster {@code R_new}; there is no cohort contradiction; every app
     * in {@code R_new} not already committed is cohort-complete; every running member has published; and
     * the mesh holds over {@code R_new}. The floor is {@code mergeMin(publications)} clamped monotonically
     * against the last committed floor — empty at genesis (no running members, no publications).
     */
    Optional<ParsleyEpochEvent.EpochCommitted> evaluateCommit() {
        if (roundOwner == null) {
            return Optional.empty();
        }
        Set<String> rNew = unanimousRoster();
        if (rNew == null || hasCohortConflict(cohortScope(rNew))) {
            return Optional.empty();
        }
        for (String app : rNew) {
            if (!committedRoster.contains(app) && !isAppCohortComplete(app)) {
                return Optional.empty();                       // a new app's cohort is not yet complete
            }
        }
        if (!publications.keySet().containsAll(runningMembers)) {
            return Optional.empty();
        }
        if (!isMeshSatisfied(rNew)) {
            return Optional.empty();
        }
        ParsleyVectorClock lowerBounds = null;
        for (ParsleyVectorClock published : publications.values()) {
            lowerBounds = (lowerBounds == null) ? published : lowerBounds.mergeMin(published);
        }
        ParsleyVectorClock floor = (lowerBounds == null ? ParsleyVectorClock.empty() : lowerBounds).merge(committedLowerBounds);
        return Optional.of(new ParsleyEpochEvent.EpochCommitted(nextEpochId(), floor, Set.copyOf(rNew)));
    }

    /** Whether every member whose app is in {@code roster} covers the whole domain that roster spans —
     * full mesh over {@code roster}. An epoch must never commit while any member that will be running
     * under it cannot see the full domain. */
    private boolean isMeshSatisfied(Set<String> roster) {
        Set<String> domain = domainOf(roster);
        for (Declaration declaration : declarations.values()) {
            if (!roster.contains(declaration.appId())) {
                continue;
            }
            Set<String> missing = new HashSet<>(domain);
            missing.removeAll(declaration.inputTopics());
            missing.removeAll(declaration.sinkTopics());
            if (!missing.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Set<String> domainOf(Set<String> roster) {
        Set<String> domain = new HashSet<>();
        for (Declaration declaration : declarations.values()) {
            if (roster.contains(declaration.appId())) {
                domain.addAll(declaration.inputTopics());
                domain.addAll(declaration.sinkTopics());
            }
        }
        return domain;
    }

    // ---- admission gate (consulted by the join waiter and the continuous roster check) -----------

    /** The apps admissible right now: the committed members, plus every app named in EVERY voting
     * member's current roster view (the committed members have unanimously acknowledged it). When there
     * are no voters (genesis with nothing declared, or every committed member has left) every declared
     * app is admissible. Consulted — via the runtime's mirror, from the join waiter's task thread — to
     * fail a never-admissible app fast instead of burning its join budget; a newcomer no committed member
     * has named yet is not in the set and simply waits. */
    Set<String> admissibleApps() {
        Set<String> admissible = new HashSet<>(committedRoster);
        Set<String> voters = votingApps();
        if (voters.isEmpty()) {
            admissible.addAll(declaredApps());
            return admissible;
        }
        Set<String> named = null;
        for (Declaration declaration : declarations.values()) {
            if (voters.contains(declaration.appId())) {
                named = (named == null) ? new HashSet<>(declaration.rosterView())
                        : intersect(named, declaration.rosterView());
            }
        }
        if (named != null) {
            admissible.addAll(named);
        }
        return admissible;
    }

    private static Set<String> intersect(Set<String> a, Set<String> b) {
        Set<String> result = new HashSet<>(a);
        result.retainAll(b);
        return result;
    }

    /** The app-id membership the last commit established (empty before genesis). */
    Set<String> committedRoster() {
        return committedRoster;
    }

    /** A snapshot of every member's admission cut (see {@link #admissionFloors}) — mirrored by the
     * runtime so a restarting task can read its own admission floor from its own thread. */
    Map<String, ParsleyEpochRuntime.CommittedEpoch> admissionFloorsSnapshot() {
        return Map.copyOf(admissionFloors);
    }

    /** Consumes (read-once, resetting to {@code -1}) the epoch id of the last commit event this fold
     * rejected as invalid, or {@code -1} if none is pending — read by the runtime to clear its
     * append-once latch exactly once per rejection, so it re-proposes at most one commit rather than
     * re-appending a duplicate on every poll until the re-append round-trips. */
    long consumeLastRejectedCommitEpoch() {
        long rejected = lastRejectedCommitEpoch;
        lastRejectedCommitEpoch = -1;
        return rejected;
    }

    /**
     * A human-readable reason an open round is not committable — for edge-triggered diagnostics only —
     * or empty when there is no round, the round is committable, or the sole blocker is running members
     * that have not yet published (which {@code ParsleyEpochRuntime#logBlockedRoundTransitions} reports
     * with its own stall debounce). Names the failing conjunct of {@link #evaluateCommit()} — a diverging
     * roster, a cohort still short of an app's task total, or an unmet mesh — so a genesis or roster
     * change that waits forever waits <em>loudly</em> (the only signal for an undercount or a never-
     * deployed roster app).
     */
    Optional<String> whyRoundNotCommittable() {
        if (roundOwner == null || evaluateCommit().isPresent()) {
            return Optional.empty();
        }
        Set<String> rNew = unanimousRoster();
        if (rNew == null) {
            return Optional.of("member-app roster does not agree among voters " + votingApps()
                    + " (a roster change in flight, or incompatible configs)");
        }
        if (hasCohortConflict(cohortScope(rNew))) {
            return Optional.of("cohort conflict: an app's task totals disagree, or a task index exceeds its "
                    + "app's declared total (partition-count instability)");
        }
        for (String app : rNew) {
            if (!committedRoster.contains(app) && !isAppCohortComplete(app)) {
                return Optional.of("cohort incomplete: roster app '" + app + "' has not declared its full "
                        + "task set — waiting for every task of '" + app + "' to join");
            }
        }
        if (!publications.keySet().containsAll(runningMembers)) {
            return Optional.empty();                           // unpublished-running is logged elsewhere
        }
        if (!isMeshSatisfied(rNew)) {
            return Optional.of("full mesh not satisfied over roster " + rNew
                    + " — some member's subscriptions do not cover the whole domain " + domainOf(rNew));
        }
        return Optional.of("round not committable");
    }

    // ---- diagnostics ----------------------------------------------------------------------------

    /** The roster-agreement state, for logging only (never a commit decision): AGREE when the voters
     * declare one roster, CONVERGING when their views form a chain under subset (a change in flight),
     * CONFLICT when they are incomparable (a genuine misconfig) or a cohort contradiction exists. */
    RosterAgreement rosterAgreement() {
        Set<String> voters = votingApps();
        Set<String> scope = new HashSet<>(voters);
        scope.addAll(committedRoster);
        if (hasCohortConflict(scope)) {
            return RosterAgreement.CONFLICT;
        }
        List<Set<String>> views = new ArrayList<>();
        for (Declaration declaration : declarations.values()) {
            if (voters.contains(declaration.appId())) {
                views.add(declaration.rosterView());
            }
        }
        if (views.isEmpty()) {
            return RosterAgreement.AGREE;
        }
        Set<String> first = views.get(0);
        boolean allEqual = true;
        for (Set<String> view : views) {
            if (!view.equals(first)) {
                allEqual = false;
                break;
            }
        }
        if (allEqual) {
            return RosterAgreement.AGREE;
        }
        for (Set<String> a : views) {
            for (Set<String> b : views) {
                if (!a.containsAll(b) && !b.containsAll(a)) {
                    return RosterAgreement.CONFLICT;
                }
            }
        }
        return RosterAgreement.CONVERGING;
    }

    /** The roster-agreement states, for edge-triggered logging. */
    enum RosterAgreement { AGREE, CONVERGING, CONFLICT }

    // ---- accessors used by the runtime / mirrors -------------------------------------------------

    long committedEpochId() {
        return committedEpochId;
    }

    ParsleyVectorClock committedLowerBounds() {
        return committedLowerBounds;
    }

    long nextEpochId() {
        return committedEpochId + 1;
    }

    boolean isRoundOpen() {
        return roundOwner != null;
    }

    @Nullable String roundOwner() {
        return roundOwner;
    }

    Set<String> runningMembers() {
        return Set.copyOf(runningMembers);
    }

    /** Every topic any member in the effective roster (committed after genesis, all declared before)
     * consumes or produces — the domain a joining task self-checks its coverage against. */
    Set<String> domainTopics() {
        return domainOf(effectiveRoster());
    }

    /** The effective roster for data-plane views: the committed roster after genesis, all declared apps
     * before it. */
    private Set<String> effectiveRoster() {
        return committedEpochId == 0 ? declaredApps() : committedRoster;
    }

    /** The topology's external source topics over the effective roster — {@code ∪inputTopics −
     * ∪sinkTopics}. A topic some member consumes but none produces is an external entry point. */
    Set<String> externalSourceTopics() {
        Set<String> roster = effectiveRoster();
        Set<String> inputs = new HashSet<>();
        Set<String> sinks = new HashSet<>();
        for (Declaration declaration : declarations.values()) {
            if (roster.contains(declaration.appId())) {
                inputs.addAll(declaration.inputTopics());
                sinks.addAll(declaration.sinkTopics());
            }
        }
        inputs.removeAll(sinks);
        return inputs;
    }

    Set<String> externalSourceTopicsAsOfPreviousCommit() {
        return previousCommitExternalSourceTopics;
    }

    boolean isRunningMember(String memberId) {
        return runningMembers.contains(memberId);
    }

    Set<String> unpublishedRunningMembers() {
        Set<String> outstanding = new HashSet<>(runningMembers);
        outstanding.removeAll(publications.keySet());
        return outstanding;
    }
}
