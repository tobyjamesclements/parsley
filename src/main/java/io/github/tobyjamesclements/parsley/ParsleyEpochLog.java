package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The deterministic fold over the {@code epoch-events} log — the heart of the leaderless epoch protocol.
 * Every node applies the log's events in total order through an instance of this class and, because the
 * fold is a pure function of the ordered events, they all agree on round ownership, membership, round
 * completeness, and the committed {@code lowerBounds} without any leader or lock.
 *
 * <p>Round lifecycle (a "round" is defined by log position, not by any id in the events):
 * <ul>
 *   <li>The first {@link ParsleyEpochEvent.SnapshotRequested} after the last {@link ParsleyEpochEvent.EpochCommitted}
 *       <strong>opens</strong> the round and elects its author as {@linkplain #roundOwner() owner};
 *       later requests (and joins) while the round is open <strong>coalesce</strong> into it.
 *   <li>Running members publish ({@link ParsleyEpochEvent.FrontierPublished}); the round is
 *       {@linkplain #isRoundComplete() complete} once every running member has published.
 *   <li>The owner then commits {@link #proposeCommit()} = {@code (nextEpochId, mergeMin(published))};
 *       applying the resulting {@link ParsleyEpochEvent.EpochCommitted} advances the settled epoch, promotes
 *       pending joiners to running, and clears the round.
 * </ul>
 *
 * <p>Membership: a {@link ParsleyEpochEvent.JoinRequested} member is <em>pending</em> until the next commit,
 * then <em>running</em>. Running members are the ones whose publication a round waits for and whose
 * frontiers are folded into {@code lowerBounds}; a joiner (blocked, not yet consuming) publishes nothing
 * and does not constrain the cut. A {@link ParsleyEpochEvent.Leave} removes a member — appended only by a
 * graceful, explicit decommission; there is no automatic eviction. Each {@link ParsleyEpochEvent.JoinRequested}
 * also declares the member's input/sink topics, from
 * which {@link #externalSourceTopics()} derives the DAG-wide source-topic registry.
 *
 * <p>Not thread-safe: a single consumer thread applies the log in order.
 */
final class ParsleyEpochLog {

    private long committedEpochId;                             // last committed epoch; 0 = none yet
    // The last committed epoch's lowerBounds — consulted by proposeCommit to clamp the next round's
    // mergeMin so a per-coordinate floor can never regress across epochs (see proposeCommit's Javadoc).
    private ParsleyClock committedLowerBounds = ParsleyClock.empty();
    private final Set<String> runningMembers = new HashSet<>();
    private final Set<String> pendingJoiners = new HashSet<>();
    private @Nullable String roundOwner;                       // non-null iff a round is open
    private final Map<String, ParsleyClock> publications = new HashMap<>();  // current round only
    // Each declared member's input/sink topics — the DAG-wide source-topic registry. Keyed by memberId,
    // populated on JoinRequested (pending or running), removed on Leave; persists across commits.
    private final Map<String, MemberTopology> declarations = new HashMap<>();
    // A two-slot shift register of externalSourceTopics() snapshots, taken at each commit: "current" is
    // the registry as of the most recent commit, "previous" as of the one before that. Gives every node —
    // including one that only just started and has no other memory — a purely log-derived answer to "what
    // was external one commit ago", the exact set a departing topic's outgoing self-adopter needs to give
    // it one more adoption cycle (see ParsleyProcessor#pollEpochCoordination's handoff grace period). Not
    // task-local, unlike the field this replaced: every node computes the same value from the same log.
    private Set<String> previousCommitExternalSourceTopics = Set.of();
    private Set<String> currentCommitExternalSourceTopics = Set.of();

    /** A member's declared input channels (topics it consumes) and sink topics (topics it produces). */
    record MemberTopology(Set<String> inputTopics, Set<String> sinkTopics) {}

    /** Applies one event in log order, updating the folded state. */
    void apply(ParsleyEpochEvent event) {
        switch (event) {
            case ParsleyEpochEvent.JoinRequested e -> {
                // A member already running does not re-join; otherwise it is pending until the next commit.
                if (!runningMembers.contains(e.memberId())) {
                    pendingJoiners.add(e.memberId());
                }
                // Record its declared topology for the source-topic registry (structural, so it counts as
                // soon as declared — pending or running — see externalSourceTopics()).
                declarations.put(e.memberId(), new MemberTopology(e.inputTopics(), e.sinkTopics()));
            }
            case ParsleyEpochEvent.SnapshotRequested e -> {
                // First request after the last commit opens the round and elects the owner; the rest
                // coalesce (a round is already open).
                if (roundOwner == null) {
                    roundOwner = e.memberId();
                }
            }
            case ParsleyEpochEvent.FrontierPublished e -> {
                // A publication counts only for the open round and only from a running member; a stray
                // publication (no round, or from a not-yet-running joiner) is ignored. Last write wins.
                if (roundOwner != null && runningMembers.contains(e.memberId())) {
                    publications.put(e.memberId(), e.completeness());
                }
            }
            case ParsleyEpochEvent.Leave e -> {
                // Remove the member from the domain — always a graceful, explicit decommission; there is
                // no automatic eviction of a silent member. Dropping it from the open round's publications
                // keeps the completeness check honest (a
                // left member is no longer awaited). Idempotent: a Leave for a non-member is a no-op.
                runningMembers.remove(e.memberId());
                pendingJoiners.remove(e.memberId());
                publications.remove(e.memberId());
                declarations.remove(e.memberId());
            }
            case ParsleyEpochEvent.EpochCommitted e -> {
                // Dedup by epochId: the first commit for an epoch is authoritative; a stale or duplicate
                // one (owner-plus-takeover, or a re-append) is ignored. Without this guard a duplicate
                // EpochCommitted(E+1) landing after round N+1 has opened would wrongly clear that round.
                if (e.epochId() <= committedEpochId) {
                    break;
                }
                // Shift the registry snapshot register before adopting the commit: "current" (as of the
                // commit that just settled) becomes "previous", and a fresh snapshot is taken now — after
                // every declaration up to and including this log position has already folded, exactly
                // mirroring what a running node's own live view would have shown right up to this commit.
                previousCommitExternalSourceTopics = currentCommitExternalSourceTopics;
                currentCommitExternalSourceTopics = externalSourceTopics();
                // Adopt the commit: advance the settled epoch and its floor, promote joiners, close the round.
                committedEpochId = e.epochId();
                committedLowerBounds = e.lowerBounds();
                runningMembers.addAll(pendingJoiners);
                pendingJoiners.clear();
                publications.clear();
                roundOwner = null;
            }
        }
    }

    /** The last committed epoch id ({@code 0} before any commit). */
    long committedEpochId() {
        return committedEpochId;
    }

    /** The epoch id a commit of the current round would carry (strictly increasing). */
    long nextEpochId() {
        return committedEpochId + 1;
    }

    /** Whether a snapshot round is currently open (a request has opened it, no commit yet). */
    boolean isRoundOpen() {
        return roundOwner != null;
    }

    /** The current round's owner (the elected coordinator), or {@code null} if no round is open. */
    @Nullable String roundOwner() {
        return roundOwner;
    }

    /** The members counted in the current cut (joined and committed in a prior epoch). */
    Set<String> runningMembers() {
        return Set.copyOf(runningMembers);
    }

    /**
     * Every topic any declared member (pending or running) consumes or produces — {@code ∪inputTopics ∪
     * sinkTopics} — the domain a full mesh must cover. Unlike {@link #externalSourceTopics()} this does
     * not subtract sinks: a topic one member only produces is still part of the domain a <em>different</em>
     * member consuming it must be able to see, and {@link #missingSubscriptions} needs the whole domain to
     * compute what a given member is missing.
     */
    Set<String> domainTopics() {
        Set<String> domain = new HashSet<>();
        for (MemberTopology declaration : declarations.values()) {
            domain.addAll(declaration.inputTopics());
            domain.addAll(declaration.sinkTopics());
        }
        return domain;
    }

    /**
     * The domain topics {@code memberId} has declared neither as an input nor a sink of its own — empty
     * means this member's own subscriptions fully cover the domain. A member not (yet) declared at all is
     * treated as missing every domain topic.
     */
    Set<String> missingSubscriptions(String memberId) {
        MemberTopology declaration = declarations.get(memberId);
        Set<String> missing = new HashSet<>(domainTopics());
        if (declaration != null) {
            missing.removeAll(declaration.inputTopics());
            missing.removeAll(declaration.sinkTopics());
        }
        return missing;
    }

    /**
     * Whether every <em>running</em> member's own declared subscriptions cover the whole domain — full
     * mesh. Pending joiners are excluded: a joiner's own insufficiency is caught by its own startup
     * self-check (see {@code ParsleyProcessor#init}) rather than blocking every other member's round.
     * Consulted by {@link #isRoundComplete()} — an epoch must never commit, and so never seed a newly
     * required subscriber's floor, while any running member cannot actually see the full domain.
     */
    boolean isFullMeshSatisfied() {
        for (String memberId : runningMembers) {
            if (!missingSubscriptions(memberId).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * The topology's external source topics, derived DAG-wide from every declared member:
     * {@code ∪inputTopics − ∪sinkTopics}. A topic some member consumes but no member produces is an
     * external entry point — no in-band epoch marker ever reaches it, so a stage consuming one must
     * self-initiate the wave and adopt that coordinate's floor from the log. Derived over all declared
     * members (pending or running), so source-layer identity is correct from the very first round.
     */
    Set<String> externalSourceTopics() {
        Set<String> external = new HashSet<>();
        for (MemberTopology declaration : declarations.values()) {
            external.addAll(declaration.inputTopics());
        }
        for (MemberTopology declaration : declarations.values()) {
            external.removeAll(declaration.sinkTopics());
        }
        return external;
    }

    /**
     * {@link #externalSourceTopics()} as it stood immediately after the commit <em>before</em> the most
     * recent one — {@code Set.of()} before two commits have happened. A departing topic's outgoing
     * self-adopter unions this with the current live registry to give that topic exactly one more
     * adoption cycle after its declaring producer's join folds it out of the live view: since a member's
     * {@link ParsleyEpochEvent.JoinRequested} always folds before the commit that admits it, the previous
     * commit's snapshot still includes a topic that departs as part of the very round that commit closes.
     * Purely derived from this log's own fold, so — unlike a per-task in-memory cache — every node
     * (including one that just started, with no other memory) computes the identical answer.
     */
    Set<String> externalSourceTopicsAsOfPreviousCommit() {
        return previousCommitExternalSourceTopics;
    }

    /** Whether {@code memberId} is currently a running member — the join block waits until this is true. */
    boolean isRunningMember(String memberId) {
        return runningMembers.contains(memberId);
    }

    /** The running members that have not yet published for the open round — the reason a round is not yet complete; no automatic eviction, the round simply waits until each has published or an explicit {@link ParsleyEpochEvent.Leave} removes it. */
    Set<String> unpublishedRunningMembers() {
        Set<String> outstanding = new HashSet<>(runningMembers);
        outstanding.removeAll(publications.keySet());
        return outstanding;
    }

    /**
     * Whether the open round is complete — every running member has published its frontier, and every
     * running member's own subscriptions cover the full domain ({@link #isFullMeshSatisfied()}) — so the
     * owner may commit. Always {@code false} when no round is open. A round with no running members
     * (e.g. the very first join into an empty topology) is vacuously complete. The full-mesh conjunct is
     * unconditional, never bypassable by a validation mode: an epoch must never commit — and so never
     * seed a newly required subscriber's floor — while any running member cannot actually see the whole
     * domain, since the completeness it publishes would then be unsound for coordinates it never
     * observes.
     */
    boolean isRoundComplete() {
        return roundOwner != null && publications.keySet().containsAll(runningMembers) && isFullMeshSatisfied();
    }

    /**
     * The commit the owner should write for the now-complete round: {@code (nextEpochId, lowerBounds)}
     * where {@code lowerBounds} is the {@link ParsleyClock#mergeMin} fold of the published frontiers —
     * per coordinate, the minimum over the members that observed it (a member without a coordinate does
     * not constrain it) — then clamped per coordinate to never regress below the previously committed
     * floor via {@link ParsleyClock#merge} (the per-coordinate maximum): a member admitted mid-round that
     * consumes from {@code earliest} publishes completeness far behind the current floor, and the raw
     * {@code mergeMin} would otherwise drag a shared coordinate's floor backwards on promotion. Clamping
     * here, once, keeps the floor honestly monotonic for every consumer instead of requiring each one to
     * guard against a regression that should never have been possible in the first place. With no
     * publications this is an empty clock (no coordinate bounded), i.e. the epoch-0 floor.
     *
     * @throws IllegalStateException if the round is not complete
     */
    ParsleyEpochEvent.EpochCommitted proposeCommit() {
        if (!isRoundComplete()) {
            throw new IllegalStateException("cannot commit: round not open or not all running members published");
        }
        ParsleyClock lowerBounds = null;
        for (ParsleyClock published : publications.values()) {
            lowerBounds = (lowerBounds == null) ? published : lowerBounds.mergeMin(published);
        }
        ParsleyClock proposed = lowerBounds == null ? ParsleyClock.empty() : lowerBounds;
        return new ParsleyEpochEvent.EpochCommitted(nextEpochId(), proposed.merge(committedLowerBounds));
    }
}
