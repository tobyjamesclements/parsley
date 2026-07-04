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
 *   <li>The first {@link EpochEvent.SnapshotRequested} after the last {@link EpochEvent.EpochCommitted}
 *       <strong>opens</strong> the round and elects its author as {@linkplain #roundOwner() owner};
 *       later requests (and joins) while the round is open <strong>coalesce</strong> into it.
 *   <li>Running members publish ({@link EpochEvent.FrontierPublished}); the round is
 *       {@linkplain #isRoundComplete() complete} once every running member has published.
 *   <li>The owner then commits {@link #proposeCommit()} = {@code (nextEpochId, mergeMin(published))};
 *       applying the resulting {@link EpochEvent.EpochCommitted} advances the settled epoch, promotes
 *       pending joiners to running, and clears the round.
 * </ul>
 *
 * <p>Membership: a {@link EpochEvent.JoinRequested} member is <em>pending</em> until the next commit,
 * then <em>running</em>. Running members are the ones whose publication a round waits for and whose
 * frontiers are folded into {@code lowerBounds}; a joiner (blocked, not yet consuming) publishes nothing
 * and does not constrain the cut. A {@link EpochEvent.Leave} removes a member (a graceful leave or an
 * eviction). Each {@link EpochEvent.JoinRequested} also declares the member's input/sink topics, from
 * which {@link #externalSourceTopics()} derives the DAG-wide source-topic registry.
 *
 * <p>Not thread-safe: a single consumer thread applies the log in order.
 */
final class ParsleyEpochLog {

    private long committedEpochId;                             // last committed epoch; 0 = none yet
    private final Set<String> runningMembers = new HashSet<>();
    private final Set<String> pendingJoiners = new HashSet<>();
    private @Nullable String roundOwner;                       // non-null iff a round is open
    private final Map<String, ParsleyClock> publications = new HashMap<>();  // current round only
    // Each declared member's input/sink topics — the DAG-wide source-topic registry. Keyed by memberId,
    // populated on JoinRequested (pending or running), removed on Leave; persists across commits.
    private final Map<String, MemberTopology> declarations = new HashMap<>();

    /** A member's declared input channels (topics it consumes) and sink topics (topics it produces). */
    record MemberTopology(Set<String> inputTopics, Set<String> sinkTopics) {}

    /** Applies one event in log order, updating the folded state. */
    void apply(EpochEvent event) {
        switch (event) {
            case EpochEvent.JoinRequested e -> {
                // A member already running does not re-join; otherwise it is pending until the next commit.
                if (!runningMembers.contains(e.memberId())) {
                    pendingJoiners.add(e.memberId());
                }
                // Record its declared topology for the source-topic registry (structural, so it counts as
                // soon as declared — pending or running — see externalSourceTopics()).
                declarations.put(e.memberId(), new MemberTopology(e.inputTopics(), e.sinkTopics()));
            }
            case EpochEvent.SnapshotRequested e -> {
                // First request after the last commit opens the round and elects the owner; the rest
                // coalesce (a round is already open).
                if (roundOwner == null) {
                    roundOwner = e.memberId();
                }
            }
            case EpochEvent.FrontierPublished e -> {
                // A publication counts only for the open round and only from a running member; a stray
                // publication (no round, or from a not-yet-running joiner) is ignored. Last write wins.
                if (roundOwner != null && runningMembers.contains(e.memberId())) {
                    publications.put(e.memberId(), e.completeness());
                }
            }
            case EpochEvent.Leave e -> {
                // Remove the member from the domain — a graceful leave or an eviction of a silent member.
                // Dropping it from the open round's publications keeps the completeness check honest (a
                // left member is no longer awaited). Idempotent: a Leave for a non-member is a no-op.
                runningMembers.remove(e.memberId());
                pendingJoiners.remove(e.memberId());
                publications.remove(e.memberId());
                declarations.remove(e.memberId());
            }
            case EpochEvent.EpochCommitted e -> {
                // Dedup by epochId: the first commit for an epoch is authoritative; a stale or duplicate
                // one (owner-plus-takeover, or a re-append) is ignored. Without this guard a duplicate
                // EpochCommitted(E+1) landing after round N+1 has opened would wrongly clear that round.
                if (e.epochId() <= committedEpochId) {
                    break;
                }
                // Adopt the commit: advance the settled epoch, promote joiners, close the round.
                committedEpochId = e.epochId();
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

    /** Whether {@code memberId} is currently a running member — the join block waits until this is true. */
    boolean isRunningMember(String memberId) {
        return runningMembers.contains(memberId);
    }

    /** The running members that have not yet published for the open round — eviction candidates once a round waits too long. */
    Set<String> unpublishedRunningMembers() {
        Set<String> outstanding = new HashSet<>(runningMembers);
        outstanding.removeAll(publications.keySet());
        return outstanding;
    }

    /**
     * Whether the open round is complete — every running member has published its frontier — so the
     * owner may commit. Always {@code false} when no round is open. A round with no running members
     * (e.g. the very first join into an empty topology) is vacuously complete.
     */
    boolean isRoundComplete() {
        return roundOwner != null && publications.keySet().containsAll(runningMembers);
    }

    /**
     * The commit the owner should write for the now-complete round: {@code (nextEpochId, lowerBounds)}
     * where {@code lowerBounds} is the {@link ParsleyClock#mergeMin} fold of the published frontiers —
     * per coordinate, the minimum over the members that observed it (a member without a coordinate does
     * not constrain it). With no publications this is an empty clock (no coordinate bounded), i.e. the
     * epoch-0 floor.
     *
     * @throws IllegalStateException if the round is not complete
     */
    EpochEvent.EpochCommitted proposeCommit() {
        if (!isRoundComplete()) {
            throw new IllegalStateException("cannot commit: round not open or not all running members published");
        }
        ParsleyClock lowerBounds = null;
        for (ParsleyClock published : publications.values()) {
            lowerBounds = (lowerBounds == null) ? published : lowerBounds.mergeMin(published);
        }
        return new EpochEvent.EpochCommitted(nextEpochId(),
                lowerBounds == null ? ParsleyClock.empty() : lowerBounds);
    }
}
