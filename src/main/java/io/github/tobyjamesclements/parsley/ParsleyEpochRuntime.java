package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

/**
 * The per-instance handle that drives the leaderless epoch protocol over a {@link ParsleyEpochTransport}.
 * One runtime per application instance folds the shared {@code epoch-events} log through a {@link
 * ParsleyEpochLog} and, for the rounds one of <em>its own</em> members owns, performs the owner's
 * <em>collect → commit</em>. Because the log is totally ordered and the fold deterministic, every
 * instance reaches the identical view — round ownership, membership, and the committed lower bounds —
 * with no leader.
 *
 * <p>Modelled on {@link ParsleyQuiesce}: a shared handle the application creates and every participating
 * task registers with (via {@link #join}). A single background thread ({@link #start}) drives the loop;
 * task threads only enqueue intents ({@link #join}, {@link #requestSnapshot}, {@link #publishFrontier}),
 * which the runtime thread appends — so the transport's non-thread-safe consumer is only ever touched by
 * the one runtime thread. The loop body {@link #runOnce} is exposed package-private so tests can drive it
 * synchronously against an in-memory transport with no thread at all.
 *
 * <p>This workstream (WS4a-cont) builds and tests the runtime in isolation; it is not yet wired into
 * {@link ParsleyProcessor} (marker relay and the publish seam are WS4b/WS4c). With no runtime created,
 * a topology runs in epoch 0 exactly as before.
 */
final class ParsleyEpochRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ParsleyEpochRuntime.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final ParsleyEpochTransport transport;
    private final ParsleyEpochLog fold = new ParsleyEpochLog();
    // How a blocked round treats members that have not published. The default (blockUntilDrained) never
    // excludes, so a transition waits for every member — only a drained member may ever be excluded, and a
    // crashed member is never known to be drained.
    private final ParsleyMembershipStrategy membershipStrategy;

    // Members whose tasks live on this instance: the runtime folds and commits on their behalf. Written
    // from task threads (join), read by the runtime thread (driveCommit).
    private final Set<String> localMembers = ConcurrentHashMap.newKeySet();
    // Local members whose causal buffer is currently empty (drained), reported by their task thread.
    // leave() waits until every local member is drained before appending its Leave — "only a drained node
    // is excluded". Written by task threads (reportDrained), read by the caller's leave() thread.
    private final Set<String> drainedLocalMembers = ConcurrentHashMap.newKeySet();
    // A local member's live completeness snapshot, registered once its task's engine exists (see
    // registerLocalCompleteness). Lets the runtime thread publish on a member's behalf when its own task
    // thread cannot run pollEpochCoordination() — see autoPublishStalledLocalMembers.
    private final Map<String, Supplier<ParsleyClock>> localCompletenessSuppliers = new ConcurrentHashMap<>();
    // Intents enqueued by task threads, appended to the log by the runtime thread only.
    private final Queue<ParsleyEpochEvent> outbox = new ConcurrentLinkedQueue<>();

    // Runtime-thread-only: the epoch we have already appended a commit for, so this node appends each
    // round's EpochCommitted exactly once (the commit round-trips through the log and advances the fold).
    private long lastCommitAppendedFor;
    // Read/written by the runtime thread alone in autoPublishStalledLocalMembers, but removed from by
    // unregisterMember, which task threads call (via ParsleyProcessor#close). Concurrent, like every other
    // member-keyed collection here, for that cross-thread write.
    private final Map<String, Long> lastAutoPublishedRound = new ConcurrentHashMap<>();

    // Mirrors of the folded decision, published for cross-thread readers (a source-layer task polls these
    // to drive the in-band wave; a joiner blocks on committedEpochId). Volatile: written by the runtime
    // thread, read by any thread.
    private volatile long committedEpochId;
    private volatile ParsleyClock committedLowerBounds = ParsleyClock.empty();
    private volatile boolean roundOpen;
    // Whether the transport has folded the whole startup backlog. The owner must not commit before this,
    // or a just-started runtime would commit a stale epoch believing the topology empty.
    private volatile boolean bootstrapped;
    // Snapshot of the running-member set, for the join block to read from any thread.
    private volatile Set<String> runningMembersMirror = Set.of();
    // Snapshot of the running members that still owe a publication for the open round, so any task thread
    // can tell whether it must (re)publish — the mechanism that makes publication restart-safe.
    private volatile Set<String> unpublishedMembersMirror = Set.of();
    // Mirror of the fold's DAG-wide external source topics, refreshed each runOnce for cross-thread readers.
    private volatile Set<String> externalSourceTopicsMirror = Set.of();

    private volatile boolean running;
    private @Nullable Thread thread;

    /** A runtime with the default {@link ParsleyMembershipStrategy#blockUntilDrained() block-until-drained} strategy. */
    ParsleyEpochRuntime(ParsleyEpochTransport transport) {
        this(transport, ParsleyMembershipStrategy.blockUntilDrained());
    }

    ParsleyEpochRuntime(ParsleyEpochTransport transport, ParsleyMembershipStrategy membershipStrategy) {
        this.transport = transport;
        this.membershipStrategy = membershipStrategy;
    }

    /**
     * Announces {@code memberId} on the log and registers it as local, so this runtime folds and commits
     * on its behalf. A task calls this once it is participating, declaring its {@code inputTopics} (the
     * channels it consumes) and {@code sinkTopics} (the topics it produces) for the DAG-wide source-topic
     * registry (see {@link #externalSourceTopics()}).
     */
    void join(String memberId, Set<String> inputTopics, Set<String> sinkTopics) {
        localMembers.add(memberId);
        outbox.add(new ParsleyEpochEvent.JoinRequested(memberId, Set.copyOf(inputTopics), Set.copyOf(sinkTopics)));
    }

    /** Stops treating {@code memberId} as local (its task left this instance, e.g. a rebalance). Does not append a log event. */
    void unregisterMember(String memberId) {
        localMembers.remove(memberId);
        drainedLocalMembers.remove(memberId);
        localCompletenessSuppliers.remove(memberId);
        lastAutoPublishedRound.remove(memberId);
    }

    /**
     * Registers {@code completenessSupplier} as {@code memberId}'s live completeness snapshot, so
     * {@link #autoPublishStalledLocalMembers()} can publish on its behalf from the runtime thread alone —
     * without needing {@code memberId}'s own task thread to run {@code pollEpochCoordination()}. Deliberately
     * separate from {@link #join}: a task calls this only once its engine exists and the snapshot is
     * meaningful (see {@code ParsleyProcessor#init}), not at join time, when a fresh joiner's snapshot would
     * still be the empty placeholder and a restarting member's would not yet reflect its restored state.
     */
    void registerLocalCompleteness(String memberId, Supplier<ParsleyClock> completenessSupplier) {
        localCompletenessSuppliers.put(memberId, completenessSupplier);
    }

    /**
     * A task reports whether its causal buffer is currently empty (drained). {@link #allLocalMembersDrained()}
     * folds these across the instance's members so {@link ParsleyCoordination#leave()} can wait until every
     * local member is drained before removing it — "only a drained node is excluded".
     */
    void reportDrained(String memberId, boolean empty) {
        if (empty) {
            drainedLocalMembers.add(memberId);
        } else {
            drainedLocalMembers.remove(memberId);
        }
    }

    /** Whether every local member's causal buffer is currently empty — the gate {@code leave()} waits on. */
    boolean allLocalMembersDrained() {
        return !localMembers.isEmpty() && drainedLocalMembers.containsAll(localMembers);
    }

    /** Whether any local member is still a running member on the log — {@code leave()} waits for this to be false after appending Leave. */
    boolean hasRunningLocalMembers() {
        for (String member : localMembers) {
            if (runningMembersMirror.contains(member)) {
                return true;
            }
        }
        return false;
    }

    /** Any member local to this instance, or {@code null} if none has joined — used to attribute an operator-triggered snapshot to a local owner. */
    @Nullable String anyLocalMember() {
        for (String member : localMembers) {
            return member;
        }
        return null;
    }

    /** Proposes a snapshot round on behalf of {@code memberId}; the first such request after the last commit opens and owns the round. */
    void requestSnapshot(String memberId) {
        outbox.add(new ParsleyEpochEvent.SnapshotRequested(memberId));
    }

    /** Publishes {@code memberId}'s current completeness frontier for the open round. */
    void publishFrontier(String memberId, ParsleyClock completeness) {
        outbox.add(new ParsleyEpochEvent.FrontierPublished(memberId, completeness));
    }

    /**
     * Gracefully removes every local member from the domain (a decommission). Marked self-initiated so
     * folding the resulting {@link ParsleyEpochEvent.Leave} is not mistaken for an eviction and does not trigger a
     * re-join. A restart, by contrast, does not call this — the member stays in the domain and returns.
     */
    void leaveLocalMembers() {
        for (String member : localMembers) {
            outbox.add(new ParsleyEpochEvent.Leave(member));
        }
    }

    /** Whether {@code memberId} is currently a running member (folded from the log) — the join block waits on this. */
    boolean isRunningMember(String memberId) {
        return runningMembersMirror.contains(memberId);
    }

    /** The topology's external source topics, derived DAG-wide from every declared member's declaration. */
    Set<String> externalSourceTopics() {
        return externalSourceTopicsMirror;
    }

    /**
     * Whether {@code memberId} owes a publication for the currently-open round — a running member that has
     * not yet published its frontier. A task polls this and (re)publishes its completeness when true, so a
     * member that restarts mid-round re-publishes off the folded log alone, without depending on having
     * consumed a one-shot in-band snapshot marker exactly once.
     */
    boolean owesPublication(String memberId) {
        return roundOpen && unpublishedMembersMirror.contains(memberId);
    }

    /** The last committed epoch id ({@code 0} before any commit). */
    long committedEpochId() {
        return committedEpochId;
    }

    /** The lower bounds of the last committed epoch (empty before any commit). */
    ParsleyClock committedLowerBounds() {
        return committedLowerBounds;
    }

    /** Whether a snapshot round is currently open (a source-layer task publishes + injects on this). */
    boolean isRoundOpen() {
        return roundOpen;
    }

    /** Whether the runtime has folded the whole startup backlog, so {@link #committedEpochId()} is accurate — the join wait blocks on this. */
    boolean isBootstrapped() {
        return bootstrapped;
    }

    /** Starts the background thread that drives the protocol until {@link #close}. Idempotent. */
    synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        // The runtime thread owns the transport's whole lifecycle: it polls in the loop and closes the
        // transport when the loop exits. That keeps the non-thread-safe consumer touched by only this one
        // thread — close() never reaches into a poll in progress; it just signals and joins.
        Thread t = new Thread(() -> {
            try {
                while (running) {
                    runOnce();
                }
            } finally {
                closeTransportQuietly();
            }
        }, "parsley-epoch-runtime");
        t.setDaemon(true);
        this.thread = t;
        t.start();
    }

    /**
     * One iteration of the drive loop: append any enqueued intents, fold the events appended since the
     * last poll, then — if a round this instance owns has become complete — commit it. Package-private so
     * tests exercise the exact production code path synchronously; the background thread just calls this
     * repeatedly.
     */
    void runOnce() {
        ParsleyEpochEvent pending;
        while ((pending = outbox.poll()) != null) {
            transport.append(pending);
        }
        for (ParsleyEpochEvent event : transport.poll(POLL_TIMEOUT)) {
            fold.apply(event);
            if (event instanceof ParsleyEpochEvent.EpochCommitted commit && commit.epochId() > committedEpochId) {
                committedEpochId = commit.epochId();
                committedLowerBounds = commit.lowerBounds();
                log.debug("Epoch {} committed with lower bounds {}", commit.epochId(), commit.lowerBounds());
            }
        }
        roundOpen = fold.isRoundOpen();
        runningMembersMirror = fold.runningMembers();
        unpublishedMembersMirror = fold.unpublishedRunningMembers();
        externalSourceTopicsMirror = fold.externalSourceTopics();
        bootstrapped = transport.caughtUp();
        driveCommit();
        applyMembershipStrategy();
        autoPublishStalledLocalMembers();
    }

    /**
     * Publishes a stalled local member's registered completeness snapshot on its behalf — the fix for the
     * deadlock where a member's own task thread can never run {@code pollEpochCoordination()} because it
     * shares a Kafka Streams {@code StreamThread} with another task blocked in {@code awaitJoinCommit}'s
     * unbounded join wait (see BACKLOG.md). This method runs on the runtime's own background thread, which
     * is distinct from every {@code StreamThread} and so keeps making progress even while one is wedged.
     *
     * <p>Safe unconditionally, with no liveness heuristic: completeness only ever advances, and the
     * committed floor is already a conservative merge-min, so publishing a possibly-stale snapshot instead
     * of the freshest one can only make the resulting floor more conservative, never unsafe. Harmless and
     * redundant with a healthy member's own publish from {@code pollEpochCoordination()} — {@link
     * ParsleyEpochLog#apply} dedups a {@code FrontierPublished} by last-write-wins, and this runtime's own
     * publish is guarded to at most one append per member per round while it round-trips back through the
     * fold, mirroring {@link #driveCommit()}'s {@code lastCommitAppendedFor} guard.
     */
    private void autoPublishStalledLocalMembers() {
        if (!bootstrapped || !fold.isRoundOpen()) {
            return;
        }
        long round = fold.nextEpochId();
        Set<String> outstanding = fold.unpublishedRunningMembers();
        for (String member : localMembers) {
            if (!outstanding.contains(member)) {
                continue;
            }
            Long lastRound = lastAutoPublishedRound.get(member);
            if (lastRound != null && lastRound == round) {
                continue;
            }
            Supplier<ParsleyClock> supplier = localCompletenessSuppliers.get(member);
            if (supplier == null) {
                continue;
            }
            transport.append(new ParsleyEpochEvent.FrontierPublished(member, supplier.get()));
            lastAutoPublishedRound.put(member, round);
        }
    }

    /**
     * Consults the {@link ParsleyMembershipStrategy} while a round is blocked and appends a {@link
     * ParsleyEpochEvent.Leave} for any member it says may be excluded. The default {@link
     * ParsleyMembershipStrategy#blockUntilDrained()} returns none, so the round simply waits for every
     * member to publish — only a drained member may ever be excluded, and a crashed member is never known
     * to be drained. The log serialises the decision and dedup makes a duplicate Leave a no-op.
     */
    private void applyMembershipStrategy() {
        if (!bootstrapped || !fold.isRoundOpen() || localMembers.isEmpty()) {
            return;
        }
        Set<String> outstanding = fold.unpublishedRunningMembers();
        if (outstanding.isEmpty()) {
            return;
        }
        Set<String> excludable = membershipStrategy.excludableMembers(
                new ParsleyBlockedRound(outstanding, fold.runningMembers()));
        for (String member : excludable) {
            transport.append(new ParsleyEpochEvent.Leave(member));
        }
    }

    /**
     * Collect → commit, leaderless: once an open round is complete (every running member has published),
     * <em>any</em> node with a local member appends the {@code EpochCommitted}. The {@link
     * ParsleyEpochLog#proposeCommit() merge-min} is a deterministic function of the published frontiers,
     * so every node computes the identical commit, and dedup-by-{@code epochId} makes the concurrent
     * appends idempotent — there is no single owner to fail (a gone owner cannot freeze the epoch). Guarded
     * so this node appends each round's commit at most once; the commit is read back through the log and
     * folded like any other event.
     */
    private void driveCommit() {
        // Never commit before the whole startup backlog is folded: committedEpochId would be stale and
        // the round would be decided against a topology this runtime has not yet fully observed.
        if (!bootstrapped || !fold.isRoundOpen() || !fold.isRoundComplete()) {
            return;
        }
        // Only nodes with skin in the game (a local member) commit, to bound the duplicate appends.
        if (localMembers.isEmpty()) {
            return;
        }
        long epochToCommit = fold.nextEpochId();
        if (epochToCommit <= lastCommitAppendedFor) {
            return;
        }
        transport.append(fold.proposeCommit());
        lastCommitAppendedFor = epochToCommit;
    }

    @Override
    public synchronized void close() {
        running = false;
        Thread t = this.thread;
        if (t == null) {
            // Never started (e.g. tests driving runOnce synchronously): close the transport directly —
            // no other thread can be touching it.
            closeTransportQuietly();
            return;
        }
        // Signal and wait; the loop returns from its 100ms poll, sees running=false, exits, and closes
        // the transport itself. No interrupt — interrupting a consumer poll is messy and unnecessary.
        try {
            t.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.thread = null;
    }

    private void closeTransportQuietly() {
        try {
            transport.close();
        } catch (Exception e) {
            log.warn("Failed to close epoch transport", e);
        }
    }
}
