package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The per-instance handle that drives the leaderless epoch protocol over a {@link ParsleyEpochTransport}.
 * One runtime per application instance folds the shared {@code epoch-events} log through a {@link
 * ParsleyEpochLog} and, for the rounds one of <em>its own</em> members owns, performs the owner's
 * <em>collect → commit</em>. Because the log is totally ordered and the fold deterministic, every
 * instance reaches the identical view — round ownership, membership, and the committed lower bounds —
 * with no leader.
 *
 * <p>Modelled on {@link CausalQuiesce}: a shared handle the application creates and every participating
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
    // How long a round may wait for a member to publish before this node evicts it (appends Leave). Must
    // exceed a rolling restart, so a briefly-absent member is not falsely evicted.
    private final Duration evictionTimeout;

    // Members whose tasks live on this instance: the runtime folds and commits on their behalf. Written
    // from task threads (join), read by the runtime thread (driveCommit / eviction).
    private final Set<String> localMembers = ConcurrentHashMap.newKeySet();
    // Local members whose departure this node itself initiated (a graceful leave) — so folding their Leave
    // is not mistaken for an eviction of a still-alive member.
    private final Set<String> selfInitiatedLeaves = ConcurrentHashMap.newKeySet();
    // Local members another node has evicted (folded a Leave we did not initiate): the task must fail and
    // re-join. Read by the task thread via isEvicted.
    private final Set<String> evictedLocalMembers = ConcurrentHashMap.newKeySet();
    // Intents enqueued by task threads, appended to the log by the runtime thread only.
    private final Queue<EpochEvent> outbox = new ConcurrentLinkedQueue<>();

    // Runtime-thread-only: the epoch we have already appended a commit for, so this node appends each
    // round's EpochCommitted exactly once (the commit round-trips through the log and advances the fold).
    private long lastCommitAppendedFor;
    // Runtime-thread-only: when this node first observed the current round open (nanoTime), 0 if none open.
    private long roundOpenSinceNanos;

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

    private volatile boolean running;
    private @Nullable Thread thread;

    /** A runtime with a default eviction timeout — for tests that never exercise eviction. */
    ParsleyEpochRuntime(ParsleyEpochTransport transport) {
        this(transport, Duration.ofSeconds(30));
    }

    ParsleyEpochRuntime(ParsleyEpochTransport transport, Duration evictionTimeout) {
        this.transport = transport;
        this.evictionTimeout = evictionTimeout;
    }

    /**
     * Announces {@code memberId} on the log and registers it as local, so this runtime folds and commits
     * on its behalf. A task calls this once it is participating.
     */
    void join(String memberId) {
        localMembers.add(memberId);
        outbox.add(new EpochEvent.JoinRequested(memberId));
    }

    /** Stops treating {@code memberId} as local (its task left this instance). No log event yet — leave/removal is a later workstream. */
    void unregisterMember(String memberId) {
        localMembers.remove(memberId);
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
        outbox.add(new EpochEvent.SnapshotRequested(memberId));
    }

    /** Publishes {@code memberId}'s current completeness frontier for the open round. */
    void publishFrontier(String memberId, ParsleyClock completeness) {
        outbox.add(new EpochEvent.FrontierPublished(memberId, completeness));
    }

    /**
     * Gracefully removes every local member from the domain (a decommission). Marked self-initiated so
     * folding the resulting {@link EpochEvent.Leave} is not mistaken for an eviction and does not trigger a
     * re-join. A restart, by contrast, does not call this — the member stays in the domain and returns.
     */
    void leaveLocalMembers() {
        for (String member : localMembers) {
            selfInitiatedLeaves.add(member);
            outbox.add(new EpochEvent.Leave(member));
        }
    }

    /** Whether {@code memberId} is currently a running member (folded from the log) — the join block waits on this. */
    boolean isRunningMember(String memberId) {
        return runningMembersMirror.contains(memberId);
    }

    /** Whether {@code memberId} (a local member) has been evicted by another node — the task must fail and re-join. */
    boolean isEvicted(String memberId) {
        return evictedLocalMembers.contains(memberId);
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
        EpochEvent pending;
        while ((pending = outbox.poll()) != null) {
            transport.append(pending);
        }
        for (EpochEvent event : transport.poll(POLL_TIMEOUT)) {
            fold.apply(event);
            if (event instanceof EpochEvent.EpochCommitted commit && commit.epochId() > committedEpochId) {
                committedEpochId = commit.epochId();
                committedLowerBounds = commit.lowerBounds();
                log.debug("Epoch {} committed with lower bounds {}", commit.epochId(), commit.lowerBounds());
            } else if (event instanceof EpochEvent.Leave leave
                    && localMembers.contains(leave.memberId()) && !selfInitiatedLeaves.contains(leave.memberId())) {
                // A local member was evicted by another node (we did not initiate its leave); surface it so
                // the task can fail and re-join under the current floor.
                evictedLocalMembers.add(leave.memberId());
                log.warn("Local member {} was evicted from the epoch domain; the task will re-join", leave.memberId());
            }
        }
        roundOpen = fold.isRoundOpen();
        runningMembersMirror = fold.runningMembers();
        bootstrapped = transport.caughtUp();
        updateRoundTimer();
        driveCommit();
        maybeEvictSilentMembers();
    }

    /** Tracks how long the current round has been open (for the eviction timeout); reset when none is open. */
    private void updateRoundTimer() {
        if (fold.isRoundOpen()) {
            if (roundOpenSinceNanos == 0) {
                roundOpenSinceNanos = System.nanoTime();
            }
        } else {
            roundOpenSinceNanos = 0;
        }
    }

    /**
     * Once a round has been open past {@link #evictionTimeout}, evicts every <em>remote</em> running member
     * that has not published, by appending a {@link EpochEvent.Leave} — so a gone member cannot hold the
     * round open forever. A node never evicts its own local members (they should publish; if one is truly
     * stuck, other nodes evict it as a remote member). The proposal uses a local clock; the log serialises
     * the decision, and dedup makes a duplicate Leave a no-op.
     */
    private void maybeEvictSilentMembers() {
        if (!bootstrapped || !fold.isRoundOpen() || localMembers.isEmpty() || roundOpenSinceNanos == 0) {
            return;
        }
        if (System.nanoTime() - roundOpenSinceNanos < evictionTimeout.toNanos()) {
            return;
        }
        for (String silent : fold.unpublishedRunningMembers()) {
            if (!localMembers.contains(silent)) {
                transport.append(new EpochEvent.Leave(silent));
            }
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
