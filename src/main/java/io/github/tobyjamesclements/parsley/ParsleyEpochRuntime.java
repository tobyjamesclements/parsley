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

    // Members whose tasks live on this instance: the runtime owns rounds these members open and drives
    // their commit. Written from task threads (join), read by the runtime thread (driveOwner).
    private final Set<String> localMembers = ConcurrentHashMap.newKeySet();
    // Intents enqueued by task threads, appended to the log by the runtime thread only.
    private final Queue<EpochEvent> outbox = new ConcurrentLinkedQueue<>();

    // Runtime-thread-only: the epoch we have already appended a commit for, so the owner appends each
    // round's EpochCommitted exactly once (the commit round-trips through the log and advances the fold).
    private long lastCommitAppendedFor;

    // Mirrors of the folded decision, published for cross-thread readers (a source-layer task polls these
    // to drive the in-band wave; a joiner blocks on committedEpochId). Volatile: written by the runtime
    // thread, read by any thread.
    private volatile long committedEpochId;
    private volatile ParsleyClock committedLowerBounds = ParsleyClock.empty();
    private volatile boolean roundOpen;
    // Whether the transport has folded the whole startup backlog. The owner must not commit before this,
    // or a just-started runtime would commit a stale epoch believing the topology empty.
    private volatile boolean bootstrapped;

    private volatile boolean running;
    private @Nullable Thread thread;

    ParsleyEpochRuntime(ParsleyEpochTransport transport) {
        this.transport = transport;
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
            }
        }
        roundOpen = fold.isRoundOpen();
        bootstrapped = transport.caughtUp();
        driveCommit();
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
