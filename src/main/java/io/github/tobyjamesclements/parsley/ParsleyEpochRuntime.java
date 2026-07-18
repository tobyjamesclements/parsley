package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
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
 * <p>A shared handle the application creates and every participating task registers with (via
 * {@link #join}). A single background thread ({@link #start}) drives the loop;
 * task threads only enqueue intents ({@link #join}, {@link #requestSnapshot}, {@link #publishFrontier}),
 * which the runtime thread appends — so the transport's non-thread-safe consumer is only ever touched by
 * the one runtime thread. The loop body {@link #runOnce} is exposed package-private so tests can drive it
 * synchronously against an in-memory transport with no thread at all.
 *
 * <p>Fully wired into {@link ParsleyProcessor}: {@code init()} joins this runtime and blocks until
 * admitted, marker relay reads the committed floor via {@link #committedEpoch()}, and {@code close()}
 * unregisters the local member. Created only when topology-epoch coordination is configured
 * ({@code parsley.coordination.epoch-events-topic}); with no runtime created, a topology runs in epoch
 * 0 exactly as before.
 */
final class ParsleyEpochRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ParsleyEpochRuntime.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    // Backoff after a failed drive-loop iteration (a transient transport/broker error). Kept well under
    // close()'s 5s join timeout so a shutdown landing mid-backoff still stops the loop promptly.
    private static final Duration ERROR_BACKOFF = Duration.ofSeconds(1);

    private final ParsleyEpochTransport transport;
    private final ParsleyEpochLog fold = new ParsleyEpochLog();

    // Members whose tasks live on this instance: the runtime folds and commits on their behalf. Written
    // from task threads (join), read by the runtime thread (driveCommit).
    private final Set<String> localMembers = ConcurrentHashMap.newKeySet();
    // Local members this instance is gracefully decommissioning (leaveLocalMembers appended their Leave).
    // The continuous roster check must not fault a member that left voluntarily once its Leave folds and
    // drops it from the running set — that would dirty-close a still-draining task and, under EOS, abort
    // its already-drained tail. Written by the leave() thread, read by task threads (the roster check).
    private final Set<String> leavingMembers = ConcurrentHashMap.newKeySet();
    // Tracks which local members are currently drained (causal buffer empty). An epoch leave cares about
    // drain state unconditionally, never gated on an external request, so allDrained() here just means
    // "every local member is currently drained". leave() waits on this before appending its Leave —
    // "only a drained node is excluded". Written by task threads (reportDrained), read by the caller's
    // leave() thread.
    private final ParsleyQuiesceTracker localDrainTracker = new ParsleyQuiesceTracker();
    // A local member's live completeness snapshot, registered once its task's engine exists (see
    // registerLocalCompleteness). Lets the runtime thread publish on a member's behalf when its own task
    // thread cannot run pollEpochCoordination() — see autoPublishStalledLocalMembers.
    private final Map<String, Supplier<ParsleyClock>> localCompletenessSuppliers = new ConcurrentHashMap<>();
    // Intents enqueued by task threads, appended to the log by the runtime thread only.
    private final Queue<ParsleyEpochEvent> outbox = new ConcurrentLinkedQueue<>();

    // Runtime-thread-only: the epoch we have already appended a commit for, so this node appends each
    // round's EpochCommitted exactly once (the commit round-trips through the log and advances the fold).
    private long lastCommitAppendedFor;
    // Runtime-thread-only: the roster-agreement state as of the last runOnce, so
    // logRosterAgreementTransitions logs only on a state-change edge, not on every 100ms poll.
    private ParsleyEpochLog.RosterAgreement lastRosterAgreement = ParsleyEpochLog.RosterAgreement.AGREE;
    // Runtime-thread-only: the last "round open but not committable" reason logged, so
    // logCohortWaitingTransitions fires only when the reason changes.
    private Optional<String> lastCohortWaitingReason = Optional.empty();
    // Runtime-thread-only: the running members an open round has been blocked on, and for how many
    // consecutive polls the same set has persisted, so logBlockedRoundTransitions can distinguish a genuine
    // stall (the set frozen for BLOCKED_ROUND_WARN_POLLS) from the healthy case where members publish within
    // a poll or two of a round opening (the set shrinks each poll). A logging debounce only — it decides
    // nothing about correctness or when to commit.
    private Set<String> lastBlockedOnMembers = Set.of();
    private int blockedPollStreak;
    private boolean warnedBlockedRound;
    private static final int BLOCKED_ROUND_WARN_POLLS = 50; // ~5s at POLL_TIMEOUT — a stall, not a healthy wait
    // Read/written by the runtime thread alone in autoPublishStalledLocalMembers, but removed from by
    // unregisterMember, which task threads call (via ParsleyProcessor#close). Concurrent, like every other
    // member-keyed collection here, for that cross-thread write.
    private final Map<String, Long> lastAutoPublishedRound = new ConcurrentHashMap<>();

    /**
     * The last committed epoch id paired with its lower bounds, read together so a caller needing both
     * (e.g. stamping a relayed boundary marker) never sees id-from-commit-N paired with
     * bounds-from-commit-{@code N-1} (or vice versa) because a second commit landed between two
     * independent reads. See {@link #committedEpoch()}.
     */
    record CommittedEpoch(long epochId, ParsleyClock lowerBounds) {}

    // Published together for cross-thread readers (a source-layer task polls these to drive the in-band
    // wave; a joiner blocks on the id). One volatile record, not two separate volatile fields — see
    // CommittedEpoch's Javadoc for why. Written by the runtime thread, read by any thread.
    private volatile CommittedEpoch committedEpoch = new CommittedEpoch(0, ParsleyClock.empty());
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
    // Mirror of the fold's domain topics (every declared member's inputs ∪ sinks), refreshed each
    // runOnce — consulted by a joining task's own full-mesh self-check (see ParsleyProcessor#init).
    private volatile Set<String> domainTopicsMirror = Set.of();
    // Mirror of the fold's externalSourceTopicsAsOfPreviousCommit(), refreshed each runOnce. See
    // ParsleyEpochLog#externalSourceTopicsAsOfPreviousCommit for what this is and why it replaced a
    // per-task in-memory cache.
    private volatile Set<String> externalSourceTopicsAsOfPreviousCommitMirror = Set.of();
    // Mirror of the fold's committed app-id roster — the authoritative membership after genesis. Read by
    // the continuous roster check and the join gate from task threads.
    private volatile Set<String> committedRosterMirror = Set.of();
    // Mirror of the fold's currently-admissible apps (committed members + those every voter has named),
    // so the join waiter can fail a never-admissible app fast from its own thread.
    private volatile Set<String> admissibleAppsMirror = Set.of();
    // Mirror of the fold's per-member admission floors, so a restarting task reads its own admission cut
    // from its own thread (see the seed in ParsleyProcessor#init and ParsleyEpochLog#admissionFloors).
    private volatile Map<String, CommittedEpoch> admissionFloorsMirror = Map.of();
    // Mirror of the fold's roster-agreement state, for edge-triggered diagnostics only.
    private volatile ParsleyEpochLog.RosterAgreement rosterAgreementMirror = ParsleyEpochLog.RosterAgreement.AGREE;
    // Set once if the fold hits a permanent wire-format incompatibility. The drive loop then stops and
    // every coordination wait surfaces this, rather than backing off forever (which reads as a misleading
    // bootstrap timeout at every join).
    private volatile @Nullable ParsleyIncompatibleEpochLogException fatalError;

    private volatile boolean running;
    private @Nullable Thread thread;

    ParsleyEpochRuntime(ParsleyEpochTransport transport) {
        this.transport = transport;
    }

    /**
     * Announces {@code memberId} on the log and registers it as local, so this runtime folds and commits
     * on its behalf. A task calls this once it is participating, declaring its {@code inputTopics} (the
     * channels it consumes) and {@code sinkTopics} (the topics it produces) for the DAG-wide source-topic
     * registry (see {@link #externalSourceTopics()}).
     */
    void join(String memberId, String appId, Set<String> siblingMemberIds, Set<String> inputTopics,
              Set<String> sinkTopics, Set<String> rosterView, int taskTotal) {
        localMembers.add(memberId);
        localDrainTracker.register(memberId);
        Set<String> inputs = Set.copyOf(inputTopics);
        Set<String> sinks = Set.copyOf(sinkTopics);
        Set<String> roster = Set.copyOf(rosterView);
        // App-level pre-declaration: declare every one of this app's task members, not just this task's.
        // The cohort barrier needs all N tasks present to commit, but a joiner blocks in init and inits
        // run serially on the StreamThread — so if a task declared only itself, task 0_1's init could
        // never run while 0_0 blocks, and the cohort would never complete (a deadlock in a correctly
        // configured domain). Declaring all siblings here — deterministic member ids, identical
        // topics/roster/total, LWW-idempotent across instances — lets the cohort complete while only this
        // task blocks. Only this task is registered as a LOCAL member (it alone lives on this instance).
        for (String sibling : siblingMemberIds) {
            outbox.add(new ParsleyEpochEvent.JoinRequested(sibling, appId, inputs, sinks, roster, taskTotal));
        }
    }

    /** Stops treating {@code memberId} as local (its task left this instance, e.g. a rebalance). Does not append a log event. */
    void unregisterMember(String memberId) {
        localMembers.remove(memberId);
        localDrainTracker.unregister(memberId);
        localCompletenessSuppliers.remove(memberId);
        lastAutoPublishedRound.remove(memberId);
    }

    /**
     * Registers {@code completenessSupplier} as {@code memberId}'s committed-completeness snapshot
     * ({@code ParsleyCommittedCompleteness#committed()} — never the live clock, which may reflect an
     * uncommitted transaction), so {@link #autoPublishStalledLocalMembers()} can publish on its behalf
     * from the runtime thread alone — without needing {@code memberId}'s own task thread to run
     * {@code pollEpochCoordination()}. Deliberately separate from {@link #join}: a task calls this only
     * once its engine exists and the snapshot is meaningful (see {@code ParsleyProcessor#init}), not at
     * join time, when a fresh joiner's snapshot would still be the empty placeholder and a restarting
     * member's would not yet reflect its restored state.
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
        localDrainTracker.setDrained(memberId, empty);
    }

    /** Whether every local member's causal buffer is currently empty — the gate {@code leave()} waits on. */
    boolean allLocalMembersDrained() {
        return localDrainTracker.allDrained();
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
            leavingMembers.add(member);   // mark BEFORE the Leave folds, so the roster check never faults it
            outbox.add(new ParsleyEpochEvent.Leave(member));
        }
    }

    /** Whether {@code memberId} is currently a running member (folded from the log) — the join block waits on this. */
    boolean isRunningMember(String memberId) {
        return runningMembersMirror.contains(memberId);
    }

    /** Whether {@code memberId} is a local member this instance is gracefully decommissioning — the
     * continuous roster check skips it, so a voluntary leave does not self-fault the still-draining task. */
    boolean isLeaving(String memberId) {
        return leavingMembers.contains(memberId);
    }

    /** The app-id membership the last commit established (empty before genesis) — the authoritative domain
     * membership, consulted by the continuous roster check in {@code ParsleyProcessor}. */
    Set<String> committedRoster() {
        return committedRosterMirror;
    }

    /** Whether {@code appId} is admissible right now (a committed member, or named in every voter's roster
     * view) — consulted by the join waiter to fail a never-admissible app fast rather than block. */
    boolean isAppAdmissible(String appId) {
        return admissibleAppsMirror.contains(appId);
    }

    /** The current roster-agreement state — read by the join waiter to explain a timeout under a roster
     * conflict or a change in flight. */
    ParsleyEpochLog.RosterAgreement rosterAgreement() {
        return rosterAgreementMirror;
    }

    /** {@code memberId}'s admission cut, or {@code null} if it has never been promoted (a genesis founder
     * still consuming before genesis commits) — the floor a stateless restart re-seeds from. */
    @Nullable CommittedEpoch admissionFloor(String memberId) {
        return admissionFloorsMirror.get(memberId);
    }

    /** The permanent wire-format incompatibility that stopped the runtime, or {@code null} while healthy —
     * surfaced by the coordination waits as a loud failure instead of a misleading bootstrap timeout. */
    @Nullable ParsleyIncompatibleEpochLogException fatalError() {
        return fatalError;
    }

    /** The topology's external source topics, derived DAG-wide from every declared member's declaration. */
    Set<String> externalSourceTopics() {
        return externalSourceTopicsMirror;
    }

    /** Every topic any declared member consumes or produces — see {@link ParsleyEpochLog#domainTopics()}. */
    Set<String> domainTopics() {
        return domainTopicsMirror;
    }

    /** See {@link ParsleyEpochLog#externalSourceTopicsAsOfPreviousCommit()}. */
    Set<String> externalSourceTopicsAsOfPreviousCommit() {
        return externalSourceTopicsAsOfPreviousCommitMirror;
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
        return committedEpoch.epochId();
    }

    /** The lower bounds of the last committed epoch (empty before any commit). */
    ParsleyClock committedLowerBounds() {
        return committedEpoch.lowerBounds();
    }

    /**
     * The last committed epoch id and its lower bounds, read together from one volatile snapshot — for a
     * caller that needs the id and the bounds to describe the <em>same</em> commit (e.g. stamping a
     * relayed boundary marker). {@link #committedEpochId()} and {@link #committedLowerBounds()} each read
     * a consistent value on their own, but calling both back-to-back risks a commit landing in between —
     * this returns one atomic pairing instead.
     */
    CommittedEpoch committedEpoch() {
        return committedEpoch;
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
                    try {
                        runOnce();
                    } catch (ParsleyIncompatibleEpochLogException e) {
                        // A wire-format incompatibility (a pre-cohort or corrupt epoch-events record) never
                        // heals on its own — retrying forever would just look like a bootstrap timeout at
                        // every join. Record it, stop the loop, and let each coordination wait surface it
                        // as a loud, actionable failure. Fail-closed: no coordination proceeds.
                        log.error("Epoch-events log is incompatible with this binary; stopping the epoch "
                                + "runtime (fail-closed). Reset the epoch-events topic (safe before genesis "
                                + "has committed) and restart the domain.", e);
                        fatalError = e;
                        running = false;
                    } catch (Exception e) {
                        // A transport append or poll can throw on a transient broker blip. The runtime
                        // thread must survive it: if it died here, bootstrapped would stay false so every
                        // join would block forever, and no round would ever commit — a silent, permanent
                        // wedge. Log, back off, and retry; the transport reconnects on its own, and an
                        // outbox event whose append threw is still queued (runOnce removes only after a
                        // successful append), so nothing enqueued is lost.
                        log.warn("Epoch runtime drive-loop iteration failed; backing off and retrying", e);
                        backoff();
                    }
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
        // Peek-then-remove: append the head, and only drop it once the append has succeeded. If append
        // throws, the event stays queued and is retried on the next iteration rather than being lost —
        // this thread is the sole consumer, so the element we peeked is still the head when we remove it.
        ParsleyEpochEvent pending;
        while ((pending = outbox.peek()) != null) {
            transport.append(pending);
            outbox.poll();
        }
        for (ParsleyEpochEvent event : transport.poll(POLL_TIMEOUT)) {
            fold.apply(event);
        }
        // Mirror write ORDER is load-bearing for two cross-thread reads (volatile release/acquire makes a
        // reader that sees a later mirror also see every earlier one):
        //   - admissionFloors + roster BEFORE runningMembers: a task that reads isRunningMember==true (in
        //     awaitJoinCommit) then reads its admission floor (the seed in ParsleyProcessor#init) must not
        //     see a stale (null) floor and seed at ∅ — that would strip pre-cut history it never skipped.
        //   - runningMembers BEFORE committedEpoch: the continuous roster check reads committedEpochId>0
        //     then !isRunningMember; observing the new epoch with a stale running set would spuriously
        //     fault a member the very commit that advanced the epoch just promoted.
        // So: fold-derived membership/floors first, then runningMembers, then committedEpoch last.
        roundOpen = fold.isRoundOpen();
        unpublishedMembersMirror = fold.unpublishedRunningMembers();
        externalSourceTopicsMirror = fold.externalSourceTopics();
        externalSourceTopicsAsOfPreviousCommitMirror = fold.externalSourceTopicsAsOfPreviousCommit();
        domainTopicsMirror = fold.domainTopics();
        committedRosterMirror = fold.committedRoster();
        admissibleAppsMirror = fold.admissibleApps();
        admissionFloorsMirror = fold.admissionFloorsSnapshot();
        rosterAgreementMirror = fold.rosterAgreement();
        runningMembersMirror = fold.runningMembers();
        // Drive the committed-epoch snapshot from the fold, not the raw commit event: a commit event can be
        // rejected on apply, so only an actual advance of the fold's committed epoch counts. Written LAST
        // (after runningMembers) for the roster-check ordering above.
        if (fold.committedEpochId() > committedEpoch.epochId()) {
            committedEpoch = new CommittedEpoch(fold.committedEpochId(), fold.committedLowerBounds());
            log.debug("Epoch {} committed with lower bounds {} and roster {}",
                    fold.committedEpochId(), fold.committedLowerBounds(), fold.committedRoster());
        }
        logRosterAgreementTransitions();
        logBlockedRoundTransitions();
        logCohortWaitingTransitions();
        bootstrapped = transport.caughtUp();
        driveCommit();
        autoPublishStalledLocalMembers();
    }

    /**
     * Publishes a stalled local member's registered committed-completeness snapshot on its behalf — the
     * fix for the deadlock where a member's own task thread can never run {@code pollEpochCoordination()}
     * because it shares a Kafka Streams {@code StreamThread} with another task blocked in
     * {@code awaitJoinCommit}'s (bounded) join wait. This method runs on the runtime's own background
     * thread, which is distinct from every {@code StreamThread} and so keeps making progress even while
     * one is wedged.
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
     * Logs (edge-triggered, {@code WARN}) when the open round cannot commit for a reason other than
     * running members not having published (which {@link #logBlockedRoundTransitions} reports): a
     * diverging roster, a cohort still short of a roster app's full task set, or an unmet mesh. This is
     * the only signal for a genesis or roster change that waits forever — a never-deployed roster app, or
     * a task-count undercount that manifests as "the round never commits". See {@link
     * ParsleyEpochLog#whyRoundNotCommittable()}.
     */
    private void logCohortWaitingTransitions() {
        Optional<String> reason = fold.whyRoundNotCommittable();
        if (reason.equals(lastCohortWaitingReason)) {
            return;
        }
        lastCohortWaitingReason = reason;
        reason.ifPresent(r -> log.warn("Epoch round {} is open but cannot commit yet: {}",
                fold.nextEpochId(), r));
    }

    /**
     * Logs (edge-triggered) when the member-app roster stops agreeing — {@code WARN} for CONVERGING (a
     * roster change in flight: some apps declare the new roster, some the old) and {@code ERROR} for
     * CONFLICT (incompatible rosters, or an app's task totals disagreeing) — and {@code INFO} when it
     * agrees again. Under either non-agreeing state no epoch commits and no member is admitted, but the
     * domain keeps delivering under the last committed floor, so this is a diagnostic, not a data-plane
     * fault. Logged only on a state-change edge so a long-lived state does not flood the log.
     */
    private void logRosterAgreementTransitions() {
        ParsleyEpochLog.RosterAgreement agreement = fold.rosterAgreement();
        if (agreement == lastRosterAgreement) {
            return;
        }
        lastRosterAgreement = agreement;
        switch (agreement) {
            case CONVERGING -> log.warn("Member-app roster is converging — a roster change is in flight; no "
                    + "epoch commits until every app declares the same roster. Committed roster: {}",
                    fold.committedRoster());
            case CONFLICT -> log.error("Member-app roster CONFLICT — apps declare incompatible rosters, or an "
                    + "app's task totals disagree. No epoch commits and no admissions until the configs agree; "
                    + "the domain keeps delivering under the last committed floor. Committed roster: {}",
                    fold.committedRoster());
            case AGREE -> log.info("Member-app roster agrees — epoch rounds can commit. Committed roster: {}",
                    fold.committedRoster());
        }
    }

    /**
     * Logs (at {@code WARN}) when an open round has been blocked on the same running members failing to
     * publish for {@link #BLOCKED_ROUND_WARN_POLLS} consecutive polls — a genuine stall, most often a
     * member that is down or one that is mis-meshed and crash-looping in {@code init} without ever
     * publishing — and (at {@code INFO}) when the stall clears. Unlike a mesh insufficiency, a member
     * stuck unpublished may leave the mesh check satisfied (nothing re-declares it), so this is the only
     * domain-side signal for that wedge. Debounced by the poll streak: a round healthily waits for its
     * members' first publications for a poll or two after opening (the outstanding set shrinks each poll),
     * so only a set frozen across the whole streak is reported, and the edge is logged once, not every poll.
     * The recovery INFO fires whenever a round that had warned reaches full publication, even if it cleared
     * incrementally (the outstanding set shrinking member by member resets the streak but not the warning).
     */
    private void logBlockedRoundTransitions() {
        Set<String> blockedOn = fold.isRoundOpen() ? fold.unpublishedRunningMembers() : Set.of();
        if (blockedOn.isEmpty()) {
            if (warnedBlockedRound) {
                log.info("Epoch round is no longer blocked — every running member has published");
                warnedBlockedRound = false;
            }
            lastBlockedOnMembers = Set.of();
            blockedPollStreak = 0;
            return;
        }
        if (blockedOn.equals(lastBlockedOnMembers)) {
            blockedPollStreak++;
        } else {
            // The outstanding set changed — members are still publishing, so this is progress, not a stall.
            lastBlockedOnMembers = blockedOn;
            blockedPollStreak = 1;
            return;
        }
        if (blockedPollStreak == BLOCKED_ROUND_WARN_POLLS) { // fire exactly once, on the edge into "stalled"
            log.warn("Epoch round {} has been blocked for ~{}ms: these running members have not published "
                    + "their completeness and may be down, or mis-meshed and crash-looping in init: {}",
                    fold.nextEpochId(), BLOCKED_ROUND_WARN_POLLS * POLL_TIMEOUT.toMillis(), blockedOn);
            warnedBlockedRound = true;
        }
    }

    /**
     * Collect → commit, leaderless: once an open round is committable, <em>any</em> node with a local
     * member appends the {@code EpochCommitted}. The {@link ParsleyEpochLog#evaluateCommit() unified
     * commit decision} is a deterministic function of the folded log, so every node computes the identical
     * commit, and dedup-by-{@code epochId} makes the concurrent appends idempotent — there is no single
     * owner to fail (a gone owner cannot freeze the epoch). Guarded so this node appends each round's
     * commit at most once; the commit is read back through the log and re-validated by the fold on apply.
     *
     * <p>A round with an outstanding (unpublished) member simply stays open until that member publishes or
     * is evicted via an explicit {@link ParsleyEpochEvent.Leave} — there is no automatic exclusion. Only a
     * drained member may ever safely leave the domain (see {@link ParsleyCoordination#leave()}); a crashed
     * member's buffer is never knowably drained from outside, so waiting, unbounded, is the only safe
     * default.
     */
    private void driveCommit() {
        // If a commit we appended was rejected by the fold (a conflicting event folded in after we read
        // the state), clear the append-once latch for that epoch so it can be re-proposed once the fold
        // is committable again. Without this a single-app domain — or every node at genesis, which all
        // race to propose — would latch on the rejected epoch and never re-commit it.
        long rejected = fold.consumeLastRejectedCommitEpoch();
        if (rejected != -1 && rejected == lastCommitAppendedFor) {
            lastCommitAppendedFor = rejected - 1;
        }
        // Never commit before the whole startup backlog is folded: the decision would be made against a
        // topology this runtime has not yet fully observed. Only nodes with a local member commit, to
        // bound the duplicate appends.
        if (!bootstrapped || localMembers.isEmpty()) {
            return;
        }
        Optional<ParsleyEpochEvent.EpochCommitted> commit = fold.evaluateCommit();
        if (commit.isEmpty()) {
            return;
        }
        long epochToCommit = commit.get().epochId();
        if (epochToCommit <= lastCommitAppendedFor) {
            return;
        }
        transport.append(commit.get());
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

    /** Sleeps out the post-error backoff; an interrupt stops the loop (treated as a shutdown signal). */
    private void backoff() {
        try {
            Thread.sleep(ERROR_BACKOFF.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private void closeTransportQuietly() {
        try {
            transport.close();
        } catch (Exception e) {
            log.warn("Failed to close epoch transport", e);
        }
    }
}
