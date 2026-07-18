package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Turns on topology-epoch coordination for a {@link CausalStreams} runtime. {@code CausalStreams} builds
 * one {@code ParsleyCoordination} internally — from {@code parsley.coordination.epoch-events-topic} in the
 * {@code props} passed to its constructor — and wires it into every stage; there is no public handle. To
 * evolve a running topology through an epoch boundary, call {@code CausalStreams#requestEpochTransition()};
 * {@code CausalStreams#close()} runs the graceful decommission ({@link #leave()}) before stopping.
 *
 * <p>Without {@code parsley.coordination.epoch-events-topic} configured, a topology runs in
 * <strong>epoch 0</strong> exactly as before: no epoch-events log, no coordination thread, every
 * coordination path inert.
 *
 * <p><strong>What it owns.</strong> One {@link ParsleyEpochRuntime} per application instance — the
 * deterministic fold over the shared {@code epochEventsTopic} log plus the round-owner runtime. It is
 * built <em>lazily</em> at the first participating task's {@code init()}, because only then is the task's
 * {@code appConfigs()} (broker + security) available for the runtime's raw Kafka clients; every later
 * task on the instance shares the same runtime.
 *
 * <p><strong>Thread-safety:</strong> safe to share across every task's Kafka Streams thread and to call
 * {@link #requestEpochTransition()} / {@link #close()} from an unrelated thread.
 */
final class ParsleyCoordination {

    private static final Logger log = LoggerFactory.getLogger(ParsleyCoordination.class);

    private static final Duration COORDINATION_POLL_INTERVAL = Duration.ofMillis(20);

    private final String epochEventsTopic;
    // Set only by forRuntime(...) — a pre-built runtime (e.g. over an in-memory transport) that bypasses
    // the lazy Kafka build, so tests exercise the wiring without a broker.
    private final @Nullable ParsleyEpochRuntime injectedRuntime;

    private final Object lock = new Object();
    private @Nullable ParsleyEpochRuntime lazyRuntime;

    private ParsleyCoordination(String epochEventsTopic, @Nullable ParsleyEpochRuntime injectedRuntime) {
        this.epochEventsTopic = epochEventsTopic;
        this.injectedRuntime = injectedRuntime;
    }

    /**
     * Creates a coordination handle over the shared {@code epochEventsTopic} log. The topology's external
     * source topics — the entry-point topics produced by systems outside the topology, on which no in-band
     * epoch marker ever arrives — are <strong>derived from the log</strong>: every participating stage
     * declares its input channels and sink topics on join, and a topic some member consumes but no member
     * produces is an external source (so a stage consuming one self-initiates the wave and adopts that
     * coordinate's floor from the log). Sink topics are declared automatically from
     * {@code CausalStreamsBuilder}/{@code CausalProcessedStream#to(...)}, which {@code CausalTopology}
     * passes through to {@code ParsleyProcessorSupplier.Builder#sinkTopics(...)}.
     *
     * <p>An epoch transition blocks — unbounded — until every running member has published; a member is
     * removed from the domain only by an explicit {@link #leave()}, never automatically, since only a
     * drained member can safely leave.
     *
     * @param epochEventsTopic the single-partition epoch-events log topic name
     * @return a new coordination handle
     */
    static ParsleyCoordination create(String epochEventsTopic) {
        Objects.requireNonNull(epochEventsTopic, "epochEventsTopic must not be null");
        return new ParsleyCoordination(epochEventsTopic, null);
    }

    /**
     * A handle over a pre-built {@code runtime} (bypassing the lazy Kafka build) for tests that drive an
     * {@link InMemoryEpochTransport}-backed runtime with no broker.
     */
    static ParsleyCoordination forRuntime(ParsleyEpochRuntime runtime) {
        return new ParsleyCoordination("", runtime);
    }

    /**
     * Returns the instance's shared {@link ParsleyEpochRuntime}, building and starting it (from {@code
     * appConfigs}) on the first call. Called by {@link ParsleyProcessor#init}; synchronized so the runtime
     * is built exactly once per instance across concurrent StreamThreads.
     */
    ParsleyEpochRuntime runtimeFor(Map<String, Object> appConfigs) {
        if (injectedRuntime != null) {
            return injectedRuntime;
        }
        synchronized (lock) {
            ParsleyEpochRuntime existing = lazyRuntime;
            if (existing != null) {
                return existing;
            }
            ParsleyEpochRuntime built = new ParsleyEpochRuntime(
                    new KafkaEpochTransport(appConfigs, epochEventsTopic));
            built.start();
            lazyRuntime = built;
            return built;
        }
    }

    /**
     * The joiner handshake, called from a task's {@code init()}. Its purpose is the <strong>consistent
     * cut</strong> at the heart of topology epochs, not membership bookkeeping. Kafka replays history: a
     * node added to a long-running topology begins consuming from offset 0 — logical time far in the past
     * — and, if it acted on that ancient traffic, would stamp outputs that <em>happen-before</em> the
     * earliest messages the topology ever agreed on, retroactively forcing every other node to have
     * mis-delivered. An epoch is a consistent cut, taken whenever the topology changes, that fixes a new
     * logical time-0 every node (the newcomer included) agrees on: anything a node emits that
     * happens-before that cut is ignored, and only what happens-before messages <em>within</em> the
     * current epoch is acted on causally. So a fresh joiner must not begin consuming until the epoch that
     * establishes its cut — {@code F_{k+1}}, computed with it — has committed and admitted it. Consuming
     * ahead of that is unsound: it would race past the not-yet-known floor and act on pre-cut history.
     *
     * <p>Waits for the runtime to fold the log to the end ({@link ParsleyEpochRuntime#isBootstrapped()}, so
     * membership is accurate), then blocks until this member is a <strong>running member</strong> — opening
     * a round to drive that commit. This one rule covers every case: a <em>fresh joiner</em> (not yet
     * running ⇒ block until an epoch computed with it commits and admits it); a <em>restart</em> of an
     * existing member (still running on the log — nothing removes it while it is absent ⇒ proceed at once
     * and drain its restored buffer under the unchanged floor); and a <em>cold start</em> (epoch 0, static
     * ⇒ proceed at once).
     *
     * <p>The wait runs on the Kafka Streams {@code StreamThread}, so it is <strong>bounded</strong> by
     * {@code budget} (sized below {@code max.poll.interval.ms} by the caller). An admission that cannot
     * happen — the domain cannot currently commit because an existing member is down or partitioned —
     * would otherwise outlive the poll deadline and be silently evicted into a rebalance crash-loop; the
     * bound turns that into a loud, actionable {@link ParsleyJoinTimeoutException} instead. It still never
     * proceeds on an unknown floor: it either admits or fails, never guesses. A blocked {@code init()}
     * delivers nothing, so waiting is safe; a clean Kafka Streams shutdown interrupts the wait, which
     * unwinds the block.
     *
     * @param budget the maximum time to wait before failing with {@link ParsleyJoinTimeoutException}
     */
    void awaitJoinCommit(ParsleyEpochRuntime runtime, String memberId, String appId, Duration budget,
                         long deadlineNanos) {
        awaitBootstrap(runtime, memberId, budget, deadlineNanos);

        // Genesis: a founder does NOT block. The genesis floor is empty by construction, so consuming from
        // the start is safe — genesis is a new logical time-0, so there is no pre-cut history to race past
        // (unlike a post-genesis joiner, whose non-empty floor is not yet known). Open the genesis round so
        // the cohort barrier (in the fold) can drive it to a commit, then return. The barrier holds genesis
        // open until the whole configured cohort has declared, so every founder — however slow to start —
        // inits during this empty-floor window; only after genesis commits is a later arrival a joiner.
        if (runtime.committedEpochId() == 0) {
            runtime.requestSnapshot(memberId);
            return;
        }
        // A normal restart: still a running member on the log — proceed at once under the unchanged floor.
        if (runtime.isRunningMember(memberId)) {
            return;
        }
        // A post-genesis joiner: block until an epoch computed with it commits and admits it. A round is
        // opened on this member's behalf only while its app is admissible (a committed member, or named by
        // every committed member's roster view) and no round is already open — so a not-yet-acknowledged
        // app (a rogue, or an add whose incumbents have not all redeployed yet) waits without churning the
        // domain through no-change commits, and becomes admittable the moment the committed members name it.
        long lastRequestedForEpoch = -1;
        while (!runtime.isRunningMember(memberId)) {
            surfaceFatalError(runtime);
            if (System.nanoTime() >= deadlineNanos) {
                throw new ParsleyJoinTimeoutException(memberId, budget, joinTimeoutReason(runtime, appId));
            }
            // Open a round on this member's behalf only while admissible and none is open — and at most
            // once per committed epoch, so the ~20ms retry loop does not enqueue a burst of coalescing
            // SnapshotRequested duplicates on the eternal log while the round-open state folds back.
            if (runtime.isAppAdmissible(appId) && !runtime.isRoundOpen()
                    && runtime.committedEpochId() != lastRequestedForEpoch) {
                runtime.requestSnapshot(memberId);
                lastRequestedForEpoch = runtime.committedEpochId();
            }
            sleep();
        }
    }

    /** Why a join wait timed out — an inadmissible app (not an agreed roster member), a roster conflict or
     * change in flight (no epoch can commit), or simply an admission that did not happen in time. */
    private static String joinTimeoutReason(ParsleyEpochRuntime runtime, String appId) {
        if (!runtime.isAppAdmissible(appId)) {
            return "application '" + appId + "' is not an agreed member of the committed roster "
                    + runtime.committedRoster() + " (add it to every member app's "
                    + ParsleyConfig.COORDINATION_MEMBER_APPS + " and redeploy to admit it)";
        }
        return switch (runtime.rosterAgreement()) {
            case CONFLICT -> "the member-app roster is in conflict (apps declare incompatible rosters, or an "
                    + "app's task totals disagree) — no epoch can commit until the configs agree";
            case CONVERGING -> "a member-app roster change is in flight — no epoch commits until every app "
                    + "declares the same roster";
            case AGREE -> "no epoch admitting it committed in time";
        };
    }

    /**
     * Convenience for a standalone join wait that owns the whole budget (no preceding {@link
     * #awaitBootstrap} sharing a deadline). {@code ParsleyProcessor#init} does not use this — it computes
     * one deadline and threads it through both waits so their sum stays within the single budget.
     */
    void awaitJoinCommit(ParsleyEpochRuntime runtime, String memberId, String appId, Duration budget) {
        awaitJoinCommit(runtime, memberId, appId, budget, System.nanoTime() + budget.toNanos());
    }

    /** Surfaces a permanent epoch-log incompatibility (a wire-format mismatch) that stopped the runtime as
     * a loud failure, rather than letting the wait silently burn its whole budget and time out. */
    private static void surfaceFatalError(ParsleyEpochRuntime runtime) {
        ParsleyIncompatibleEpochLogException fatal = runtime.fatalError();
        if (fatal != null) {
            throw new IllegalStateException(
                    "topology-epoch coordination cannot proceed: " + fatal.getMessage(), fatal);
        }
    }

    /**
     * Blocks until the runtime has folded the epoch-events log to its end ({@link
     * ParsleyEpochRuntime#isBootstrapped()}), so {@link ParsleyEpochRuntime#domainTopics()} and membership
     * are accurate — the caller in {@code ParsleyProcessor#init} waits on this <em>before</em> validating
     * its own full-mesh coverage and declaring itself, so a mis-meshed member is caught before it ever
     * appends a {@link ParsleyEpochEvent.JoinRequested}. Shares the caller's single {@code deadlineNanos}
     * with the subsequent {@link #awaitJoinCommit} so the two waits together never exceed the one join
     * budget (a second, independent budget could overrun {@code max.poll.interval.ms} and get the consumer
     * silently evicted mid-block).
     */
    void awaitBootstrap(ParsleyEpochRuntime runtime, String memberId, Duration budget,
                        long deadlineNanos) {
        while (!runtime.isBootstrapped()) {
            surfaceFatalError(runtime);
            if (System.nanoTime() >= deadlineNanos) {
                throw new ParsleyJoinTimeoutException(memberId, budget,
                        "the epoch-events log was not folded to its end");
            }
            sleep();
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(COORDINATION_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting topology-epoch coordination", e);
        }
    }

    /**
     * Requests an epoch transition across the currently-running nodes: opens a snapshot round attributed
     * to a local running member so this instance owns and commits it. The running nodes publish their
     * frontiers, the owner merge-mins them into the new floor, and the boundary propagates in-band.
     *
     * @throws IllegalStateException if no task has initialised coordination yet, or no local member has
     *                               joined (so there is nothing to attribute the request to)
     */
    void requestEpochTransition() {
        ParsleyEpochRuntime runtime = currentRuntime();
        if (runtime == null) {
            throw new IllegalStateException(
                    "coordination has not initialised yet — no participating task has started");
        }
        String member = runtime.anyLocalMember();
        if (member == null) {
            throw new IllegalStateException("no local member has joined yet");
        }
        runtime.requestSnapshot(member);
    }

    /**
     * Gracefully decommissions this instance's members from the epoch domain — a genuine removal, not a
     * restart (a restart must <em>not</em> call this: leaving it out lets the member stay in the domain and
     * return with no epoch churn). Blocking, in three phases, honouring "only a drained node is excluded":
     * <ol>
     *   <li><strong>Drain.</strong> Waits — unbounded, no timeout — until every local member's causal buffer
     *       is empty, so removing them strands no held record. The task threads keep delivering meanwhile;
     *       the buffer drains through the ordinary path as dependencies arrive.
     *   <li><strong>Remove.</strong> Appends a {@code Leave} for each local member and waits until the fold
     *       has dropped them from the running set, so the departure is durable on the log.
     *   <li><strong>Re-settle.</strong> Requests a new epoch over the remaining members — the leaver
     *       excluded, since its {@code Leave} precedes the request in log order — then returns without
     *       waiting for that epoch to commit, so shutdown is never coupled to the remaining members'
     *       liveness (the survivors commit it on their own).
     * </ol>
     * A no-op if no task has initialised coordination or no local member has joined. <strong>Contract:</strong>
     * the caller must have stopped feeding this node new input before decommissioning — {@code leave()}
     * drains the in-flight buffer, not records that arrive after it returns.
     *
     * <p>The phase-1 drain wait is unbounded only while draining can actually progress: when
     * {@code canStillDrain} reports false (the streams instance died in {@code ERROR}, or was already
     * stopped), no task will ever deliver again, so the whole decommission is <em>abandoned</em> — the
     * members stay in the domain exactly as a crash would leave them, never evicted with an undrained
     * buffer ("only a drained node is excluded"), and a later restart resumes them as running members
     * under the unchanged floor.
     *
     * @param canStillDrain whether the owning streams instance can still deliver records — polled each
     *                      wait iteration; returning {@code false} abandons the decommission
     */
    void leave(BooleanSupplier canStillDrain) {
        ParsleyEpochRuntime runtime = currentRuntime();
        if (runtime == null || runtime.anyLocalMember() == null) {
            return;
        }
        // Phase 1 — drain: block until every local member's buffer is empty (unbounded while draining can
        // progress; the StreamThreads keep delivering, so the buffer drains as dependencies arrive). A
        // dead streams instance can never drain, so abandon the decommission rather than hang — and
        // rather than evict members whose buffers still hold records.
        while (!runtime.allLocalMembersDrained()) {
            if (!canStillDrain.getAsBoolean()) {
                log.warn("Abandoning the epoch-domain decommission: the streams instance can no longer "
                        + "deliver, so the causal buffer will never drain. Members stay in the domain "
                        + "(as after a crash) and resume as running members on the next start.");
                return;
            }
            sleep();
        }
        // Phase 2 — remove: append a Leave for each local member, then block until the fold has dropped them
        // from the running set, so the departure is durable on the log before we proceed.
        runtime.leaveLocalMembers();
        while (runtime.hasRunningLocalMembers()) {
            if (runtime.fatalError() != null) {
                log.warn("Abandoning the epoch-domain decommission: the epoch runtime has halted on a "
                        + "wire-format incompatibility, so the Leave can never fold. Members stay in the "
                        + "domain (as after a crash) and resume on the next compatible start.");
                return;
            }
            sleep();
        }
        // Phase 3 — re-settle: request a new epoch over the remaining members (this node already excluded)
        // and return; the leaver is safe, so it does not wait for the commit.
        String member = runtime.anyLocalMember();
        if (member != null) {
            runtime.requestSnapshot(member);
        }
    }

    /**
     * Stops the coordination runtime (its poll thread and Kafka clients). Idempotent; call it from your
     * shutdown path after {@code KafkaStreams#close}. A no-op if no task ever initialised coordination.
     */
    void close() {
        ParsleyEpochRuntime runtime = currentRuntime();
        if (runtime != null) {
            runtime.close();
        }
    }

    private @Nullable ParsleyEpochRuntime currentRuntime() {
        if (injectedRuntime != null) {
            return injectedRuntime;
        }
        synchronized (lock) {
            return lazyRuntime;
        }
    }
}
