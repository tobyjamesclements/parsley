package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

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
     * coordinate's floor from the log). Declare sink topics via {@code CausalStreams.addSink(...)} — which
     * does so automatically — or {@code ParsleyProcessors.Builder.sinkTopics(...)} on the low-level path.
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
     * The joiner handshake, called from a task's {@code init()}: a node deployed into an already-running
     * (coordinated) topology must not begin consuming until an epoch computed <em>without it</em> commits,
     * so it never drags the floor's min-over-running-members toward its offset-0 position.
     *
     * <p>Waits for the runtime to fold the log to the end ({@link ParsleyEpochRuntime#isBootstrapped()}, so
     * membership is accurate), then blocks until this member is a <strong>running member</strong> — opening
     * a round to drive that commit. This one rule covers every case: a <em>fresh joiner</em> (not yet
     * running ⇒ block until an epoch computed without it commits and admits it); a <em>restart</em> of an
     * existing member (still running on the log — nothing removes it while it is absent ⇒ proceed at once
     * and drain its restored buffer under the unchanged floor); and a <em>cold start</em> (epoch 0, static
     * ⇒ proceed at once).
     *
     * <p>The block is <strong>unbounded</strong> — there is no join timeout. It never proceeds on an unknown
     * floor: if the domain cannot yet commit (an existing member is absent), the join simply waits until it
     * can, exactly as an epoch transition does. A blocked {@code init()} delivers nothing, so waiting is
     * safe; a clean Kafka Streams shutdown interrupts the wait, which unwinds the block.
     */
    void awaitJoinCommit(ParsleyEpochRuntime runtime, String memberId) {
        awaitBootstrap(runtime);

        // Cold start (epoch 0 is static), or already a running member (a normal restart): no block.
        if (runtime.committedEpochId() == 0 || runtime.isRunningMember(memberId)) {
            return;
        }
        // A fresh joiner: open a round (the running members answer it) and block until a commit promotes
        // this member to running.
        runtime.requestSnapshot(memberId);
        while (!runtime.isRunningMember(memberId)) {
            sleep();
        }
    }

    private static void awaitBootstrap(ParsleyEpochRuntime runtime) {
        while (!runtime.isBootstrapped()) {
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
     */
    void leave() {
        ParsleyEpochRuntime runtime = currentRuntime();
        if (runtime == null || runtime.anyLocalMember() == null) {
            return;
        }
        // Phase 1 — drain: block until every local member's buffer is empty (unbounded; the StreamThreads
        // keep delivering, so the buffer drains as dependencies arrive).
        while (!runtime.allLocalMembersDrained()) {
            sleep();
        }
        // Phase 2 — remove: append a Leave for each local member, then block until the fold has dropped them
        // from the running set, so the departure is durable on the log before we proceed.
        runtime.leaveLocalMembers();
        while (runtime.hasRunningLocalMembers()) {
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
