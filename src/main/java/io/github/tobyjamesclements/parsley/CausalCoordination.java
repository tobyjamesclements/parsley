package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The public handle that turns on topology-epoch coordination for a causal application. Create one,
 * register it with every {@link CausalProcessors} / {@link CausalStreams} builder whose stages should
 * participate (via {@code withCoordination(...)}), and close it from your shutdown path — the same shape
 * as {@link CausalQuiesce}:
 *
 * <pre>{@code
 * CausalCoordination coordination =
 *         CausalCoordination.create("parsley-epoch-events", Set.of("prices", "orders"));
 *
 * Topology topology = CausalStreams.builder(userSupplier)
 *         .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
 *         .addSource(CausalBuffer.of("prices", Serdes.String(), priceSerde))
 *         .addSink("enriched", "enriched-output", Serdes.String(), enrichedSerde)
 *         .withCoordination(coordination)
 *         .build();
 *
 * KafkaStreams streams = new KafkaStreams(topology, props);
 * streams.start();
 * // ... to evolve the running topology through an epoch boundary:
 * coordination.requestEpochTransition();
 * // ... in shutdown, after streams.close():
 * coordination.close();
 * }</pre>
 *
 * <p>Without a {@code CausalCoordination}, a topology runs in <strong>epoch 0</strong> exactly as before:
 * no epoch-events log, no coordination thread, every coordination path inert.
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
public final class CausalCoordination {

    /** The default bound on how long a joining task waits for its epoch to commit before failing to retry. */
    public static final Duration DEFAULT_JOIN_TIMEOUT = Duration.ofSeconds(60);

    /** The default bound a snapshot round waits for a member before evicting it (must exceed a rolling restart). */
    public static final Duration DEFAULT_EVICTION_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration JOIN_POLL_INTERVAL = Duration.ofMillis(20);

    private final String epochEventsTopic;
    private final Set<String> sourceTopics;
    private final Duration joinTimeout;
    // Set only by forRuntime(...) — a pre-built runtime (e.g. over an in-memory transport) that bypasses
    // the lazy Kafka build, so tests exercise the wiring without a broker.
    private final @Nullable ParsleyEpochRuntime injectedRuntime;

    private final Duration evictionTimeout;

    private final Object lock = new Object();
    private @Nullable ParsleyEpochRuntime lazyRuntime;

    private CausalCoordination(String epochEventsTopic, Set<String> sourceTopics, Duration joinTimeout,
                               Duration evictionTimeout, @Nullable ParsleyEpochRuntime injectedRuntime) {
        this.epochEventsTopic = epochEventsTopic;
        this.sourceTopics = sourceTopics;
        this.joinTimeout = joinTimeout;
        this.evictionTimeout = evictionTimeout;
        this.injectedRuntime = injectedRuntime;
    }

    /**
     * Creates a coordination handle over the shared {@code epochEventsTopic} log, declaring the
     * topology's external {@code sourceTopics} — the entry-point topics produced by systems outside this
     * topology, on which no in-band epoch marker will ever arrive, so a stage consuming one self-initiates
     * the epoch wave and adopts that coordinate's floor from the log.
     *
     * @param epochEventsTopic the single-partition epoch-events log topic name
     * @param sourceTopics     the topology's external source topic names
     * @return a new coordination handle
     */
    public static CausalCoordination create(String epochEventsTopic, Set<String> sourceTopics) {
        return create(epochEventsTopic, sourceTopics, DEFAULT_JOIN_TIMEOUT, DEFAULT_EVICTION_TIMEOUT);
    }

    /**
     * As {@link #create(String, Set)}, with explicit timeouts. {@code joinTimeout} bounds how long a task
     * deployed into an already-running topology waits for its epoch to commit before failing (so Kafka
     * Streams restarts and retries) rather than proceeding on an unknown floor. {@code evictionTimeout}
     * bounds how long a snapshot round waits for a member to publish before this instance evicts it (so a
     * gone member cannot freeze the domain); it must exceed a rolling restart.
     *
     * @param epochEventsTopic the single-partition epoch-events log topic name
     * @param sourceTopics     the topology's external source topic names
     * @param joinTimeout      the bound on the join wait
     * @param evictionTimeout  the bound a round waits for a member before evicting it
     * @return a new coordination handle
     */
    public static CausalCoordination create(String epochEventsTopic, Set<String> sourceTopics,
                                            Duration joinTimeout, Duration evictionTimeout) {
        Objects.requireNonNull(epochEventsTopic, "epochEventsTopic must not be null");
        Objects.requireNonNull(joinTimeout, "joinTimeout must not be null");
        Objects.requireNonNull(evictionTimeout, "evictionTimeout must not be null");
        return new CausalCoordination(epochEventsTopic, Set.copyOf(sourceTopics), joinTimeout, evictionTimeout, null);
    }

    /**
     * A handle over a pre-built {@code runtime} (bypassing the lazy Kafka build) for tests that drive an
     * {@link InMemoryEpochTransport}-backed runtime with no broker. The runtime carries its own eviction
     * timeout; this handle's is unused on the injected path.
     */
    static CausalCoordination forRuntime(ParsleyEpochRuntime runtime, Set<String> sourceTopics) {
        return new CausalCoordination("", Set.copyOf(sourceTopics), DEFAULT_JOIN_TIMEOUT, DEFAULT_EVICTION_TIMEOUT, runtime);
    }

    /** As {@link #forRuntime(ParsleyEpochRuntime, Set)} with an explicit join timeout (for timeout tests). */
    static CausalCoordination forRuntime(ParsleyEpochRuntime runtime, Set<String> sourceTopics, Duration joinTimeout) {
        return new CausalCoordination("", Set.copyOf(sourceTopics), joinTimeout, DEFAULT_EVICTION_TIMEOUT, runtime);
    }

    /** The declared external source topics. */
    Set<String> sourceTopics() {
        return sourceTopics;
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
                    new ParsleyKafkaEpochTransport(appConfigs, epochEventsTopic), evictionTimeout);
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
     * a round to drive that commit. This one rule covers every case: a <em>fresh joiner</em> or an
     * <em>evicted-then-restarted</em> member (not running ⇒ block until re-included); a <em>normal
     * restart</em> (still a running member on the log ⇒ proceed at once); and a <em>cold start</em> (epoch
     * 0, static ⇒ proceed at once). Throws (failing the task, so Streams restarts and retries) if the
     * timeout elapses first — never proceeds on an unknown floor.
     */
    void awaitJoinCommit(ParsleyEpochRuntime runtime, String memberId) {
        long deadlineNanos = System.nanoTime() + joinTimeout.toNanos();
        awaitBootstrap(runtime, deadlineNanos);

        // Cold start (epoch 0 is static), or already a running member (a normal restart): no block.
        if (runtime.committedEpochId() == 0 || runtime.isRunningMember(memberId)) {
            return;
        }
        // A fresh or evicted-and-rejoining member: open a round (the running members answer it) and block
        // until a commit promotes this member to running.
        runtime.requestSnapshot(memberId);
        while (!runtime.isRunningMember(memberId)) {
            if (System.nanoTime() > deadlineNanos) {
                throw new IllegalStateException("topology-epoch join did not commit within " + joinTimeout
                        + "; failing the task so Kafka Streams restarts and retries the join");
            }
            sleep();
        }
    }

    private static void awaitBootstrap(ParsleyEpochRuntime runtime, long deadlineNanos) {
        while (!runtime.isBootstrapped()) {
            if (System.nanoTime() > deadlineNanos) {
                throw new IllegalStateException(
                        "topology-epoch coordination did not fold the epoch-events log within the join timeout");
            }
            sleep();
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(JOIN_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting the topology-epoch join commit", e);
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
    public void requestEpochTransition() {
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
     * Gracefully removes this instance's members from the epoch domain — for a genuine <strong>decommission
     * </strong>, not a restart. After this, running rounds no longer wait for them. A restart should
     * <em>not</em> call this: leaving it out lets the member stay in the domain and return (no epoch churn);
     * a member that crashes without leaving is removed by timeout eviction instead. A no-op if no task has
     * initialised coordination.
     */
    public void leave() {
        ParsleyEpochRuntime runtime = currentRuntime();
        if (runtime != null) {
            runtime.leaveLocalMembers();
        }
    }

    /**
     * Stops the coordination runtime (its poll thread and Kafka clients). Idempotent; call it from your
     * shutdown path after {@code KafkaStreams#close}. A no-op if no task ever initialised coordination.
     */
    public void close() {
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
