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

    private static final Duration JOIN_POLL_INTERVAL = Duration.ofMillis(20);

    private final String epochEventsTopic;
    private final Set<String> sourceTopics;
    private final Duration joinTimeout;
    // Set only by forRuntime(...) — a pre-built runtime (e.g. over an in-memory transport) that bypasses
    // the lazy Kafka build, so tests exercise the wiring without a broker.
    private final @Nullable ParsleyEpochRuntime injectedRuntime;

    private final Object lock = new Object();
    private @Nullable ParsleyEpochRuntime lazyRuntime;

    // The join handshake, captured once per instance under joinLock (the first participating task drives
    // it; the rest wait for the same commit).
    private final Object joinLock = new Object();
    private boolean joinInitiated;
    private long joinEpoch;
    private boolean joinRequired;

    private CausalCoordination(String epochEventsTopic, Set<String> sourceTopics, Duration joinTimeout,
                               @Nullable ParsleyEpochRuntime injectedRuntime) {
        this.epochEventsTopic = epochEventsTopic;
        this.sourceTopics = sourceTopics;
        this.joinTimeout = joinTimeout;
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
        return create(epochEventsTopic, sourceTopics, DEFAULT_JOIN_TIMEOUT);
    }

    /**
     * As {@link #create(String, Set)}, with an explicit {@code joinTimeout} — how long a task deployed
     * into an already-running topology waits for its epoch boundary to commit before failing (so Kafka
     * Streams restarts and retries the join) rather than proceeding on an unknown floor.
     *
     * @param epochEventsTopic the single-partition epoch-events log topic name
     * @param sourceTopics     the topology's external source topic names
     * @param joinTimeout      the bound on the join wait
     * @return a new coordination handle
     */
    public static CausalCoordination create(String epochEventsTopic, Set<String> sourceTopics,
                                            Duration joinTimeout) {
        Objects.requireNonNull(epochEventsTopic, "epochEventsTopic must not be null");
        Objects.requireNonNull(joinTimeout, "joinTimeout must not be null");
        return new CausalCoordination(epochEventsTopic, Set.copyOf(sourceTopics), joinTimeout, null);
    }

    /**
     * A handle over a pre-built {@code runtime} (bypassing the lazy Kafka build) for tests that drive an
     * {@link InMemoryEpochTransport}-backed runtime with no broker.
     */
    static CausalCoordination forRuntime(ParsleyEpochRuntime runtime, Set<String> sourceTopics) {
        return new CausalCoordination("", Set.copyOf(sourceTopics), DEFAULT_JOIN_TIMEOUT, runtime);
    }

    /** As {@link #forRuntime(ParsleyEpochRuntime, Set)} with an explicit join timeout (for timeout tests). */
    static CausalCoordination forRuntime(ParsleyEpochRuntime runtime, Set<String> sourceTopics, Duration joinTimeout) {
        return new CausalCoordination("", Set.copyOf(sourceTopics), joinTimeout, runtime);
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
            ParsleyEpochRuntime built =
                    new ParsleyEpochRuntime(new ParsleyKafkaEpochTransport(appConfigs, epochEventsTopic));
            built.start();
            lazyRuntime = built;
            return built;
        }
    }

    /**
     * The joiner handshake, called from a fresh task's {@code init()}: a node deployed into an
     * already-running (coordinated) topology must not begin consuming until an epoch computed <em>without
     * it</em> commits, so it never drags the floor's min-over-running-members toward its offset-0 position.
     *
     * <p>Waits for the runtime to fold the log to the end ({@link ParsleyEpochRuntime#isBootstrapped()},
     * so the committed epoch is accurate); then, once per instance, captures the join epoch and — only if
     * the topology is at an <strong>established</strong> epoch (committed &gt; 0; epoch 0 is static, with
     * no history to strip) — opens one snapshot round attributed to {@code memberId} and blocks every
     * participating task until an epoch strictly after the join epoch commits. At epoch 0 it returns
     * immediately (a cold start is unaffected). Throws (failing the task, so Streams restarts and retries)
     * if the timeout elapses first — never proceeds on an unknown floor.
     */
    void awaitJoinCommit(ParsleyEpochRuntime runtime, String memberId) {
        long deadlineNanos = System.nanoTime() + joinTimeout.toNanos();
        awaitBootstrap(runtime, deadlineNanos);

        long epoch;
        boolean required;
        synchronized (joinLock) {
            if (!joinInitiated) {
                joinInitiated = true;
                joinEpoch = runtime.committedEpochId();
                joinRequired = joinEpoch > 0;
                if (joinRequired) {
                    // Open the round the existing running nodes will answer; this joiner owns it, and its
                    // instance drives the commit once they have published (their punctuator-driven poll).
                    runtime.requestSnapshot(memberId);
                }
            }
            epoch = joinEpoch;
            required = joinRequired;
        }
        if (!required) {
            return;
        }
        while (runtime.committedEpochId() <= epoch) {
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
