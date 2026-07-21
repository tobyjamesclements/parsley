package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The causal application runtime: a Facade (GoF) over the Kafka Streams instance a {@link CausalTopology}
 * runs as, and the causal machinery a plain {@code KafkaStreams} doesn't know about — graceful causal
 * drain on shutdown — behind the one simple
 * start/close lifecycle below. Plays the same role {@link KafkaStreams} plays for a plain Kafka Streams
 * application:
 *
 * <pre>{@code
 * CausalTopology topology = new CausalStreamsBuilder()
 *         .stream("orders", Serdes.String(), orderSerde)
 *         .process(new EnrichmentProcessorSupplier())
 *         .to("orders.enriched", Serdes.String(), enrichedSerde)
 *         .build();
 *
 * CausalStreams causalStreams = new CausalStreams(topology, props);
 * causalStreams.start();
 *
 * Runtime.getRuntime().addShutdownHook(new Thread(causalStreams::close));
 * }</pre>
 *
 * <p><strong>Joins need zero coordination</strong>: a fresh application simply starts consuming, and
 * its replay self-gates into causal order. There is no membership, no epoch, and no join barrier; no
 * key under {@code parsley.coordination.*} is part of the configuration surface, and startup fails
 * if one is present (see {@link ParsleyConfig}).
 *
 * <p><strong>{@link #close()}</strong> always runs the full graceful shutdown: it waits for every task's
 * causal buffer to drain through the ordinary delivery path before stopping the underlying
 * {@code KafkaStreams}. <strong>{@link #close(Duration)}</strong> is the bounded form for callers
 * that cannot block unbounded — a JVM shutdown hook above all. It never delivers a record early to
 * meet its deadline; a truncated drain only defers held records to the next start's replay.
 */
public final class CausalStreams implements AutoCloseable {

    private static final Duration QUIESCE_POLL_INTERVAL = Duration.ofMillis(100);
    // How often the topic-identity watch compares the broker's current topic IDs against what the
    // tasks resolved at init (E1 / T3.0 A13). Also the bound on the mislabelling window a mid-run
    // delete+recreate can open before the member fails fast — see ParsleyTopicIdentityWatch.
    private static final Duration TOPIC_IDENTITY_POLL_INTERVAL = Duration.ofSeconds(5);

    private final KafkaStreams kafkaStreams;
    private final ParsleyQuiesce quiesce;
    // The minted id under which this instance's producer-ack registry is registered JVM-wide (D2):
    // injected into props as producer.<CONFIG_KEY> so the interceptor (producer side) and each
    // task's init (via appConfigs) resolve the same registry; unregistered at close().
    private final String ownOutputRegistryId;
    // The minted id and instance of this application's topic-identity watch (E1 / T3.0 A13): tasks
    // register their init-time name → UUID resolutions and check intactness per record; start()
    // spins the poll loop below and close() tears it down + unregisters.
    private final String topicIdentityWatchId;
    private final ParsleyTopicIdentityWatch topicIdentityWatch;
    private @Nullable ScheduledExecutorService identityPollExecutor;
    private @Nullable ParsleyTopicAdmin identityPollAdmin;
    // Everything the pre-start offset seeding needs, captured at construction: the consumer group id
    // (application.id), every causal source topic to seed (read off the assembled topology), and the
    // admin configuration to reach the broker. See seedSourceOffsets().
    private final @Nullable String applicationId;
    private final Set<String> sourceTopics;
    private final Set<String> changelogTopics;
    private final Map<String, Object> adminConfigs;

    /**
     * Assembles {@code topology} into a real Kafka Streams topology and wraps a {@code KafkaStreams}
     * instance over it.
     *
     * @param topology the causal topology to run
     * @param props    standard Kafka Streams configuration plus Parsley's {@code parsley.*} keys
     */
    public CausalStreams(CausalTopology topology, Properties props) {
        this.quiesce = new ParsleyQuiesce();
        // Work on a copy of the caller's Properties: the interceptor entry and the minted
        // registry/watch ids are injected below, so mutating the caller's object would let a
        // second instance built from the same Properties duplicate the interceptor and
        // cross-wire the ids.
        Properties effective = new Properties();
        props.forEach(effective::put);
        Topology assembled = topology.assemble(effective, quiesce);
        this.ownOutputRegistryId = registerOwnOutputTracking(topology, effective);
        this.topicIdentityWatchId = UUID.randomUUID().toString();
        this.topicIdentityWatch = new ParsleyTopicIdentityWatch();
        ParsleyTopicIdentityWatch.register(topicIdentityWatchId, topicIdentityWatch);
        effective.put("producer." + ParsleyTopicIdentityWatch.CONFIG_KEY, topicIdentityWatchId);
        // The two JVM-wide registrations above precede this throwing constructor (a bad Streams
        // config throws here), and a failed construction hands the caller no instance to close() —
        // so the registrations must be rolled back on the way out or they leak forever.
        KafkaStreams streams;
        try {
            streams = new KafkaStreams(assembled, effective);
        } catch (RuntimeException e) {
            ParsleyOwnOutputRegistry.unregister(ownOutputRegistryId);
            ParsleyTopicIdentityWatch.unregister(topicIdentityWatchId);
            throw e;
        }
        this.kafkaStreams = streams;
        this.applicationId = effective.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        this.sourceTopics = sourceTopicsOf(assembled);
        this.changelogTopics = changelogTopicsOf(assembled, applicationId);
        this.adminConfigs = new HashMap<>();
        effective.forEach((key, value) -> adminConfigs.put(String.valueOf(key), value));
    }

    /** The minted id this instance's producer-ack registry is registered under. Test seam. */
    String ownOutputRegistryId() {
        return ownOutputRegistryId;
    }

    /** Every source topic the assembled topology consumes — the causal sources to seed before start. */
    private static Set<String> sourceTopicsOf(Topology assembled) {
        Set<String> topics = new LinkedHashSet<>();
        for (TopologyDescription.Subtopology subtopology : assembled.describe().subtopologies()) {
            for (TopologyDescription.Node node : subtopology.nodes()) {
                if (node instanceof TopologyDescription.Source source && source.topicSet() != null) {
                    topics.addAll(source.topicSet());
                }
            }
        }
        return topics;
    }

    /**
     * The exact changelog topic names for this application's state stores — the marker of surviving causal
     * state the offset seeder checks for. Derived from the assembled topology's actual store names
     * ({@code <application.id>-<store>-changelog}), not a name prefix, so an unrelated application whose id
     * merely shares this one's prefix (e.g. {@code orders} vs {@code orders-enrichment}) cannot be mistaken
     * for this application's own state.
     */
    private static Set<String> changelogTopicsOf(Topology assembled, @Nullable String applicationId) {
        if (applicationId == null || applicationId.isEmpty()) {
            return Set.of();
        }
        Set<String> changelogs = new LinkedHashSet<>();
        for (TopologyDescription.Subtopology subtopology : assembled.describe().subtopologies()) {
            for (TopologyDescription.Node node : subtopology.nodes()) {
                if (node instanceof TopologyDescription.Processor processor) {
                    for (String store : processor.stores()) {
                        changelogs.add(applicationId + "-" + store + "-changelog");
                    }
                }
            }
        }
        return changelogs;
    }

    /**
     * Creates and registers this instance's producer-ack registry (the {@code ownOutputs} clock's
     * feed, D2) and injects the wiring into {@code props} before the {@code KafkaStreams} instance
     * is built from them: {@link ParsleyOwnOutputInterceptor} is appended to any user-configured
     * {@code producer.interceptor.classes} (never replacing them), and the minted registry id rides
     * the same public {@code producer.} prefix so every stream producer's interceptor and every
     * task's init resolve the same registry — no {@code *.internals.*} type is touched (O1).
     *
     * @return the minted registry id, for {@link #close()} to unregister
     */
    // Package-private for direct unit coverage: the effective Properties a KafkaStreams instance
    // is built from are a construction-time copy, no longer observable through the caller's object.
    static String registerOwnOutputTracking(CausalTopology topology, Properties props) {
        String registryId = UUID.randomUUID().toString();
        ParsleyOwnOutputRegistry.register(registryId, new ParsleyOwnOutputRegistry(topology.sinkTopics()));
        String interceptorsKey = "producer." + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
        Object existing = props.get(interceptorsKey);
        if (existing instanceof List<?> list) {
            List<Object> appended = new ArrayList<>(list);
            appended.add(ParsleyOwnOutputInterceptor.class.getName());
            props.put(interceptorsKey, appended);
        } else if (existing != null && !existing.toString().isBlank()) {
            props.put(interceptorsKey, existing + "," + ParsleyOwnOutputInterceptor.class.getName());
        } else {
            props.put(interceptorsKey, ParsleyOwnOutputInterceptor.class.getName());
        }
        props.put("producer." + ParsleyOwnOutputRegistry.CONFIG_KEY, registryId);
        return registryId;
    }

    /**
     * Seeds any un-committed causal source offset for a genuine first start, then starts the underlying
     * {@code KafkaStreams} instance. The seeding runs first, and before the group is joined, so a first
     * start does not trip the {@code AutoOffsetReset.none()} every causal source is declared with; a real
     * out-of-range offset (data loss) is deliberately left to fail fast under {@code none()}. Seeding
     * failures — surviving state with a missing offset, an absent source topic, or an unreachable broker —
     * abort the start loudly (see {@link ParsleyOffsetSeeder}).
     */
    public void start() {
        seedSourceOffsets();
        startTopicIdentityPolling();
        kafkaStreams.start();
    }

    /**
     * Starts the topic-identity poll loop (E1 / T3.0 A13): a single daemon thread comparing the
     * broker's current topic IDs against every binding the tasks resolved at init, on a fixed
     * interval. A detected delete or delete+recreate marks the watch broken; the tasks' per-record
     * checks then fail the member fast (see {@link ParsleyTopicIdentityWatch}). The poll itself
     * never throws — transient admin failures are logged and retried next tick.
     */
    private void startTopicIdentityPolling() {
        ParsleyTopicAdmin admin = ParsleyTopicAdmin.ofConfigs(adminConfigs);
        this.identityPollAdmin = admin;
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "parsley-topic-identity-watch");
            thread.setDaemon(true);
            return thread;
        });
        this.identityPollExecutor = executor;
        long intervalMs = TOPIC_IDENTITY_POLL_INTERVAL.toMillis();
        executor.scheduleWithFixedDelay(() -> topicIdentityWatch.poll(admin),
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void seedSourceOffsets() {
        if (applicationId == null || applicationId.isEmpty() || sourceTopics.isEmpty()) {
            return;
        }
        try (ParsleySeedAdmin admin = ParsleySeedAdmin.ofConfigs(adminConfigs)) {
            ParsleyOffsetSeeder.seed(admin, applicationId, sourceTopics, changelogTopics);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to seed causal source offsets before starting '"
                    + applicationId + "'", e);
        }
    }

    /** The underlying {@code KafkaStreams} instance's current state. */
    public KafkaStreams.State state() {
        return kafkaStreams.state();
    }

    /**
     * The underlying {@code KafkaStreams} instance's metrics, Parsley's own per-task sensors
     * included (records buffered and released, deserialization and clock-resolution failures,
     * ignored out-of-scope dependencies, and the rest of the {@code parsley} group) — the
     * in-process way to reach them without going through JMX.
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaStreams.metrics();
    }

    /**
     * Registers a listener notified on each state transition of the underlying
     * {@code KafkaStreams}, delegating to it. Set it before {@link #start()}.
     */
    public void setStateListener(KafkaStreams.StateListener listener) {
        kafkaStreams.setStateListener(listener);
    }

    /**
     * Sets the handler invoked when a stream thread dies from an uncaught exception, delegating to the
     * underlying {@code KafkaStreams}. Set it before {@link #start()}.
     */
    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler) {
        kafkaStreams.setUncaughtExceptionHandler(handler);
    }

    /**
     * Gracefully shuts down: waits — unbounded, no timeout — for every task's causal buffer to drain
     * through the ordinary delivery path, then stops the underlying {@code KafkaStreams}.
     * Idempotent-safe to call even if {@link #start()} was never called.
     *
     * <p>The drain wait is unbounded only while draining can actually progress: if the underlying
     * streams instance leaves {@code RUNNING}/{@code REBALANCING} (it died in {@code ERROR}, or was
     * already stopped), no task will ever deliver again, so the wait ends and shutdown proceeds — every
     * held record is changelog-backed and survives to the next start, so nothing is lost by closing an
     * already-dead instance (see {@link ParsleyQuiesce}: the drain is a stall-avoidance optimisation,
     * not a correctness requirement).
     */
    @Override
    public void close() {
        if (kafkaStreams.state() != KafkaStreams.State.CREATED) {
            quiesce.requestQuiesce();
            awaitDrain(quiesce, kafkaStreams::state);
        }
        kafkaStreams.close();
        releaseResources();
    }

    /**
     * Gracefully shuts down within a time budget, for callers that cannot block unbounded — a JVM
     * shutdown hook above all. The budget spans the whole shutdown: first the causal drain wait,
     * then stopping the underlying {@code KafkaStreams} with whatever remains, mirroring
     * {@link KafkaStreams#close(Duration)}.
     *
     * <p>Giving up on the drain is always causally safe — records are <em>never</em> delivered
     * early to beat the deadline. A truncated drain only leaves held records in the changelog-backed
     * buffer, exactly as an ungraceful stop would, to be replayed and delivered in causal order on
     * the next start (see {@link ParsleyQuiesce}: the drain is a stall-avoidance optimisation, not a
     * correctness requirement). Prefer {@link #close()} for routine shutdown, where an
     * unbounded-but-progressing drain avoids that replay cost.
     *
     * @param timeout the maximum time to spend on drain plus shutdown; {@code Duration.ZERO} skips
     *                straight to an immediate close
     * @return {@code true} if the underlying streams instance fully stopped within the budget
     */
    public boolean close(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        if (kafkaStreams.state() != KafkaStreams.State.CREATED) {
            quiesce.requestQuiesce();
            awaitDrain(quiesce, kafkaStreams::state, deadlineNanos, System::nanoTime);
        }
        long remainingMs = Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
        boolean stopped = kafkaStreams.close(Duration.ofMillis(remainingMs));
        releaseResources();
        return stopped;
    }

    /**
     * Tears down everything {@code close} must release regardless of how the drain ended: the
     * JVM-wide registry and watch registrations, the identity-poll executor, and its admin client.
     * Idempotent, so a bounded close after an unbounded one (or vice versa) is harmless.
     */
    private void releaseResources() {
        ParsleyOwnOutputRegistry.unregister(ownOutputRegistryId);
        ScheduledExecutorService executor = identityPollExecutor;
        if (executor != null) {
            executor.shutdownNow();
        }
        ParsleyTopicAdmin admin = identityPollAdmin;
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                // Closing a poll-only admin client can fail only on shutdown races; nothing to do.
            }
        }
        ParsleyTopicIdentityWatch.unregister(topicIdentityWatchId);
    }

    /**
     * Polls until every registered task reports drained ({@link ParsleyQuiesce#isSafeToClose}), or
     * {@code state} reports the streams instance can no longer drain anything — any state other than
     * {@code RUNNING} or {@code REBALANCING}, where tasks are gone or dying and a task's buffer could
     * otherwise never report drained, hanging {@code close()} forever on an instance that is already
     * dead. An instance with zero registered tasks is safe to close at once (nothing to strand; see
     * {@link ParsleyQuiesce#isSafeToClose}), so this returns immediately rather than spinning while it
     * sits {@code RUNNING} with no task that could ever drain. Package-private for tests; {@link #close()}
     * is the only production caller.
     */
    static void awaitDrain(ParsleyQuiesce quiesce, Supplier<KafkaStreams.State> state) {
        while (!quiesce.isSafeToClose()) {
            if (!canStillDrain(state.get())) {
                return;
            }
            sleep();
        }
    }

    /**
     * The deadline-bounded variant {@link #close(Duration)} uses: identical to
     * {@link #awaitDrain(ParsleyQuiesce, Supplier)} except the wait also ends once {@code nanoTime}
     * reaches {@code deadlineNanos}. Giving up never delivers anything early — the un-drained
     * records stay in the changelog-backed buffer for the next start. {@code nanoTime} is a seam so
     * tests drive the deadline without real waiting; production passes {@code System::nanoTime}.
     *
     * @return {@code true} if every registered task drained, {@code false} on the deadline or the
     *         dead-instance escape
     */
    static boolean awaitDrain(ParsleyQuiesce quiesce, Supplier<KafkaStreams.State> state,
                              long deadlineNanos, LongSupplier nanoTime) {
        while (!quiesce.isSafeToClose()) {
            if (!canStillDrain(state.get())) {
                return false;
            }
            if (nanoTime.getAsLong() - deadlineNanos >= 0) {
                return false;
            }
            sleep();
        }
        return true;
    }

    /** Whether tasks in {@code state} can still deliver records — the liveness rule
     * {@link #awaitDrain} polls. */
    private static boolean canStillDrain(KafkaStreams.State state) {
        return state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING;
    }

    private static void sleep() {
        try {
            Thread.sleep(QUIESCE_POLL_INTERVAL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the causal buffer to drain", e);
        }
    }
}
