package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerConfig;
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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * The causal application runtime: a Facade (GoF) over the Kafka Streams instance a {@link CausalTopology}
 * runs as, and the causal machinery a plain {@code KafkaStreams} doesn't know about — graceful causal
 * drain on shutdown, and (when configured) topology-epoch coordination — behind the one simple
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
 * <p><strong>Topology-epoch coordination</strong> turns on by setting
 * {@code parsley.coordination.epoch-events-topic} in {@code props}; {@code application.id} supplies the
 * epoch member identity. Absent that key, the topology runs in epoch 0 — no epoch-events log, no
 * coordination thread. Evolve a running, coordinated topology through an epoch boundary with
 * {@link #requestEpochTransition()}. A transition blocks — unbounded — until every running member has
 * published; see {@link ParsleyCoordination#leave()} for how a member is removed from the domain.
 *
 * <p><strong>{@link #close()}</strong> always runs the full graceful shutdown: it waits for every task's
 * causal buffer to drain through the ordinary delivery path, then — if coordination is configured —
 * permanently decommissions this instance's members (so a restart always rejoins as a fresh member and
 * waits to be re-admitted; slower than resuming as the same running member, but there is no restart/leave
 * distinction for a caller to get wrong), before stopping the underlying {@code KafkaStreams}.
 */
public final class CausalStreams implements AutoCloseable {

    private static final Duration QUIESCE_POLL_INTERVAL = Duration.ofMillis(100);

    private final KafkaStreams kafkaStreams;
    private final ParsleyQuiesce quiesce;
    private final @Nullable ParsleyCoordination coordination;
    // The minted id under which this instance's producer-ack registry is registered JVM-wide (D2):
    // injected into props as producer.<CONFIG_KEY> so the interceptor (producer side) and each
    // task's init (via appConfigs) resolve the same registry; unregistered at close().
    private final String ownOutputRegistryId;
    // Everything the pre-start offset seeding needs, captured at construction: the consumer group id
    // (application.id), every causal source topic to seed (business and passthrough alike, read off the
    // assembled topology), and the admin configuration to reach the broker. See seedSourceOffsets().
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
        this.coordination = coordinationFrom(props);
        Topology assembled = topology.assemble(props, quiesce, coordination);
        this.ownOutputRegistryId = registerOwnOutputTracking(topology, props);
        this.kafkaStreams = new KafkaStreams(assembled, props);
        this.applicationId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        this.sourceTopics = sourceTopicsOf(assembled);
        this.changelogTopics = changelogTopicsOf(assembled, applicationId);
        this.adminConfigs = new HashMap<>();
        props.forEach((key, value) -> adminConfigs.put(String.valueOf(key), value));
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
    private static String registerOwnOutputTracking(CausalTopology topology, Properties props) {
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

    private static @Nullable ParsleyCoordination coordinationFrom(Properties props) {
        Properties merged = ParsleyConfig.loadProperties();
        merged.putAll(props);
        String epochEventsTopic = ParsleyConfig.from(merged).coordinationEpochEventsTopic();
        return epochEventsTopic == null ? null : ParsleyCoordination.create(epochEventsTopic);
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
        kafkaStreams.start();
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
     * Sets the handler invoked when a stream thread dies from an uncaught exception, delegating to the
     * underlying {@code KafkaStreams}. Set it before {@link #start()}.
     */
    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler) {
        kafkaStreams.setUncaughtExceptionHandler(handler);
    }

    /**
     * Requests an epoch transition across the currently-running nodes, evolving a running, coordinated
     * topology through an epoch boundary.
     *
     * @throws IllegalStateException if topology-epoch coordination is not configured (see
     *         {@code parsley.coordination.epoch-events-topic}), or no task has initialised it yet
     */
    public void requestEpochTransition() {
        if (coordination == null) {
            throw new IllegalStateException("topology-epoch coordination is not configured — set "
                    + "parsley.coordination.epoch-events-topic to enable it");
        }
        coordination.requestEpochTransition();
    }

    /**
     * Gracefully shuts down: waits — unbounded, no timeout — for every task's causal buffer to drain
     * through the ordinary delivery path, then, if coordination is configured, permanently decommissions
     * this instance's members from the epoch domain, then stops the underlying {@code KafkaStreams}, then
     * closes the coordination runtime. Idempotent-safe to call even if {@link #start()} was never called.
     *
     * <p>The drain wait is unbounded only while draining can actually progress: if the underlying
     * streams instance leaves {@code RUNNING}/{@code REBALANCING} (it died in {@code ERROR}, or was
     * already stopped), no task will ever deliver again, so the wait ends and shutdown proceeds — every
     * held record is changelog-backed and survives to the next start, so nothing is lost by closing an
     * already-dead instance (see {@link ParsleyQuiesce}: the drain is a stall-avoidance optimisation,
     * not a correctness requirement). The coordination decommission honours the same liveness rule: on
     * an instance that can no longer drain, {@link ParsleyCoordination#leave} abandons the removal
     * (members stay in the domain, exactly as a crash would leave them) instead of hanging on a buffer
     * no task will ever empty.
     */
    @Override
    public void close() {
        if (kafkaStreams.state() != KafkaStreams.State.CREATED) {
            quiesce.requestQuiesce();
            awaitDrain(quiesce, kafkaStreams::state);
        }
        if (coordination != null) {
            coordination.leave(() -> canStillDrain(kafkaStreams.state()));
        }
        kafkaStreams.close();
        ParsleyOwnOutputRegistry.unregister(ownOutputRegistryId);
        if (coordination != null) {
            coordination.close();
        }
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

    /** Whether tasks in {@code state} can still deliver records — the liveness rule both {@link
     * #awaitDrain} and the coordination decommission's drain wait poll. */
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
