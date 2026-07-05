package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * The causal application runtime: wraps the Kafka Streams instance a {@link CausalTopology} runs as, and
 * owns the causal machinery a plain {@code KafkaStreams} doesn't know about — graceful causal drain on
 * shutdown, and (when configured) topology-epoch coordination. Plays the same role
 * {@link KafkaStreams} plays for a plain Kafka Streams application:
 *
 * <pre>{@code
 * CausalStreamsBuilder builder = new CausalStreamsBuilder();
 * builder.stream("orders", Serdes.String(), orderSerde)
 *        .process(new EnrichmentProcessorSupplier())
 *        .to("orders.enriched", Serdes.String(), enrichedSerde);
 * CausalTopology topology = builder.build();
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
 * {@link #requestEpochTransition()}.
 *
 * <p><strong>{@link #close()}</strong> always runs the full graceful shutdown: it waits for every task's
 * causal buffer to drain through the ordinary delivery path, then — if coordination is configured —
 * permanently decommissions this instance's members (so a restart always rejoins as a fresh member and
 * waits to be re-admitted; slower than resuming as the same running member, but there is no restart/leave
 * distinction for a caller to get wrong), before stopping the underlying {@code KafkaStreams}.
 */
public final class CausalStreams implements AutoCloseable {

    private static final Duration QUIESCE_POLL_INTERVAL = Duration.ofMillis(100);

    private final Properties props;
    private final KafkaStreams kafkaStreams;
    private final CausalQuiesce quiesce;
    private final @Nullable CausalCoordination coordination;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> topicAdminFactory;

    /**
     * Assembles {@code topology} into a real Kafka Streams topology and wraps a {@code KafkaStreams}
     * instance over it, blocking a topology-epoch transition (if configured) until every member has
     * published — see {@link CausalMembershipStrategy#blockUntilDrained()}.
     *
     * @param topology the causal topology to run
     * @param props    standard Kafka Streams configuration plus Parsley's {@code parsley.*} keys
     */
    public CausalStreams(CausalTopology topology, Properties props) {
        this(topology, props, CausalMembershipStrategy.blockUntilDrained(), ParsleyTopicAdmin::ofConfigs);
    }

    /**
     * As above, with the {@link ParsleyTopicAdmin} factory {@link #provisionDeadLetterTopic()} uses
     * overridden — a test seam, for substituting a stub admin without a real broker.
     */
    CausalStreams(CausalTopology topology, Properties props, CausalMembershipStrategy membershipStrategy,
                  Function<Map<String, Object>, ParsleyTopicAdmin> topicAdminFactory) {
        this.props = props;
        this.topicAdminFactory = topicAdminFactory;
        this.quiesce = CausalQuiesce.create();
        this.coordination = coordinationFrom(props, membershipStrategy);
        Topology assembled = topology.assemble(props, quiesce, coordination);
        this.kafkaStreams = new KafkaStreams(assembled, props);
    }

    private static @Nullable CausalCoordination coordinationFrom(
            Properties props, CausalMembershipStrategy membershipStrategy) {
        Properties merged = ParsleyConfig.loadProperties();
        merged.putAll(props);
        String epochEventsTopic = ParsleyConfig.from(merged).coordinationEpochEventsTopic();
        return epochEventsTopic == null ? null : CausalCoordination.create(epochEventsTopic, membershipStrategy);
    }

    /**
     * Provisions the dead-letter topic (if it does not already exist), then starts the underlying
     * {@code KafkaStreams} instance. Every stage's dead-letter sink writes to the same topic name {@link
     * CausalTopology#assemble} computes from these same {@code props} — {@code parsley.deadletter.topic}
     * if set, else {@code {application.id}-deadletter} — provisioned here with {@code
     * parsley.deadletter.partitions} partitions (default 1).
     */
    public void start() {
        provisionDeadLetterTopic();
        kafkaStreams.start();
    }

    /**
     * Creates the dead-letter topic if absent, tolerating a concurrent creation (another instance of
     * this application racing to create the same topic) — {@link ParsleyTopicAdmin#createTopic}'s
     * underlying {@code Admin.createTopics(...).all().get()} wraps an already-exists race as an {@link
     * ExecutionException} carrying a {@link TopicExistsException} cause, which is exactly the case this
     * tolerates; any other failure (e.g. broker unreachable) fails {@link #start()} fast. Package-private
     * (not called only from {@link #start()}) so a test can exercise it directly, without also
     * triggering the real {@code kafkaStreams.start()} broker connection attempt.
     */
    void provisionDeadLetterTopic() {
        ParsleyConfig config = resolveConfig(props);
        String applicationId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        String topic = config.deadLetterTopic() != null ? config.deadLetterTopic()
                : (applicationId == null || applicationId.isEmpty() ? "deadletter" : applicationId + "-deadletter");
        Map<String, Object> configs = new HashMap<>();
        props.forEach((key, value) -> configs.put(String.valueOf(key), value));
        try (ParsleyTopicAdmin admin = topicAdminFactory.apply(configs)) {
            admin.createTopic(topic, config.deadLetterPartitions());
        } catch (Exception e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("failed to provision the dead-letter topic '" + topic + "'", e);
            }
        }
    }

    /** Classpath {@code parsley.properties} as a base layer, overlaid with the runtime's {@code props}. */
    private static ParsleyConfig resolveConfig(Properties props) {
        Properties merged = ParsleyConfig.loadProperties();
        merged.putAll(props);
        return ParsleyConfig.from(merged);
    }

    /** The underlying {@code KafkaStreams} instance's current state. */
    public KafkaStreams.State state() {
        return kafkaStreams.state();
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
     */
    @Override
    public void close() {
        if (kafkaStreams.state() != KafkaStreams.State.CREATED) {
            quiesce.requestQuiesce();
            while (!quiesce.isSafeToClose()) {
                sleep();
            }
        }
        if (coordination != null) {
            coordination.leave();
        }
        kafkaStreams.close();
        if (coordination != null) {
            coordination.close();
        }
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
