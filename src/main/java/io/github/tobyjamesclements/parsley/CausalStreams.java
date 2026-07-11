package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Properties;

/**
 * The causal application runtime: a Facade (GoF) over the Kafka Streams instance a {@link CausalTopology}
 * runs as, and the causal machinery a plain {@code KafkaStreams} doesn't know about — graceful causal
 * drain on shutdown, and (when configured) topology-epoch coordination — behind the one simple
 * start/close lifecycle below. Plays the same role {@link KafkaStreams} plays for a plain Kafka Streams
 * application:
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
        this.kafkaStreams = new KafkaStreams(assembled, props);
    }

    private static @Nullable ParsleyCoordination coordinationFrom(Properties props) {
        Properties merged = ParsleyConfig.loadProperties();
        merged.putAll(props);
        String epochEventsTopic = ParsleyConfig.from(merged).coordinationEpochEventsTopic();
        return epochEventsTopic == null ? null : ParsleyCoordination.create(epochEventsTopic);
    }

    /** Starts the underlying {@code KafkaStreams} instance. */
    public void start() {
        kafkaStreams.start();
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
