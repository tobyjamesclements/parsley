package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * The running application: a curated view of the Kafka Streams runtime, assembled by
 * {@link Parsley#streams} and started here. The surface is an allowlist — each member is
 * present because it is causally inert — and there is no accessor to the underlying
 * {@code KafkaStreams}, ever: any single escape hatch would reintroduce the full inherited
 * surface, parts of which can violate causality (pausing one instance freezes its release
 * punctuator and causally stalls other instances; the protocol stores must not be handed
 * out).
 *
 * <p>Absent by design, with the operational alternative: {@code pause()}/{@code resume()}
 * (stop the instance or scale by instances), {@code store()} and interactive queries (the
 * protocol and fold stores are internal; observe through sinks), thread add/remove
 * (replace the instance), {@code cleanUp()} (delete the state directory of a stopped
 * instance).
 */
public final class CausalStreams implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStreams.class);

    private final KafkaStreams streams;
    private final Admin admin;

    CausalStreams(KafkaStreams streams, Admin admin) {
        this.streams = streams;
        this.admin = admin;
    }

    /** Registers a state-transition listener. Before {@link #start()}. */
    public void setStateListener(KafkaStreams.StateListener listener) {
        streams.setStateListener(listener);
    }

    /**
     * Registers the processing-exception handler. Before {@link #start()}.
     *
     * <p>The handler is registered totalized: a handler that throws, or returns {@code null},
     * resolves to {@code SHUTDOWN_CLIENT}, with the processing failure and the handler's own
     * failure both logged. Kafka Streams invokes the handler on the failing stream thread with
     * no guard of its own, so a handler failure would otherwise discard the decision, kill the
     * thread without a replacement, and leave the client reporting {@code RUNNING} with nothing
     * processing. Shutting down instead keeps handler failure loud.
     */
    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler) {
        streams.setUncaughtExceptionHandler(t -> totalDecision(handler, t));
    }

    /**
     * Applies the handler as a total function: its decision when it yields one, otherwise
     * {@code SHUTDOWN_CLIENT}. Catches {@link Throwable} deliberately: on a handler {@link Error}
     * the runtime may already be compromised, and shutting down is strictly better than losing
     * the thread silently.
     */
    static StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse totalDecision(
            StreamsUncaughtExceptionHandler handler, Throwable failure) {
        try {
            var response = handler.handle(failure);
            if (response != null) return response;
            LOG.error("uncaught exception handler returned null; shutting down the client."
                    + " Processing failure:", failure);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        } catch (Throwable fromHandler) {
            LOG.error("uncaught exception handler failed; shutting down the client."
                    + " Handler failure:", fromHandler);
            LOG.error("Processing failure that reached the handler:", failure);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }
    }

    /** Registers a restore listener for the protocol and fold stores. Before {@link #start()}. */
    public void setGlobalStateRestoreListener(StateRestoreListener listener) {
        streams.setGlobalStateRestoreListener(listener);
    }

    public void start() {
        streams.start();
    }

    public KafkaStreams.State state() {
        return streams.state();
    }

    public Map<MetricName, ? extends Metric> metrics() {
        return streams.metrics();
    }

    public Set<ThreadMetadata> metadataForLocalThreads() {
        return streams.metadataForLocalThreads();
    }

    /** Store-partition restoration lag, keyed by store name; covers the protocol and fold stores. */
    public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags() {
        return streams.allLocalStorePartitionLags();
    }

    @Override
    public void close() {
        streams.close();
        admin.close();
    }

    /** Bounded shutdown; {@code false} when the timeout elapsed first. */
    public boolean close(Duration timeout) {
        try {
            return streams.close(timeout);
        } finally {
            admin.close();
        }
    }
}
