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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The running application: a curated view of the Kafka Streams runtime, assembled by
 * {@link Parsley#streams} and started here. The surface is an allowlist — each member is
 * present because it is causally inert — and there is no accessor to the underlying
 * {@code KafkaStreams}, ever: any single escape hatch would reintroduce the full inherited
 * surface. The withheld members are withheld for different reasons, each with its
 * operational alternative:
 *
 * <ul>
 * <li>{@code store()} and interactive queries are the causal hazard. The protocol store is
 * internal, and under exactly-once semantics a local store holds the writes of the open
 * transaction until commit or abort, so a query can observe fold state that is not a
 * function of any delivered history. Observe state through sinks.</li>
 * <li>{@code pause()}/{@code resume()} are a liveness footgun, not a safety hazard. Pausing
 * one instance freezes its release punctuator, and every instance waiting on its outputs
 * stalls with it — a fleet-wide stall commanded from one handle. Stop the instance, or
 * scale by instances.</li>
 * <li>Thread add/remove and {@code cleanUp()} are causally inert and withheld only to keep
 * the surface minimal: thread-count changes are scaling, expressed by instances, and
 * cleanup equals deleting the state directory of a stopped instance. A minimal surface can
 * grow compatibly; a regretted member cannot be removed compatibly.</li>
 * </ul>
 */
public final class CausalStreams implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStreams.class);

    private final KafkaStreams streams;
    private final Admin admin;
    private final String applicationId;

    CausalStreams(KafkaStreams streams, Admin admin, String applicationId) {
        this.streams = streams;
        this.admin = admin;
        this.applicationId = applicationId;
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

    /**
     * Why records are currently waiting at the gate, longest-held first: one entry per gating
     * head across this instance's local tasks, each naming the causes it still waits for and
     * the local watermarks that show the gap. Empty when nothing is held.
     *
     * <p>This is the answer to "why is nothing coming out", and it exists so the correct next
     * step is cheaper than the wrong one. A held record means a cause is missing, lagging, or
     * unstamped; the fix is to deliver that cause, never to skip, reorder, or add a timeout.
     * Each {@link HeldRecord.Diagnosis} carries the documentation anchor for its case, and
     * {@code docs/guide/diagnosing-holds.md} walks them through.
     *
     * <p>It is present on this allowlist where {@code store()} is not, because it is causally
     * and operationally inert. It returns coordinates, claims, and watermarks — never key or
     * value bytes, so it cannot become a back door to state a query would have observed — it
     * reads only a snapshot each task published from its own stream thread, so it takes no
     * locks on the processing path, and it changes nothing. The snapshots refresh on the
     * task's wall-clock punctuator, so a reading is at most that stale, and it describes the
     * task's in-flight view, which an aborted transaction may roll back.
     */
    public List<HeldRecord> explainHolds() {
        return Holds.forApplication(applicationId);
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
