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
 * A running Parsley application, created by {@link Parsley#streams} and started here.
 *
 * <p>Exposes a restricted subset of {@link KafkaStreams}, with no accessor for the underlying
 * instance. Where a member is absent, use the alternative:
 *
 * <ul>
 *   <li>Interactive queries and {@code store()}: observe state through sinks. The protocol
 *       store is internal, and under exactly-once semantics a local store holds the open
 *       transaction's writes until commit or abort, so a query can observe fold state that is
 *       not a function of any delivered history.</li>
 *   <li>{@code pause()} and {@code resume()}: stop the instance, or scale by instances.
 *       Pausing one instance freezes its release punctuator and stalls every instance waiting
 *       on its outputs.</li>
 *   <li>Thread add/remove and {@code cleanUp()}: scale by instances, and delete the state
 *       directory of a stopped instance.</li>
 * </ul>
 */
public final class CausalStreams implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalStreams.class);

    private final KafkaStreams streams;
    private final Admin admin;
    private final String applicationId;

    /**
     * Wraps an assembled but unstarted Kafka Streams client.
     *
     * @param streams the assembled client, not yet started
     * @param admin the admin client to close with this instance
     * @param applicationId the composition's id, which scopes {@link #explainHolds()}
     */
    CausalStreams(KafkaStreams streams, Admin admin, String applicationId) {
        this.streams = streams;
        this.admin = admin;
        this.applicationId = applicationId;
    }

    /**
     * Registers a state-transition listener. Call before {@link #start()}.
     *
     * @param listener the listener to notify on each state transition
     */
    public void setStateListener(KafkaStreams.StateListener listener) {
        streams.setStateListener(listener);
    }

    /**
     * Registers the processing-exception handler. Call before {@link #start()}.
     *
     * <p>The handler is registered totalized. One that throws, or returns {@code null},
     * resolves to {@code SHUTDOWN_CLIENT}, and the processing failure and the handler's own
     * failure are both logged.
     *
     * @param handler the handler to consult for each uncaught processing exception
     */
    public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler) {
        streams.setUncaughtExceptionHandler(t -> totalDecision(handler, t));
    }

    /**
     * Applies the handler as a total function, yielding its decision where it gives one and
     * {@code SHUTDOWN_CLIENT} otherwise.
     *
     * <p>Kafka Streams invokes the handler on the failing stream thread with no guard of its
     * own, so a handler failure would otherwise discard the decision, kill the thread without
     * a replacement, and leave the client reporting {@code RUNNING} with nothing processing.
     *
     * <p>Catches {@link Throwable} deliberately. On a handler {@link Error} the runtime may
     * already be compromised, and shutting down is better than losing the thread silently.
     *
     * @param handler the caller's handler
     * @param failure the processing failure to hand to it
     * @return the handler's decision, or {@code SHUTDOWN_CLIENT} if it threw or returned
     *         {@code null}
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

    /**
     * Registers a restore listener for the protocol and fold stores. Call before
     * {@link #start()}.
     *
     * @param listener the listener to notify as each store partition restores
     */
    public void setGlobalStateRestoreListener(StateRestoreListener listener) {
        streams.setGlobalStateRestoreListener(listener);
    }

    /** Starts the application. Register listeners before calling. */
    public void start() {
        streams.start();
    }

    /**
     * Returns the current runtime state.
     *
     * @return the state of the underlying Kafka Streams client
     */
    public KafkaStreams.State state() {
        return streams.state();
    }

    /**
     * Returns the Kafka Streams metrics for this instance.
     *
     * @return the metrics, keyed by name, including the {@code vc-metrics} group
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return streams.metrics();
    }

    /**
     * Returns metadata for each stream thread of this instance.
     *
     * @return one entry per local stream thread
     */
    public Set<ThreadMetadata> metadataForLocalThreads() {
        return streams.metadataForLocalThreads();
    }

    /**
     * Returns restoration lag for each local store partition, keyed by store name.
     *
     * <p>Covers the protocol store and the fold stores.
     *
     * @return the lag of each partition of each local store
     */
    public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags() {
        return streams.allLocalStorePartitionLags();
    }

    /**
     * Returns records currently held at the gate, longest-held first.
     *
     * <p>One entry per gating head across this instance's local tasks, each naming the causes
     * it still waits for and the local watermarks that show the gap.
     *
     * <p>A held record means a cause is missing, lagging, or unstamped. Deliver the cause. Do
     * not skip, reorder, or add a timeout. Each {@link HeldRecord.Diagnosis} names its case.
     *
     * <p>Returns coordinates, claims, and watermarks, never key or value bytes. Reads a
     * snapshot each task publishes from its own stream thread, refreshed on the wall-clock
     * punctuator. A reading may be that stale, and describes an in-flight view that an aborted
     * transaction may roll back.
     *
     * @return the held records, longest-held first, or an empty list when nothing is held
     */
    public List<HeldRecord> explainHolds() {
        return Holds.forApplication(applicationId);
    }

    /** Closes the application, and the admin client it was assembled with. */
    @Override
    public void close() {
        streams.close();
        admin.close();
    }

    /**
     * Closes within {@code timeout}.
     *
     * <p>The admin client is closed either way, including when the timeout elapses.
     *
     * @param timeout how long to wait for clean shutdown
     * @return {@code false} if the timeout elapsed first
     */
    public boolean close(Duration timeout) {
        try {
            return streams.close(timeout);
        } finally {
            admin.close();
        }
    }
}
