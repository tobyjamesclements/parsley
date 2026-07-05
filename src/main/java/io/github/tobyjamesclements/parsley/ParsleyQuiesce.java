package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates graceful shutdown across every causal task in one {@link CausalStreams} runtime
 * instance. {@code CausalStreams} owns one {@code ParsleyQuiesce} internally: every participating
 * task registers with it at {@code init()}, and {@code CausalStreams#close()} calls
 * {@link #requestQuiesce()} then polls {@link #isSafeToClose()} before stopping the underlying
 * {@code KafkaStreams} — so a clean shutdown never strands a causally-held record. There is no
 * public handle; this is purely {@code CausalStreams}' internal shutdown mechanism.
 *
 * <p>A task registered with a {@code ParsleyQuiesce} keeps processing normally after
 * {@link #requestQuiesce()} — nothing about how it delivers or forwards records changes. It only
 * reports itself drained once its causal buffer has emptied through the ordinary delivery path (a
 * held record's dependencies becoming satisfied by a later message, exactly as it would without
 * quiesce), so no synthetic completeness is ever fabricated to force this. {@link #isSafeToClose()}
 * becomes {@code true} once quiesce has been requested and every currently registered task is
 * drained.
 *
 * <p><strong>This is a stall-avoidance optimization, not a correctness requirement.</strong> Every
 * held record is already changelog-backed and survives an ungraceful stop; closing without ever
 * calling {@link #requestQuiesce()} loses nothing but resumes with a non-empty buffer to replay.
 *
 * <p><strong>Thread-safety:</strong> safe to share across every task/partition's own Kafka Streams
 * thread, and to poll from an unrelated shutdown thread.
 */
final class ParsleyQuiesce {

    private final AtomicBoolean requested = new AtomicBoolean(false);
    private final Set<TaskId> registered = ConcurrentHashMap.newKeySet();
    private final Set<TaskId> drained = ConcurrentHashMap.newKeySet();

    private ParsleyQuiesce() {}

    /**
     * Creates a new, not-yet-requested {@code ParsleyQuiesce} with no registered tasks.
     *
     * @return a new {@code ParsleyQuiesce}
     */
    static ParsleyQuiesce create() {
        return new ParsleyQuiesce();
    }

    /**
     * Requests quiesce: every registered task starts reporting itself drained once its buffer
     * empties, so {@link #isSafeToClose()} can eventually become {@code true}. Idempotent.
     */
    void requestQuiesce() {
        requested.set(true);
    }

    /**
     * Whether {@link #requestQuiesce()} has been called.
     *
     * @return {@code true} once quiesce has been requested
     */
    boolean isQuiesceRequested() {
        return requested.get();
    }

    /**
     * Whether it is safe to call {@code KafkaStreams#close} without stranding causally-buffered
     * work: quiesce has been requested, at least one task is registered, and every registered task's
     * buffer is currently empty.
     *
     * @return {@code true} if every registered task is drained and quiesce was requested
     */
    boolean isSafeToClose() {
        return requested.get() && !registered.isEmpty() && drained.containsAll(registered);
    }

    /** Registers a task, called by {@link ParsleyProcessor#init}. */
    void register(TaskId taskId) {
        registered.add(taskId);
    }

    /** Unregisters a task, called by {@link ParsleyProcessor#close}. */
    void unregister(TaskId taskId) {
        registered.remove(taskId);
        drained.remove(taskId);
    }

    /** Marks a task drained (buffer empty) or not, called after every buffer-depth-changing event. */
    void setDrained(TaskId taskId, boolean isDrained) {
        if (isDrained) {
            drained.add(taskId);
        } else {
            drained.remove(taskId);
        }
    }
}
