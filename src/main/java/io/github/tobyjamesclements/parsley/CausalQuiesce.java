package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shared handle for coordinating graceful shutdown across every causal task in one application
 * instance. Register the same {@code CausalQuiesce} with every {@link CausalProcessors} /
 * {@link CausalStreams} builder whose tasks should participate, then, from your own shutdown path
 * (e.g. a signal handler), call {@link #requestQuiesce()} and poll {@link #isSafeToClose()} before
 * calling {@code KafkaStreams#close}:
 *
 * <pre>{@code
 * CausalQuiesce quiesce = CausalQuiesce.create();
 * // ... pass quiesce to every CausalProcessors/CausalStreams builder via withQuiesce(quiesce) ...
 * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
 *     quiesce.requestQuiesce();
 *     while (!quiesce.isSafeToClose()) {
 *         Thread.sleep(100);
 *     }
 *     streams.close();
 * }));
 * }</pre>
 *
 * <p>A task registered with a {@code CausalQuiesce} keeps processing normally after
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
public final class CausalQuiesce {

    private final AtomicBoolean requested = new AtomicBoolean(false);
    private final Set<TaskId> registered = ConcurrentHashMap.newKeySet();
    private final Set<TaskId> drained = ConcurrentHashMap.newKeySet();

    private CausalQuiesce() {}

    /**
     * Creates a new, not-yet-requested {@code CausalQuiesce} with no registered tasks.
     *
     * @return a new {@code CausalQuiesce}
     */
    public static CausalQuiesce create() {
        return new CausalQuiesce();
    }

    /**
     * Requests quiesce: every registered task starts reporting itself drained once its buffer
     * empties, so {@link #isSafeToClose()} can eventually become {@code true}. Idempotent.
     */
    public void requestQuiesce() {
        requested.set(true);
    }

    /**
     * Whether {@link #requestQuiesce()} has been called.
     *
     * @return {@code true} once quiesce has been requested
     */
    public boolean isQuiesceRequested() {
        return requested.get();
    }

    /**
     * Whether it is safe to call {@code KafkaStreams#close} without stranding causally-buffered
     * work: quiesce has been requested, at least one task is registered, and every registered task's
     * buffer is currently empty.
     *
     * @return {@code true} if every registered task is drained and quiesce was requested
     */
    public boolean isSafeToClose() {
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
