package io.github.tobyjamesclements.parsley;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates graceful shutdown across a registered set of causal tasks, identified by an opaque string
 * id — a Kafka Streams {@code TaskId}'s string form. {@code CausalStreams} owns one {@code ParsleyQuiesce}
 * internally: every participating task registers with it at {@code init()}, and
 * {@code CausalStreams#close()} calls {@link #requestQuiesce()} then polls {@link #isSafeToClose()}
 * before stopping the underlying {@code KafkaStreams} — so a clean shutdown never strands a
 * causally-held record. There is no public handle; this is purely an internal shutdown mechanism.
 *
 * <p>A task registered with a {@code ParsleyQuiesce} keeps processing normally after
 * {@link #requestQuiesce()} — nothing about how it delivers or forwards records changes. It only
 * reports itself drained once its causal buffer has emptied through the ordinary delivery path (a
 * held record's dependencies becoming satisfied by a later message, exactly as it would without
 * quiesce), so no synthetic progress is ever fabricated to force this. {@link #isSafeToClose()}
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
    private final Set<String> registered = ConcurrentHashMap.newKeySet();
    private final Set<String> drained = ConcurrentHashMap.newKeySet();

    ParsleyQuiesce() {}

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
     * work: quiesce has been requested and every registered task's buffer is currently empty. An
     * instance with <em>no</em> registered tasks (more instances than partitions, or every task
     * migrated away — each task unregisters on its own {@code close()}) is trivially safe: it holds
     * nothing that could be stranded. Requiring a registered task here instead would hang
     * {@link CausalStreams#close()}'s drain wait forever on such an instance, which stays {@code RUNNING}
     * (so the dead-instance escape never fires) yet can never report a registered task drained.
     *
     * @return {@code true} once quiesce was requested and every registered task (possibly none) is drained
     */
    boolean isSafeToClose() {
        return requested.get() && drained.containsAll(registered);
    }

    /** Registers a member, called by {@link ParsleyProcessor#init}. */
    void register(String memberId) {
        registered.add(memberId);
    }

    /** Unregisters a member, called by {@link ParsleyProcessor#close}. */
    void unregister(String memberId) {
        registered.remove(memberId);
        drained.remove(memberId);
    }

    /** Marks a member drained (buffer empty) or not, called after every buffer-depth-changing event. */
    void setDrained(String memberId, boolean isDrained) {
        if (isDrained) {
            drained.add(memberId);
        } else {
            drained.remove(memberId);
        }
    }
}
