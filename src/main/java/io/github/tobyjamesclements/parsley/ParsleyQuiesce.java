package io.github.tobyjamesclements.parsley;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates graceful shutdown across a registered set of causal tasks or members, identified by an
 * opaque string id — a Kafka Streams {@code TaskId}'s string form for {@link CausalStreams}, or an
 * epoch member id (see {@link ParsleyEpochRuntime}). {@code CausalStreams} owns one {@code ParsleyQuiesce}
 * internally: every participating task registers with it at {@code init()}, and
 * {@code CausalStreams#close()} calls {@link #requestQuiesce()} then polls {@link #isSafeToClose()}
 * before stopping the underlying {@code KafkaStreams} — so a clean shutdown never strands a
 * causally-held record. There is no public handle; this is purely an internal shutdown mechanism.
 *
 * <p>{@link ParsleyEpochRuntime} reuses this same class for an unrelated but structurally identical
 * need — tracking which of its local members are currently drained, so {@link ParsleyCoordination#leave()}
 * waits until every local member is drained before removing it from the epoch domain. There, quiesce is
 * requested unconditionally at construction (an epoch leave always cares about drain state, never gated
 * on an external request), so {@link #isSafeToClose()} degenerates to "every registered member is
 * currently drained".
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
     * work: quiesce has been requested, at least one task is registered, and every registered task's
     * buffer is currently empty.
     *
     * @return {@code true} if every registered task is drained and quiesce was requested
     */
    boolean isSafeToClose() {
        return requested.get() && !registered.isEmpty() && drained.containsAll(registered);
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
