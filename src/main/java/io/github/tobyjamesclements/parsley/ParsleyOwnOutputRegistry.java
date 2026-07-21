package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The producer-acknowledgement registry behind the {@code ownOutputs} clock (D2): the concurrent
 * hand-off between {@link ParsleyOwnOutputInterceptor}'s callbacks — which run on the producer's
 * network thread — and the stream threads that fold acknowledged coordinates into
 * {@link ParsleyChannels#acknowledge} before each stamp. The name is a coinage: no literature
 * analog — the mechanism exists only because the broker performs the sender's clock increment
 * (offset assignment) and reports it asynchronously through the producer ack path.
 *
 * <p>One registry exists per {@link CausalStreams} instance, registered in a JVM-wide map under a
 * minted id that travels to every one of the instance's producers through the public
 * {@code producer.} config prefix ({@link #CONFIG_KEY}) — the interceptor and the processor path
 * both resolve the same instance from configuration, so no {@code *.internals.*} type is touched
 * and unrelated {@code CausalStreams} instances in one JVM (tests) cannot cross-talk.
 *
 * <p>Two structures with deliberately different granularity (T2.1's carry-forward — under
 * {@code exactly_once_v2} there is one producer per <em>StreamThread</em>, not per task, so
 * {@code client.id} is not a task identity):
 * <ul>
 *   <li><strong>Acked offsets are a global max per (topic, partition)</strong>, across every
 *       producer of this instance. A task folds the coordinates of its own sink topics; if a
 *       sibling task produced the max on a shared sink, folding it is an over-claim on a real
 *       appended offset of this same application — conservative-sound by I8, and the task's own
 *       sends never exceed the max, so its own outputs are always covered (the #22 requirement).
 *       Per-task ack routing therefore dissolves: no send-to-task correlation is ever needed for
 *       the fold.</li>
 *   <li><strong>Pending sends are tracked per producer</strong>, which under EOS v2 means per
 *       StreamThread: every send through a producer happens on its owning stream thread, so the
 *       first {@link PendingTracker#sent} binds that thread to the producer's tracker, and
 *       {@link #awaitQuiescentExcept} resolves "this task's pending sends" from
 *       {@code Thread.currentThread()} alone. Only declared sink topics are tracked — changelog
 *       writes share the producer but their acks are never own-output coordinates, and waiting on
 *       them would add pure latency.</li>
 * </ul>
 *
 * <p>{@link #awaitQuiescentExcept} is the crossing-wait primitive (O1/T3.0 A7): it blocks until no
 * send to any tracked coordinate outside the caller's excluded destination set — a different topic
 * or a different partition of the same topic — is unacknowledged, and it upholds the A8
 * implementation invariant by construction: it returns normally only on quiescence, and
 * <strong>throws</strong> — so the caller's EOS transaction dies rather than stamp — on timeout or
 * on any acknowledgement failure observed while waiting. {@link ParsleyCausalBroadcast#broadcast}
 * runs it (via {@link ParsleyChannels#awaitOwnOutputQuiescence}) before every stamp: business
 * forwards exclude nothing (their destination partition is unknowable at stamp time — over-waiting
 * is monotone-sound, I8), marker forwards exclude their exact destination set.
 */
final class ParsleyOwnOutputRegistry implements ParsleyChannels.AckedOutputs, ParsleyChannels.PendingSends {

    /**
     * The producer config key carrying the registry id — injected by {@link CausalStreams} with the
     * {@code producer.} prefix, read un-prefixed by {@link ParsleyOwnOutputInterceptor#configure}
     * and prefixed (via {@code appConfigs()}) by {@code ParsleyProcessor}. The double-underscore
     * shape marks it internal wiring, not user configuration.
     */
    static final String CONFIG_KEY = "__parsley.own-output.registry__";

    /** Live registries by minted id; registered at {@link CausalStreams} construction, removed at close. */
    private static final ConcurrentHashMap<String, ParsleyOwnOutputRegistry> REGISTRIES = new ConcurrentHashMap<>();

    /** Sink topic names whose sends this registry tracks; everything else is ignored outright. */
    private final Set<String> trackedTopics;
    /** Highest successfully acked offset per sink coordinate, max-merged across all producers. */
    private final ConcurrentHashMap<TopicPartition, Long> acked = new ConcurrentHashMap<>();
    /** Stream thread → the pending tracker of the producer that thread owns (bound at first send). */
    private final ConcurrentHashMap<Thread, PendingTracker> trackerByThread = new ConcurrentHashMap<>();

    ParsleyOwnOutputRegistry(Set<String> trackedTopics) {
        this.trackedTopics = Set.copyOf(trackedTopics);
    }

    /** Registers {@code registry} under {@code id} for {@link #lookup} from producer/task config. */
    static void register(String id, ParsleyOwnOutputRegistry registry) {
        REGISTRIES.put(id, registry);
    }

    /** The registry registered under {@code id}, or {@code null} (e.g. a TopologyTestDriver run). */
    static @Nullable ParsleyOwnOutputRegistry lookup(String id) {
        return REGISTRIES.get(id);
    }

    /** Removes the {@code id} binding; the closing {@link CausalStreams} instance owns this call. */
    static void unregister(String id) {
        REGISTRIES.remove(id);
    }

    /** Whether sends to {@code topic} are tracked (a declared sink topic of this instance). */
    boolean tracks(String topic) {
        return trackedTopics.contains(topic);
    }

    /** A pending tracker for one producer; each interceptor instance creates exactly one. */
    PendingTracker newTracker() {
        return new PendingTracker();
    }

    /**
     * Binds {@code thread} — the stream thread observed inside {@code onSend} — to its producer's
     * tracker. Idempotent per producer lifetime; a replacement thread's new producer re-binds.
     */
    void bindThread(Thread thread, PendingTracker tracker) {
        trackerByThread.put(thread, tracker);
    }

    /** Unbinds every thread mapped to {@code tracker} — the closing producer's interceptor calls this. */
    void unbind(PendingTracker tracker) {
        trackerByThread.values().removeIf(bound -> bound == tracker);
    }

    /** Folds a successful acknowledgement into the global per-coordinate max. */
    void fold(String topic, int partition, long offset) {
        acked.merge(new TopicPartition(topic, partition), offset, Math::max);
    }

    @Override
    public void forEachAcked(ParsleyChannels.AckedOutputs.Consumer consumer) {
        acked.forEach((tp, offset) -> consumer.accept(tp.topic(), tp.partition(), offset));
    }

    /**
     * The crossing-wait primitive: blocks until the calling thread's producer has no
     * unacknowledged send to any tracked coordinate outside {@code exceptDestinations} (a pending
     * send to the record's own destination coordinate needs no wait — partition FIFO plus I3 cover
     * it; O1). An empty set waits for full quiescence. A thread with no bound tracker (it has
     * never sent) returns immediately.
     *
     * <p><strong>Never stamp-and-proceed</strong> (T3.0 A8): this method returns normally only on
     * genuine quiescence. It throws {@link CausalPendingAckException} when {@code timeoutMs}
     * elapses first — the unacked send has effectively failed, so the potentially under-claiming
     * stamp must die with the transaction — and when any acknowledgement failure is observed during
     * the wait, since T2.1 confirmed an aborting transaction fails each pending send with exactly
     * one exception-carrying callback: the wait releases, and the failure outcome makes it die loud.
     */
    @Override
    public void awaitQuiescentExcept(Set<TopicPartition> exceptDestinations, long timeoutMs) {
        PendingTracker tracker = trackerByThread.get(Thread.currentThread());
        if (tracker != null) {
            tracker.awaitQuiescentExcept(exceptDestinations, timeoutMs);
        }
    }

    /**
     * Unacknowledged-send bookkeeping for one producer. All sends through a producer happen on its
     * one owning stream thread, so writers are that thread ({@code sent}) and the producer's
     * network thread ({@code acknowledged}); the monitor provides the happens-before edge and the
     * wake-up for {@link #awaitQuiescentExcept}.
     */
    static final class PendingTracker {

        /** Unacked send count per explicit-partition coordinate. */
        private final Map<TopicPartition, Integer> pending = new HashMap<>();
        /**
         * Unacked sends whose partition was still unassigned at {@code onSend} (no explicit
         * partition on the record — not produced by any Parsley path, which always partitions
         * before send, but tolerated). Keyed by topic; the crossing wait never excludes these,
         * conservatively: an unknown partition might be a different one, and waiting extra is
         * safe where skipping is not.
         */
        private final Map<String, Integer> unresolved = new HashMap<>();
        /** Count of exception-carrying acknowledgements ever observed; waiters throw on any advance. */
        private long failures;

        /** Records a tracked send; {@code partition} is the record's explicit partition, or null. */
        synchronized void sent(String topic, @Nullable Integer partition) {
            if (partition != null) {
                pending.merge(new TopicPartition(topic, partition), 1, Integer::sum);
            } else {
                unresolved.merge(topic, 1, Integer::sum);
            }
        }

        /**
         * Records an acknowledgement (success or failure — T2.1: exactly one callback per send,
         * abort included). Decrements the coordinate's explicit-partition count when one is
         * outstanding, else the topic's unresolved count — with per-partition FIFO
         * acknowledgement order this pairing is exact, and a stray unmatched callback (never
         * observed) simply decrements nothing rather than corrupting a count. A negative
         * {@code partition} (a send that failed before partition assignment) can only match the
         * unresolved side.
         */
        synchronized void acknowledged(String topic, int partition, boolean failed) {
            Integer explicit = partition < 0 ? null : pending.get(new TopicPartition(topic, partition));
            if (explicit != null && explicit > 0) {
                TopicPartition tp = new TopicPartition(topic, partition);
                if (explicit == 1) {
                    pending.remove(tp);
                } else {
                    pending.put(tp, explicit - 1);
                }
            } else {
                unresolved.computeIfPresent(topic, (t, count) -> count == 1 ? null : count - 1);
            }
            if (failed) {
                failures++;
            }
            notifyAll();
        }

        /** See {@link ParsleyOwnOutputRegistry#awaitQuiescentExcept}. */
        synchronized void awaitQuiescentExcept(Set<TopicPartition> exceptDestinations, long timeoutMs) {
            long failuresAtStart = failures;
            long deadlineNanos = System.nanoTime() + timeoutMs * 1_000_000L;
            while (true) {
                if (failures > failuresAtStart) {
                    throw new CausalPendingAckException("a pending own-output send failed while "
                            + "waiting to stamp a send (destinations " + exceptDestinations
                            + "); failing the transaction rather than stamping (T3.0 A8)");
                }
                if (pendingOutside(exceptDestinations) == 0) {
                    return;
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new CausalPendingAckException("own-output acknowledgements still pending "
                            + "on a coordinate outside the stamped send's destinations "
                            + exceptDestinations + " after " + timeoutMs + " ms; failing the "
                            + "transaction rather than stamping with a potentially incomplete "
                            + "own-output clock (T3.0 A8)");
                }
                try {
                    wait(remainingNanos / 1_000_000L + 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CausalPendingAckException(
                            "interrupted while waiting for pending own-output acknowledgements", e);
                }
            }
        }

        private int pendingOutside(Set<TopicPartition> exceptDestinations) {
            int count = 0;
            for (Map.Entry<TopicPartition, Integer> entry : pending.entrySet()) {
                if (!exceptDestinations.contains(entry.getKey())) {
                    count += entry.getValue();
                }
            }
            for (int unresolvedCount : unresolved.values()) {
                count += unresolvedCount;
            }
            return count;
        }
    }
}
