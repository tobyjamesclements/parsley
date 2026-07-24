package io.github.tobyjamesclements.parsley;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

/**
 * The test fixture factory for the two protocol classes whose convenience constructors used to
 * live in main: {@link ParsleyChannels} instances over an in-memory store double, and
 * {@link ParsleyCausalBroadcast} instances with the predicate/destroyed-set parameters defaulted.
 * Production code always uses the full constructors directly (the task's changelog-backed store,
 * every parameter explicit); these shapes exist only for tests, so they live here.
 */
final class ParsleyTestFixtures {

    private ParsleyTestFixtures() {
    }

    /**
     * A {@link ParsleyChannels} over a fresh in-memory {@link TestKeyValueStore}, seeded with
     * {@code initialFrontier} (written as a minimal {@code "f"} blob — frontier only, no channel
     * clocks or trailing sections — which the store constructor restores).
     */
    static ParsleyChannels channels(ParsleyVectorClock initialFrontier, ParsleyForwardedIndex forwardedIndex) {
        TestKeyValueStore<String, byte[]> store =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        if (!initialFrontier.equals(ParsleyVectorClock.empty())) {
            store.put(ParsleyStores.FRONTIER_KEY, frontierBlob(initialFrontier));
        }
        return new ParsleyChannels(store, forwardedIndex);
    }

    /**
     * A {@link ParsleyCausalBroadcast} over in-memory {@link #channels}: every coordinate
     * consumed, nothing treated as an own sink, no destroyed sources, wall-clock time.
     */
    static <K, V> ParsleyCausalBroadcast<K, V> broadcast(ParsleyVectorClock initialFrontier,
            ParsleyBufferStore<K, V> buffer, ParsleyCandidateIndex candidateIndex,
            ParsleyForwardedIndex forwardedIndex, ParsleyMetrics metrics) {
        return broadcast(initialFrontier, buffer, candidateIndex, forwardedIndex, metrics,
                System::currentTimeMillis);
    }

    /** As {@link #broadcast(ParsleyVectorClock, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyForwardedIndex, ParsleyMetrics)}, with an explicit clock. */
    static <K, V> ParsleyCausalBroadcast<K, V> broadcast(ParsleyVectorClock initialFrontier,
            ParsleyBufferStore<K, V> buffer, ParsleyCandidateIndex candidateIndex,
            ParsleyForwardedIndex forwardedIndex, ParsleyMetrics metrics, LongSupplier clock) {
        return broadcast(channels(initialFrontier, forwardedIndex), buffer, candidateIndex, metrics, clock);
    }

    /**
     * A {@link ParsleyCausalBroadcast} over the given {@link ParsleyChannels}: every coordinate
     * consumed and nothing treated as an own sink — for tests that do not need to exercise the
     * gate's ignore branch or the reflected-claim diagnostic.
     */
    static <K, V> ParsleyCausalBroadcast<K, V> broadcast(ParsleyChannels channels,
            ParsleyBufferStore<K, V> buffer, ParsleyCandidateIndex candidateIndex,
            ParsleyMetrics metrics, LongSupplier clock) {
        return broadcast(channels, buffer, candidateIndex, metrics, clock, (topicId, partition) -> true);
    }

    /**
     * As {@link #broadcast(ParsleyChannels, ParsleyBufferStore, ParsleyCandidateIndex,
     * ParsleyMetrics, LongSupplier)} with an explicit consumed predicate, nothing treated as an
     * own sink — for tests that do not need the reflected-claim diagnostic.
     */
    static <K, V> ParsleyCausalBroadcast<K, V> broadcast(ParsleyChannels channels,
            ParsleyBufferStore<K, V> buffer, ParsleyCandidateIndex candidateIndex,
            ParsleyMetrics metrics, LongSupplier clock, ParsleyVectorClock.CoordinatePredicate consumed) {
        return broadcast(channels, buffer, candidateIndex, metrics, clock, consumed,
                (topicId, partition) -> false);
    }

    /**
     * As the full {@link ParsleyCausalBroadcast} constructor with no destroyed source incarnations
     * — for tests whose restored buffer predates no input recreation.
     */
    static <K, V> ParsleyCausalBroadcast<K, V> broadcast(ParsleyChannels channels,
            ParsleyBufferStore<K, V> buffer, ParsleyCandidateIndex candidateIndex,
            ParsleyMetrics metrics, LongSupplier clock, ParsleyVectorClock.CoordinatePredicate consumed,
            ParsleyVectorClock.CoordinatePredicate ownSinkTopics) {
        return new ParsleyCausalBroadcast<>(channels, buffer, candidateIndex, metrics, clock,
                consumed, ownSinkTopics, Set.of());
    }

    // The minimal combined "f" blob: [frontier-len:4][frontier bytes][channel-count:4 = 0].
    // Mirrors ParsleyChannels#toBytes' leading sections; the trailing sections are optional on load.
    private static byte[] frontierBlob(ParsleyVectorClock frontier) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            byte[] f = frontier.toBytes();
            dos.writeInt(f.length);
            dos.write(f);
            dos.writeInt(0);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The throwable's message, asserted non-null, for assertions that inspect it. */
    static String message(Throwable t) {
        return requireNonNull(t.getMessage(), "expected a non-null exception message");
    }

    /** The throwable's cause, asserted non-null, for assertions that inspect it. */
    static Throwable cause(Throwable t) {
        return requireNonNull(t.getCause(), "expected a non-null cause");
    }
}
