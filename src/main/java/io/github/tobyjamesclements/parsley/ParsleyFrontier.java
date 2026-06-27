package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.HashSet;
import java.util.Set;

/**
 * The causal state of a {@link ParsleyEngine}: the contiguous frontier clock and the
 * infrastructure required to maintain it.
 *
 * <p>Implements the three Lamport causal-delivery operations:
 *
 * <ol>
 *   <li><strong>Delivery predicate</strong> ({@link #isDeliverable}): a record can be delivered
 *       when the frontier dominates its effective dependencies — every coordinate the record
 *       depends on has already been delivered here.
 *   <li><strong>Delivery</strong> ({@link #deliver}): advance the frontier to account for the
 *       record just delivered. The frontier is a contiguous watermark: it only advances past a
 *       coordinate once every offset up to it has been forwarded, never past a gap.
 *   <li><strong>Baseline seeding</strong> ({@link #seedIfFirstSeen}): the first offset seen on a
 *       coordinate need not be 0 (finite retention, fresh consumer group). Anything below it is
 *       outside the engine's purview, not a real gap; folding it into the frontier means the
 *       contiguous walk can start from there rather than stalling at {@code -1}.
 * </ol>
 *
 * <p>{@link ParsleyEngine} enforces causal transitivity (the cascade after each delivery) and
 * manages all buffer infrastructure around these three operations.
 */
final class ParsleyFrontier {

    private ParsleyClock frontier;
    private final ParsleyForwardedIndex forwardedIndex;
    // Coordinates observed at least once; guards the one-time baseline seed in seedIfFirstSeen.
    private final Set<CoordKey> seenCoordinates = new HashSet<>();

    ParsleyFrontier(ParsleyClock initial, ParsleyForwardedIndex forwardedIndex) {
        this.frontier = initial;
        this.forwardedIndex = forwardedIndex;
    }

    /**
     * The Lamport delivery predicate: returns {@code true} when the frontier dominates
     * {@code effectiveDeps} — every coordinate the record depends on has already been delivered.
     */
    boolean isDeliverable(ParsleyClock effectiveDeps) {
        return frontier.dominates(effectiveDeps);
    }

    /**
     * Records that the record at {@code (topicId, partition, offset)} was delivered. Marks the
     * offset forwarded in the {@link ParsleyForwardedIndex}, walks the longest contiguous run now
     * achievable, advances the frontier, and notifies {@code callback} with the new frontier so
     * {@link ParsleyProcessor} can snapshot it for stamping. The callback fires before control
     * returns to the caller, which adds the record to its out-bound list — preserving the 1:1
     * snapshot/admitted zip.
     *
     * <p>Use {@link #deliverSilently} for poison-drops and baseline seeds that have no
     * corresponding out-bound record.
     */
    void deliver(Uuid topicId, int partition, long offset, ParsleyEngine.FrontierCallback callback) {
        frontier = frontier.observe(topicId, partition, mergeForward(topicId, partition, offset));
        callback.frontierAdvanced(frontier);
    }

    /**
     * As {@link #deliver}, without notifying the callback. Used for poison-drops and baseline
     * seeds that have no corresponding out-bound record; the 1:1 snapshot/admitted zip in
     * {@link ParsleyProcessor} must not be broken by a notification with no matching record.
     * Any records subsequently released via {@link ParsleyEngine}'s cascade still go through
     * {@link #deliver} and notify normally.
     */
    void deliverSilently(Uuid topicId, int partition, long offset) {
        frontier = frontier.observe(topicId, partition, mergeForward(topicId, partition, offset));
    }

    /**
     * Establishes the contiguous frontier's starting point the first time this coordinate is
     * observed. Kafka delivers records in strictly increasing offset order, so the first offset
     * seen for a coordinate is wherever consumption began — which need not be 0 (finite
     * retention, fresh consumer group, etc.). Anything below it is outside the engine's purview,
     * not an unfillable gap; folding it into the frontier allows the contiguous walk to start from
     * there.
     *
     * <p>Returns {@code true} if a seed was applied, meaning the frontier advanced and the caller
     * should call {@link ParsleyEngine}'s cascade to release any records waiting on this
     * coordinate. Returns {@code false} on every subsequent call for the same coordinate, and when
     * the persisted frontier already reflects it (the engine's prior progress is authoritative).
     *
     * <p>The coordinate is marked seen unconditionally on the very first call — even if the record
     * is held (not immediately forwarded) — so a later record on the same partition cannot
     * wrongly re-trigger the seed and treat the still-held earlier record as outside the engine's
     * purview.
     */
    boolean seedIfFirstSeen(Uuid topicId, int partition, long offset) {
        if (!seenCoordinates.add(new CoordKey(topicId, partition))) return false;
        if (offset <= 0) return false;
        if (frontier.offsetFor(topicId, partition) >= 0) return false;
        frontier = frontier.observe(topicId, partition, offset - 1);
        return true;
    }

    /** The current frontier clock. */
    ParsleyClock snapshot() {
        return frontier;
    }

    /**
     * Marks {@code offset} forwarded and returns the longest contiguous run now achievable on
     * {@code (topicId, partition)} — {@code offset} itself if nothing above it is pending, or
     * further if this offset closed a gap. Prunes absorbed entries from the forwarded index.
     */
    private long mergeForward(Uuid topicId, int partition, long offset) {
        forwardedIndex.mark(topicId, partition, offset);
        long watermark = frontier.offsetFor(topicId, partition);
        long extended = watermark;
        for (long candidate : forwardedIndex.forwardedAfter(topicId, partition, watermark)) {
            if (candidate != extended + 1) break;
            forwardedIndex.unmark(topicId, partition, candidate);
            extended = candidate;
        }
        return extended;
    }

    private record CoordKey(Uuid topicId, int partition) {}
}
