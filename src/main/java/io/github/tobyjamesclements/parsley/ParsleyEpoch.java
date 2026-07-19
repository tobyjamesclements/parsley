package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * The topology epoch's lower bounds — the proven lower edge of the causal domain, per
 * {@code (topicId, partition)} coordinate. {@link #startsAt} returns the lowest offset that still
 * participates in the current epoch for a coordinate; anything below it references a prior, closed
 * epoch. This one bound drives the whole floored-clock invariant: a dependency below it is stripped
 * before the delivery gate ever sees the clock ({@link ParsleyChannels#normalize}), and the node's
 * own causal state is floored to it ({@link ParsleyChannels} — the frontier does not anchor below the
 * bound, and {@code completeness()} / the channel clocks / the outbound stamp are all floored).
 *
 * <p>Returns {@link #NO_BOUND} for any coordinate with no recorded bound, so a dependency on it is
 * never stripped and it is gated normally over its full history (epoch 0, the conservative default).
 *
 * <p>{@link #NONE} (epoch 0) is the default whenever topology-epoch coordination is not configured
 * ({@code parsley.coordination.epoch-events-topic} unset). When it is configured, {@link
 * ParsleyEpochState} is the live implementation: it populates real per-coordinate bounds from
 * committed floors relayed in-band by {@code ParsleyEpochBoundary} markers, driven by the leaderless
 * epoch protocol ({@link ParsleyEpochLog} / {@link ParsleyEpochRuntime}) and consumed by
 * {@link ParsleyProcessor}'s epoch-marker handlers.
 */
@FunctionalInterface
interface ParsleyEpoch {

    /**
     * The {@code startsAt} of a coordinate with no recorded bound: the minimum possible value, so
     * {@code offset < NO_BOUND} is false for every offset and nothing is ever stripped or floored.
     * Gating such a coordinate over its full history is the correct, conservative default (epoch 0).
     */
    long NO_BOUND = Long.MIN_VALUE;

    /**
     * The lowest offset of {@code (topicId, partition)} that still participates in the current epoch —
     * the first in-domain offset. {@link #NO_BOUND} for a coordinate with no recorded bound.
     */
    long startsAt(Uuid topicId, int partition);

    /** The no-op epoch used when bounding is disabled (epoch 0): every coordinate is unbounded. */
    ParsleyEpoch NONE = (topicId, partition) -> NO_BOUND;
}
