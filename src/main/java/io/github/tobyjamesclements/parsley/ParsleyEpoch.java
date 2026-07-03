package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * The topology epoch's lower bounds — the proven lower edge of the causal domain, per
 * {@code (topicId, partition)} coordinate. {@link #startsAt} returns the lowest offset that still
 * participates in the current epoch for a coordinate; anything below it references a prior, closed
 * epoch. This one bound drives the whole floored-clock invariant: a dependency below it is stripped
 * before the delivery gate's completeness check ({@link ParsleyClock#strippedBelow}), and the node's
 * own causal state is floored to it ({@link ParsleyFrontier} — the frontier does not anchor below the
 * bound, and {@code completeness()} / the channel clocks / the outbound stamp are all floored).
 *
 * <p>Returns {@link #NO_BOUND} for any coordinate with no recorded bound, so a dependency on it is
 * never stripped and it is gated normally over its full history (epoch 0, the conservative default).
 *
 * <p>Nothing populates a non-{@link #NONE NONE} epoch yet: bounds are established by the Topology
 * Co-ordinator via an in-band epoch-boundary marker (parked design — see
 * {@code ~/.claude/plans/resume-the-topology-epochs-reactive-owl.md}); until that lands, every gate
 * runs with {@link #NONE} (epoch 0), so the whole invariant is a no-op and behaviour is unchanged.
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
