package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

/**
 * The topology epoch's lower bounds — the proven lower edge of the causal domain, per
 * {@code (topicId, partition)} coordinate. A dependency below a coordinate's bound references a prior,
 * closed epoch and is stripped before the delivery gate's completeness check (see
 * {@link ParsleyClock#strippedBelow} and {@code ParsleyEngine.isDeliverable}).
 *
 * <p>This is currently just the {@link View} seam the gate consults, plus {@link #NO_BOUND}. Nothing
 * populates a non-{@link View#NONE NONE} view yet: epoch bounds are established by the Topology
 * Co-ordinator via an in-band epoch-boundary marker (parked design — see
 * {@code ~/.claude/plans/resume-the-topology-epochs-reactive-owl.md}); until that lands, every gate
 * runs with {@link View#NONE} (epoch 0), so the strip is a no-op and behaviour is unchanged.
 */
final class ParsleyEpoch {

    /**
     * The {@code startsAt} of a coordinate with no recorded bound: the minimum possible value, so
     * {@code offset < NO_BOUND} is false for every offset and nothing is ever stripped. Gating such a
     * coordinate over its full history is the correct, conservative default (epoch 0).
     */
    static final long NO_BOUND = Long.MIN_VALUE;

    private ParsleyEpoch() {}

    /**
     * A view of the {@code startsAt} bounds the delivery gate consults. Returns {@link #NO_BOUND} for
     * any coordinate with no recorded bound, so a dependency on it is never stripped (gated normally
     * over its full history).
     */
    @FunctionalInterface
    interface View {
        long startsAt(Uuid topicId, int partition);

        /** The no-op view used when epoch bounding is disabled (epoch 0): every coordinate is unbounded. */
        View NONE = (topicId, partition) -> NO_BOUND;
    }
}
