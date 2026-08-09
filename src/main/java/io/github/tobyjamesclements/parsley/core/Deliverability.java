package io.github.tobyjamesclements.parsley.core;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * The delivery decision, as a pure function.
 *
 * <p>{@link #decide} is the whole of the protocol's safety rule. It reads no state of its
 * own, consults no clock and performs no side effect, so a verdict depends only on its
 * arguments and can be tested exhaustively.
 *
 * @see ProcessEngine
 */
public final class Deliverability {
    private Deliverability() {
    }

    /** Per-channel view of how far delivery has settled. */
    @FunctionalInterface
    public interface SettledView {
        /**
         * Reports how far one channel has settled.
         *
         * @param channel the channel to report on
         * @return the highest settled position, or empty when nothing has settled
         */
        OptionalLong settled(ChannelId channel);
    }

    /** The outcome of one decision. */
    public sealed interface Verdict permits Deliverable, Held {
        /**
         * Returns {@code true} when the message may be delivered now.
         *
         * @return {@code true} when the message may be delivered now
         */
        boolean isDeliverable();
    }

    /** Every cause is satisfied. */
    public static final class Deliverable implements Verdict {
        static final Deliverable INSTANCE = new Deliverable();

        private Deliverable() {
        }

        /**
         * Returns {@code true}.
         *
         * @return {@code true}
         */
        @Override
        public boolean isDeliverable() {
            return true;
        }
    }

    /**
     * At least one cause is outstanding.
     *
     * @param blockers every unsatisfied cause, which is the diagnosis for a held message
     */
    public record Held(List<Blocker> blockers) implements Verdict {
        /**
         * Returns {@code false}.
         *
         * @return {@code false}
         */
        @Override
        public boolean isDeliverable() {
            return false;
        }
    }

    /**
     * One unsatisfied cause.
     *
     * @param channel          the channel the cause lies on
     * @param requiredPosition the position that must settle before delivery
     * @param settledPosition  how far that channel has settled, or empty
     */
    public record Blocker(ChannelId channel, long requiredPosition, OptionalLong settledPosition) {
    }

    /**
     * Decides whether a message expressing {@code causes} may be delivered.
     *
     * <p>A cause on a channel this process does not receive is ignored: the process will
     * never deliver it, so it cannot invert an order this process can observe. A cause on a
     * received channel blocks until that channel has settled to at least the named position.
     *
     * @param causes           the frontier the message expressed
     * @param receivedChannels the channels this process receives
     * @param settled          how far each channel has settled
     * @return {@link Deliverable} when every relevant cause is satisfied, otherwise
     *         {@link Held} naming each outstanding one
     */
    public static Verdict decide(Causes causes, Set<ChannelId> receivedChannels, SettledView settled) {
        List<Blocker> blockers = null;
        for (var entry : causes.byChannel().entrySet()) {
            ChannelId channel = entry.getKey();
            long required = entry.getValue();
            if (!receivedChannels.contains(channel)) {
                continue;
            }
            OptionalLong settledPosition = settled.settled(channel);
            if (settledPosition.isEmpty() || settledPosition.getAsLong() < required) {
                if (blockers == null) {
                    blockers = new ArrayList<>();
                }
                blockers.add(new Blocker(channel, required, settledPosition));
            }
        }
        return blockers == null ? Deliverable.INSTANCE : new Held(List.copyOf(blockers));
    }
}
