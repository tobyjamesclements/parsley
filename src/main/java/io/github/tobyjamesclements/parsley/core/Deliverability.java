package io.github.tobyjamesclements.parsley.core;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * The deliverability decision (SPEC Structural 7): a pure function of the message's expressed causes, the process's
 * received-channel set, and the settled frontier the process's ordering state defines. It reads no clock, performs no
 * I/O, and depends on nothing outside its arguments. {@link ProcessEngine} invokes this same unit for every delivery
 * it performs; it is public and callable without a host.
 *
 * <p>A dependency (c, p) is satisfied when either c is not in the received-channel set — ordering with messages this
 * process will never deliver is vacuous (SPEC Liveness 4), which also covers dead incarnations of recreated topics —
 * or the settled frontier of c is defined and at least p, meaning every position on c up to p has either been
 * delivered here or will never yield a message this process receives.</p>
 *
 * <p>This decides causal deliverability only. The separate requirement that a channel's messages are delivered in
 * channel order behind an undeliverable message (SPEC Safety 3) is enforced structurally by the engine, which offers
 * only the head of each channel's hold-back buffer to this decision.</p>
 */
public final class Deliverability {

    private Deliverability() {
    }

    /** The settled frontier: for a channel, the highest position at or below which every position is delivered at
     * this process or will never yield a message it receives; empty when nothing is yet known of the channel. */
    @FunctionalInterface
    public interface SettledView {
        OptionalLong settled(ChannelId channel);
    }

    public sealed interface Verdict permits Deliverable, Held {
        boolean isDeliverable();
    }

    public static final class Deliverable implements Verdict {
        static final Deliverable INSTANCE = new Deliverable();

        private Deliverable() {
        }

        @Override
        public boolean isDeliverable() {
            return true;
        }
    }

    /** The dependencies that block delivery, for observability and tests. */
    public record Held(List<Blocker> blockers) implements Verdict {
        @Override
        public boolean isDeliverable() {
            return false;
        }
    }

    public record Blocker(ChannelId channel, long requiredPosition, OptionalLong settledPosition) {
    }

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
