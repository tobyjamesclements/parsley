package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.CausalSendException;
import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.SendTracker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Simulator {@link SendTracker}: the broker assigns offsets at append, but the acknowledgement
 * only reaches the node when the scheduler delivers it (or when the crossing wait forces it —
 * modeling the producer flush a real await performs).
 */
final class SimSendTracker implements SendTracker {

    private record Pending(Channel channel, long offset) {}

    private final SimBroker broker;
    /** Acks in flight: appended, not yet seen by the node. */
    private final Deque<Pending> inFlight = new ArrayDeque<>();
    /** Acks seen by the node, not yet drained by the core. */
    private final List<Ack> arrived = new ArrayList<>();
    private boolean failNextAwait;

    SimSendTracker(SimBroker broker) {
        this.broker = broker;
    }

    /** Called by the sim node when it appends an own send. */
    void sent(Channel c, long offset) {
        inFlight.add(new Pending(c, offset));
    }

    /** Scheduler action: one in-flight ack reaches the node. */
    boolean deliverOneAck() {
        Pending p = inFlight.poll();
        if (p == null) return false;
        arrived.add(new Ack(p.channel, p.offset));
        return true;
    }

    boolean hasInFlight() {
        return !inFlight.isEmpty();
    }

    /** Arms a send-failure: the next crossing wait throws, as a failed produce would. */
    void failNextAwait() {
        failNextAwait = true;
    }

    @Override
    public List<Ack> drainAcks() {
        List<Ack> out = List.copyOf(arrived);
        arrived.clear();
        return out;
    }

    @Override
    public void awaitQuiescence(Set<Channel> except) {
        if (failNextAwait) {
            failNextAwait = false;
            throw new CausalSendException("simulated send failure observed during crossing wait");
        }
        // The real implementation blocks on producer futures; the simulator resolves them.
        inFlight.removeIf(p -> {
            if (except.contains(p.channel)) return false;
            arrived.add(new Ack(p.channel, p.offset));
            return true;
        });
    }

    @Override
    public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
        Map<Channel, Long> out = new HashMap<>();
        for (Channel c : broker.allChannels()) {
            if (sinkTopics.contains(c.topicId())) out.put(c, broker.endOffset(c));
        }
        return out;
    }

    /** Crash: in-flight and arrived acks are lost with the process. */
    void reset() {
        inFlight.clear();
        arrived.clear();
        failNextAwait = false;
    }
}
