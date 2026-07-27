package io.github.tobyjamesclements.parsley.sim;

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
 * only reaches the node when the scheduler delivers it. Per-channel acknowledgement order
 * matches send order (Kafka's per-partition guarantee), which the core's own-claim
 * normalisation relies on.
 *
 * <p>{@code dropAcks} mode discards every acknowledgement: the node's own-output claims can
 * then never be upgraded to offset space, so cross-partition ordering of its outputs rests
 * entirely on sequence claims — the decisive configuration for the sender-sequence
 * experiment.
 */
final class SimSendTracker implements SendTracker {

    private record Pending(Channel channel, long offset) {}

    private final SimBroker broker;
    private final boolean dropAcks;
    /** Acks in flight: appended, not yet seen by the node. */
    private final Deque<Pending> inFlight = new ArrayDeque<>();
    /** Acks seen by the node, not yet drained by the core. */
    private final List<Ack> arrived = new ArrayList<>();

    SimSendTracker(SimBroker broker, boolean dropAcks) {
        this.broker = broker;
        this.dropAcks = dropAcks;
    }

    /** Called by the sim node when it appends an own send. */
    void sent(Channel c, long offset) {
        if (!dropAcks) inFlight.add(new Pending(c, offset));
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

    @Override
    public List<Ack> drainAcks() {
        List<Ack> out = List.copyOf(arrived);
        arrived.clear();
        return out;
    }

    @Override
    public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
        Map<Channel, Long> out = new HashMap<>();
        for (Channel c : broker.allChannels()) {
            if (sinkTopics.contains(c.topicId())) out.put(c, broker.endOffset(c));
        }
        return out;
    }

    @Override
    public EarliestOffsets earliestOffsets(Set<Channel> channels) {
        Map<Channel, Long> logStarts = new HashMap<>();
        Set<Channel> absent = new java.util.HashSet<>();
        for (Channel c : channels) {
            if (broker.allChannels().contains(c)) {
                logStarts.put(c, broker.logStart(c));
            } else {
                absent.add(c); // the sim broker knows its topics definitively
            }
        }
        return new EarliestOffsets(logStarts, absent);
    }

    /** Crash: in-flight and arrived acks are lost with the process. */
    void reset() {
        inFlight.clear();
        arrived.clear();
    }
}
