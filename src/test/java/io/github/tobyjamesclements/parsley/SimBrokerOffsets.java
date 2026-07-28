package io.github.tobyjamesclements.parsley;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Simulator {@link BrokerOffsets}: both queries answered by the in-memory broker. The sim
 * broker knows its topics definitively, so absence reported here is genuine absence.
 */
final class SimBrokerOffsets implements BrokerOffsets {

    private final SimBroker broker;

    SimBrokerOffsets(SimBroker broker) {
        this.broker = broker;
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
        Set<Channel> absent = new HashSet<>();
        for (Channel c : channels) {
            if (broker.allChannels().contains(c)) {
                logStarts.put(c, broker.logStart(c));
            } else {
                absent.add(c);
            }
        }
        return new EarliestOffsets(logStarts, absent);
    }
}
