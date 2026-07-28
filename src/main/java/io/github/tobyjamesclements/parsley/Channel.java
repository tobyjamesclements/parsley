package io.github.tobyjamesclements.parsley;

import java.util.UUID;

/**
 * A causal channel: one partition of one topic, identified by the topic's stable UUID rather
 * than its name. A topic deleted and recreated under the same name is a different channel, so a
 * coordinate can never silently rebind to a different record history.
 */
public record Channel(UUID topicId, int partition) implements Comparable<Channel> {

    public Channel {
        if (topicId == null) throw new NullPointerException("topicId");
        if (partition < 0) throw new IllegalArgumentException("partition " + partition);
    }

    @Override
    public int compareTo(Channel o) {
        int c = topicId.compareTo(o.topicId);
        return c != 0 ? c : Integer.compare(partition, o.partition);
    }

    /** Stable text form used in state-store keys: {@code <uuid>:<partition>}. */
    public String key() {
        return topicId + ":" + partition;
    }

    public static Channel fromKey(String key) {
        int i = key.lastIndexOf(':');
        return new Channel(UUID.fromString(key.substring(0, i)), Integer.parseInt(key.substring(i + 1)));
    }

    @Override
    public String toString() {
        // Short prefix keeps oracle failure messages readable.
        return topicId.toString().substring(0, 8) + ":" + partition;
    }
}
