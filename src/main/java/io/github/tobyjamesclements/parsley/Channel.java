package io.github.tobyjamesclements.parsley;

import java.util.UUID;

/**
 * A causal channel, one partition of one topic.
 *
 * <p>Identified by the topic's stable UUID rather than its name. A topic deleted and recreated
 * under the same name is a different channel, so a coordinate can never silently rebind to a
 * different record history.
 *
 * @param topicId the topic's stable identity, as the broker assigns it
 * @param partition the partition within that topic, not negative
 */
record Channel(UUID topicId, int partition) implements Comparable<Channel> {

    /**
     * Rejects a null topic identity or a negative partition.
     *
     * @throws NullPointerException if {@code topicId} is null
     * @throws IllegalArgumentException if {@code partition} is negative
     */
    Channel {
        if (topicId == null) throw new NullPointerException("topicId");
        if (partition < 0) throw new IllegalArgumentException("partition " + partition);
    }

    @Override
    public int compareTo(Channel o) {
        int c = topicId.compareTo(o.topicId);
        return c != 0 ? c : Integer.compare(partition, o.partition);
    }

    /**
     * Returns the stable text form used in state-store keys, {@code <uuid>:<partition>}.
     *
     * @return the text form, which {@link #fromKey} reads back
     */
    String key() {
        return topicId + ":" + partition;
    }

    /**
     * Reads back a channel from {@link #key}.
     *
     * @param key the text form, {@code <uuid>:<partition>}
     * @return the channel it denotes
     * @throws IllegalArgumentException if the uuid or the partition is malformed
     */
    static Channel fromKey(String key) {
        int i = key.lastIndexOf(':');
        return new Channel(UUID.fromString(key.substring(0, i)), Integer.parseInt(key.substring(i + 1)));
    }

    @Override
    public String toString() {
        // Short prefix keeps oracle failure messages readable.
        return topicId.toString().substring(0, 8) + ":" + partition;
    }
}
