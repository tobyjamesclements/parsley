package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Deterministic in-memory model of the broker side of Kafka, as seen through EOS producers and
 * a {@code read_committed} consumer.
 *
 * <p>Modeling assumptions, stated once:
 * <ul>
 *   <li>Transactions are step-atomic: a simulation step appends a transaction's records and its
 *       per-partition commit or abort marker together. No transaction spans steps, so consumers
 *       never observe an open transaction and last-stable-offset tracking is unnecessary. What
 *       consumers do observe — real offsets occupied by markers and aborted records that a fetch
 *       silently skips — is modeled faithfully, because that is what the density adaptation
 *       (seed/bridge) must survive.</li>
 *   <li>Offsets are assigned at append, exactly like the broker; acknowledgements carrying them
 *       reach producers asynchronously via the scheduler.</li>
 * </ul>
 */
final class SimBroker {

    enum Kind { BUSINESS, NULL_MESSAGE, MARKER, ABORTED }

    /** One slot in a partition log. {@code recordId} is the oracle's id, -1 for non-business. */
    record Entry(Kind kind, long recordId, Clock clock, byte[] key, byte[] value, long timestamp) {
        boolean fetchable() {
            return kind == Kind.BUSINESS || kind == Kind.NULL_MESSAGE;
        }
    }

    private final Map<String, UUID> topicIds = new HashMap<>();
    private final Map<String, Integer> partitionCounts = new HashMap<>();
    private final Map<Channel, List<Entry>> logs = new HashMap<>();

    void createTopic(String name, int partitions) {
        if (topicIds.containsKey(name)) throw new IllegalStateException("topic exists: " + name);
        UUID id = UUID.nameUUIDFromBytes((name + "#" + partitions).getBytes());
        topicIds.put(name, id);
        partitionCounts.put(name, partitions);
        for (int p = 0; p < partitions; p++) {
            logs.put(new Channel(id, p), new ArrayList<>());
        }
    }

    UUID topicId(String name) {
        UUID id = topicIds.get(name);
        if (id == null) throw new IllegalArgumentException("no such topic: " + name);
        return id;
    }

    int partitions(String name) {
        return partitionCounts.get(name);
    }

    Channel channel(String topic, int partition) {
        return new Channel(topicId(topic), partition);
    }

    /** Appends an entry, returning its offset. */
    long append(Channel c, Entry e) {
        List<Entry> log = log(c);
        log.add(e);
        return log.size() - 1;
    }

    /** Appends one commit/abort marker to each of the given partitions. */
    void appendMarkers(Set<Channel> touched) {
        for (Channel c : touched) {
            append(c, new Entry(Kind.MARKER, -1, null, null, null, -1));
        }
    }

    Entry entry(Channel c, long offset) {
        return log(c).get((int) offset);
    }

    /** Converts an appended entry to an aborted record (its offset stays occupied). */
    void markAborted(Channel c, long offset) {
        List<Entry> log = log(c);
        Entry e = log.get((int) offset);
        log.set((int) offset, new Entry(Kind.ABORTED, -1, null, null, null, -1));
        if (e.kind() == Kind.MARKER) throw new IllegalStateException("aborting a marker");
    }

    /** Next offset to be assigned — the end offset the init-time own-outputs seed folds. */
    long endOffset(Channel c) {
        return log(c).size();
    }

    /**
     * The offset of the next fetchable entry at or above {@code from}, or -1 when none exists
     * yet. A {@code read_committed} consumer never returns markers or aborted records.
     */
    long nextFetchable(Channel c, long from) {
        List<Entry> log = log(c);
        for (long o = Math.max(from, 0); o < log.size(); o++) {
            if (log.get((int) o).fetchable()) return o;
        }
        return -1;
    }

    Set<Channel> allChannels() {
        return logs.keySet();
    }

    private List<Entry> log(Channel c) {
        List<Entry> log = logs.get(c);
        if (log == null) throw new IllegalArgumentException("no such channel: " + c);
        return log;
    }
}
