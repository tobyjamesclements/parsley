package io.github.tobyjamesclements.parsley;


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
 *   <li>Transactions are step-atomic. A simulation step appends a transaction's records and its
 *       per-partition commit or abort marker together. No transaction spans steps, so consumers
 *       never observe an open transaction and last-stable-offset tracking is unnecessary. What
 *       consumers do observe is modeled faithfully, meaning real offsets occupied by markers and
 *       aborted records that a fetch silently skips, because that is what the seed and bridge
 *       density adaptation must survive.</li>
 *   <li>Offsets are assigned at append, exactly like the broker. Nothing carries them back to
 *       the sender. Neither the protocol nor the Streams adapter folds a producer
 *       acknowledgement, so a node's own sends stay claimed in sequence space until a
 *       restart's end-offset seed converts them to offsets.</li>
 * </ul>
 */
final class SimBroker {

    enum Kind { BUSINESS, MARKER, ABORTED }

    /**
     * One slot in a partition log.
     *
     * <p>{@code recordId} is the oracle's id, or -1 for non-business entries. {@code senderId}
     * and {@code senderSeq} are the sender tag, null and -1 for untagged producers and
     * non-business entries.
     */
    record Entry(Kind kind, long recordId, VectorClock clock, java.util.UUID senderId, long senderSeq,
                 byte[] key, byte[] value, long timestamp) {
        boolean fetchable() {
            return kind == Kind.BUSINESS;
        }
    }

    private final Map<String, UUID> topicIds = new HashMap<>();
    private final Map<String, Integer> partitionCounts = new HashMap<>();
    private final Map<Channel, List<Entry>> logs = new HashMap<>();
    /** First non-deleted offset per channel, which is retention's high-water mark. */
    private final Map<Channel, Long> logStarts = new HashMap<>();

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
            append(c, new Entry(Kind.MARKER, -1, null, null, -1, null, null, -1));
        }
    }

    Entry entry(Channel c, long offset) {
        return log(c).get((int) offset);
    }

    /** Converts an appended entry to an aborted record (its offset stays occupied). */
    void markAborted(Channel c, long offset) {
        List<Entry> log = log(c);
        Entry e = log.get((int) offset);
        log.set((int) offset, new Entry(Kind.ABORTED, -1, null, null, -1, null, null, -1));
        if (e.kind() == Kind.MARKER) throw new IllegalStateException("aborting a marker");
    }

    /** Next offset to be assigned, the end offset the init-time own-outputs seed folds. */
    long endOffset(Channel c) {
        return log(c).size();
    }

    /**
     * The offset of the next fetchable entry at or above {@code from}, or -1 when none exists
     * yet. A {@code read_committed} consumer never returns markers or aborted records, and
     * retention-deleted offsets (below the log start) are gone for every consumer.
     */
    long nextFetchable(Channel c, long from) {
        List<Entry> log = log(c);
        for (long o = Math.max(Math.max(from, 0), logStart(c)); o < log.size(); o++) {
            if (log.get((int) o).fetchable()) return o;
        }
        return -1;
    }

    long logStart(Channel c) {
        return logStarts.getOrDefault(c, 0L);
    }

    /** Retention deletes everything below {@code offset}. Never regresses. */
    void advanceLogStart(Channel c, long offset) {
        if (offset > endOffset(c)) throw new IllegalArgumentException("log start past end");
        logStarts.merge(c, offset, Math::max);
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
