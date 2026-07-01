package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The causal state of a {@link ParsleyEngine}: the contiguous frontier clock, the per-input-channel
 * clocks, and the seeding/forwarding infrastructure that maintains the frontier — the single owner
 * of all causal metadata a node persists (the held-record buffer and its candidate index are a
 * separate concern).
 *
 * <p>Two structures fold into one durable value here, stored as a single {@code "f"} key-value pair
 * in the frontier state store (loaded once at construction, rewritten on change, read from memory):
 * <ul>
 *   <li>the <strong>contiguous frontier clock</strong> — the highest offset delivered without a gap
 *       on each coordinate this node consumes; and
 *   <li>the <strong>channel clocks</strong> — for each input channel {@code (topicId, partition)},
 *       the dependencies advertised on it (max-merged). {@link #completeness()} is the per-coordinate
 *       minimum across all channels (each channel's advertised deps plus its own delivered position),
 *       which is the delivery gate and the outbound stamp.
 * </ul>
 *
 * <p>The <strong>forwarded-offset index</strong> is <em>not</em> in the {@code "f"} blob: it is a
 * growable, order-sensitive set (offsets delivered above the contiguous frontier) with incremental
 * per-offset writes and range reads, so it keeps its own keyed store, injected here as a collaborator.
 *
 * <p>Core operations: {@link #completeness()} (the delivery boundary), {@link #deliver} (advance the
 * contiguous frontier for a delivered record), {@link #seedIfFirstSeen} (establish the baseline the
 * first time a coordinate is observed, since consumption need not start at offset 0), and the channel
 * accessors. {@link ParsleyEngine} enforces causal transitivity (the cascade after each delivery) and
 * owns the buffer around these operations.
 */
final class ParsleyFrontier {

    private ParsleyClock frontier;
    // Per input channel (topicId, partition) -> the dependencies advertised on it (max-merged).
    private final Map<CoordKey, ParsleyClock> channels = new HashMap<>();
    // Coordinates observed at least once; guards the one-time baseline seed in seedIfFirstSeen.
    private final Set<CoordKey> seenCoordinates = new HashSet<>();
    private final ParsleyForwardedIndex forwardedIndex;
    // The frontier state store, holding this frontier+channels blob at key "f"; null for in-memory
    // (test) instances, which skip persistence.
    private final @Nullable KeyValueStore<String, byte[]> store;
    // When false, channel clocks are not tracked: channelUpdate is a no-op and completeness() is the
    // node's own frontier (single-layer, frontier-only gating). Used to exercise the frontier/buffer
    // mechanics in isolation, without the cross-channel completeness layer.
    private final boolean trackChannels;

    /**
     * In-memory instance that tracks channel clocks: starts from {@code initial} with no channels and
     * no persistence. Used by tests exercising {@link #completeness()} and any caller that does not
     * need a durable frontier.
     */
    ParsleyFrontier(ParsleyClock initial, ParsleyForwardedIndex forwardedIndex) {
        this(initial, forwardedIndex, true);
    }

    /**
     * In-memory instance with channel tracking optionally disabled. With {@code trackChannels = false},
     * {@link #completeness()} is the node's own frontier and {@link #channelUpdate} is a no-op — the
     * single-layer, frontier-only mode used to test frontier/buffer mechanics in isolation.
     */
    ParsleyFrontier(ParsleyClock initial, ParsleyForwardedIndex forwardedIndex, boolean trackChannels) {
        this.frontier = initial;
        this.forwardedIndex = forwardedIndex;
        this.store = null;
        this.trackChannels = trackChannels;
    }

    /**
     * Durable instance: loads the frontier clock and channel clocks from key {@code "f"} of
     * {@code store} (empty if absent), and rewrites that single value on every subsequent change.
     */
    ParsleyFrontier(KeyValueStore<String, byte[]> store, ParsleyForwardedIndex forwardedIndex) {
        this.store = store;
        this.forwardedIndex = forwardedIndex;
        this.trackChannels = true;
        byte[] blob = store.get(ParsleyStores.FRONTIER_KEY);
        this.frontier = ParsleyClock.empty();
        if (blob != null) {
            load(blob);
        }
    }

    /** The current contiguous frontier clock. */
    ParsleyClock snapshot() {
        return frontier;
    }

    /**
     * The causal completeness frontier: for each coordinate, the greatest offset every input channel
     * has confirmed — the per-coordinate {@link ParsleyClock#intersectMin intersection-minimum} across
     * channels, each channel contributing its advertised dependencies plus its own contiguous delivered
     * position. A coordinate any channel has not observed is absent, so a dependency on it is not yet
     * satisfiable. With no channel clocks recorded, this is the node's own frontier.
     */
    ParsleyClock completeness() {
        ParsleyClock result = null;
        for (Map.Entry<CoordKey, ParsleyClock> entry : channels.entrySet()) {
            CoordKey key = entry.getKey();
            // Each channel's view = the dependencies it has advertised, plus its own delivered
            // position so the owning channel supplies its coordinate's contiguous value.
            long ownDelivered = frontier.offsetFor(key.topicId(), key.partition());
            ParsleyClock view = ownDelivered >= 0
                    ? entry.getValue().observe(key.topicId(), key.partition(), ownDelivered)
                    : entry.getValue();
            result = (result == null) ? view : result.intersectMin(view);
        }
        // No channel clocks (cold start): fall back to the node's own frontier.
        return result == null ? frontier : result;
    }

    /**
     * Records that the record at {@code (topicId, partition, offset)} was delivered: marks the offset
     * forwarded, walks the longest contiguous run now achievable, advances the frontier, and persists.
     */
    void deliver(Uuid topicId, int partition, long offset) {
        frontier = frontier.observe(topicId, partition, mergeForward(topicId, partition, offset));
        persist();
    }

    /**
     * Establishes the contiguous frontier's starting point the first time this coordinate is observed.
     * The first offset seen need not be 0 (finite retention, fresh consumer group); anything below it
     * is outside the engine's purview, not an unfillable gap, so folding {@code offset - 1} into the
     * frontier lets the contiguous walk start there. Returns {@code true} if a seed was applied (the
     * caller should then cascade). The coordinate is marked seen on the first call even if the record
     * is held, so a later record cannot re-trigger the seed and skip the still-held earlier one.
     */
    boolean seedIfFirstSeen(Uuid topicId, int partition, long offset) {
        if (!seenCoordinates.add(new CoordKey(topicId, partition))) return false;
        if (offset <= 0) return false;
        if (frontier.offsetFor(topicId, partition) >= 0) return false;
        frontier = frontier.observe(topicId, partition, offset - 1);
        persist();
        return true;
    }

    /** The clock advertised on channel {@code (topicId, partition)}, or empty if never updated. */
    ParsleyClock channelGet(Uuid topicId, int partition) {
        ParsleyClock clock = channels.get(new CoordKey(topicId, partition));
        return clock == null ? ParsleyClock.empty() : clock;
    }

    /**
     * Max-merges {@code clock} into channel {@code (topicId, partition)}'s advertised dependencies
     * (monotonic: the stored clock never decreases) and persists. A first call for a channel
     * initialises it from {@code clock}.
     */
    void channelUpdate(Uuid topicId, int partition, ParsleyClock clock) {
        if (!trackChannels) {
            return;
        }
        CoordKey key = new CoordKey(topicId, partition);
        ParsleyClock existing = channels.get(key);
        channels.put(key, existing == null ? clock : existing.merge(clock));
        persist();
    }

    /** The number of channels currently recorded (including seeded, silent ones). */
    int channelCount() {
        return channels.size();
    }

    /**
     * Prunes causal state to the coordinates {@code inScope} accepts: retains the frontier clock and
     * drops any channel whose coordinate is out of scope (e.g. a topic dropped and recreated with a
     * new UUID). Called once at init before seeding the current input channels.
     */
    void pruneToScope(ParsleyClock.CoordinatePredicate inScope) {
        frontier = frontier.retaining(inScope);
        channels.keySet().removeIf(key -> !inScope.test(key.topicId(), key.partition()));
        persist();
    }

    /**
     * Marks {@code offset} forwarded and returns the longest contiguous run now achievable on
     * {@code (topicId, partition)} — {@code offset} itself if nothing above it is pending, or further
     * if this offset closed a gap. Prunes absorbed entries from the forwarded index.
     */
    private long mergeForward(Uuid topicId, int partition, long offset) {
        forwardedIndex.mark(topicId, partition, offset);
        long watermark = frontier.offsetFor(topicId, partition);
        long extended = watermark;
        for (long candidate : forwardedIndex.forwardedAfter(topicId, partition, watermark)) {
            if (candidate != extended + 1) break;
            forwardedIndex.unmark(topicId, partition, candidate);
            extended = candidate;
        }
        return extended;
    }

    private void persist() {
        if (store != null) {
            store.put(ParsleyStores.FRONTIER_KEY, toBytes());
        }
    }

    /**
     * Serialises the frontier clock and channel clocks into the single {@code "f"} value:
     * {@code [frontier-len:4][frontier bytes][channel-count:4]} then per channel
     * {@code [topicId MSB:8][topicId LSB:8][partition:4][clock-len:4][clock bytes]}.
     */
    private byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            byte[] f = frontier.toBytes();
            dos.writeInt(f.length);
            dos.write(f);
            dos.writeInt(channels.size());
            for (Map.Entry<CoordKey, ParsleyClock> entry : channels.entrySet()) {
                dos.writeLong(entry.getKey().topicId().getMostSignificantBits());
                dos.writeLong(entry.getKey().topicId().getLeastSignificantBits());
                dos.writeInt(entry.getKey().partition());
                byte[] c = entry.getValue().toBytes();
                dos.writeInt(c.length);
                dos.write(c);
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyFrontier serialisation failed", e);
        }
    }

    private void load(byte[] blob) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(blob))) {
            byte[] f = dis.readNBytes(dis.readInt());
            frontier = ParsleyClock.fromBytes(f);
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                long msb = dis.readLong();
                long lsb = dis.readLong();
                int partition = dis.readInt();
                byte[] c = dis.readNBytes(dis.readInt());
                channels.put(new CoordKey(new Uuid(msb, lsb), partition), ParsleyClock.fromBytes(c));
            }
        } catch (IOException e) {
            throw new IllegalStateException("ParsleyFrontier deserialisation failed", e);
        }
    }

    private record CoordKey(Uuid topicId, int partition) {}
}