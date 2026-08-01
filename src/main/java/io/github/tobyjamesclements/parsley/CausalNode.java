package io.github.tobyjamesclements.parsley;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The protocol implementation, causal delivery with head-of-line blocking per channel.
 *
 * <p>One instance is one causal node (one task). The host feeds records in per-channel offset
 * order through {@link #onRecord}, reports consumer position advances through
 * {@link #positionAdvance}, and stamps every outbound send through {@link #prepareSend}. All
 * state mutations go through the {@link StateStore}, which the host commits atomically with the
 * consumed offsets and the sends of each processing step (EOS).
 *
 * <p>Each channel has a FIFO hold queue, and only the head is ever gated. A record is
 * deliverable when the frontier dominates its dependency clock, normalised (own-channel
 * entries dropped, since FIFO satisfies them structurally) and restricted to consumed channels
 * (unconsumed entries are ignored, sound because every stamp claims consumed ancestry behind an
 * unconsumed coordinate directly). Delivering advances the frontier, which can release other
 * heads. The cascade runs to fixpoint.
 *
 * <p>Liveness rides append-time facts. Every claim names a really-appended offset, and the
 * host's position advances bridge runs of offsets a {@code read_committed} consumer skips
 * (transaction markers, aborted records). The wait graph is acyclic because every wait edge,
 * whether a dependency wait or a head-of-line wait, points from a later-appended record to an
 * earlier-appended one, even under end-offset over-claims, whose watermarks are append-time
 * snapshots taken before the claiming record's own append.
 */
final class CausalNode implements DeliveryProtocol {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CausalNode.class);

    private static final String KEY_SCOPE = "scope";
    private static final String KEY_CARRIED = "ca";
    private static final String PREFIX_FRONTIER = "f/";
    private static final String PREFIX_CHANNEL_CLOCK = "cc/";
    private static final String PREFIX_QUEUE = "q/";
    /** Per sink channel, the last send sequence this node issued. */
    private static final String PREFIX_SEND_SEQ = "sq/";
    /** Per consumed channel and sender, the highest delivered sequence and its offset. */
    private static final String PREFIX_DELIVERED_SEQ = "ds/";

    private record Held(long offset, VectorClock clock, UUID senderId, long senderSeq,
                        byte[] key, byte[] value, long timestamp) {}

    /**
     * The resolution state for one channel and sender.
     *
     * @param seq the highest sequence delivered from that sender on that channel
     * @param offset the offset the sequence was delivered at
     */
    private record SeqMark(long seq, long offset) {}

    private static final class Queue {
        final ArrayDeque<Held> held = new ArrayDeque<>();
        long headIndex; // store index of the current head
        long tailIndex; // store index one past the last element
    }

    private final NodeConfig config;
    private final StateStore store;
    private final BrokerOffsets broker;

    /**
     * The consumed channels and declared sink topics in a stable total order. {@link NodeConfig}
     * normalises both with {@code Set.copyOf}, and the JDK's immutable sets randomise iteration
     * order per JVM (a salt seeded at class-init). Iterating them directly would make the
     * cascade's cross-channel delivery order and the scope record's bytes vary between runs of
     * the same tree, which would in turn make a seeded simulator run depend on the JVM as well
     * as its seed. Ordering is arbitrary but fixed. Nothing depends on which order, only that
     * it is the same one every time.
     */
    private final List<Channel> consumedInOrder;
    private final List<UUID> sinkTopicsInOrder;

    private final Map<Channel, Long> frontier = new HashMap<>();
    private final Map<Channel, VectorClock> channelClocks = new HashMap<>();
    private final VectorClock carriedAncestry;
    /** Memory-only. Seeded from end offsets at init, so persistence would be redundant. */
    private final VectorClock ownOutputs = new VectorClock();
    private final Map<Channel, Queue> queues = new HashMap<>();
    /** Memory-only. The consumer position, below which everything is fetched-or-skipped. */
    private final Map<Channel, Long> positionKnown = new HashMap<>();
    private final Map<Channel, Long> resumePositions = new HashMap<>();
    /** Last issued send sequence per sink channel. Persisted, with -1 meaning none. */
    private final Map<Channel, Long> sendSeq = new HashMap<>();
    /**
     * The send counter as restored at init, per sink channel. Every send at or below it is
     * committed and therefore covered by the init end-offset seed in offset space. Sends above
     * it, this incarnation's, are claimed in sequence space.
     */
    private final Map<Channel, Long> baselineSeq = new HashMap<>();
    /** Highest delivered sequence and its offset, per consumed channel and sender (persisted). */
    private final Map<Channel, Map<UUID, SeqMark>> deliveredSeq = new HashMap<>();
    /** Memory-only. Records fed at or below the frontier and discarded, since init. */
    private long replaysSkipped;

    /**
     * Initialises the node, restoring persisted state and seeding own-outputs from end offsets.
     *
     * @param config the node's identity, consumed channels and declared sink topics
     * @param store the durable state the host commits with each processing step
     * @param broker the end-offset and log-start view for the node's channels
     */
    CausalNode(NodeConfig config, StateStore store, BrokerOffsets broker) {
        this.config = config;
        this.store = store;
        this.broker = broker;
        this.consumedInOrder = config.consumed().stream().sorted().toList();
        this.sinkTopicsInOrder = config.sinkTopics().stream().sorted().toList();

        Scope recorded = readScope();
        restoreState(recorded);
        this.carriedAncestry = readClock(KEY_CARRIED);

        // Own-outputs seed: each declared sink's end offset, an over-claim on real appended
        // offsets (delay-only, therefore sound). Strict resolution: a missing entry here means
        // the topic genuinely does not exist.
        Map<Channel, Long> ends = broker.endOffsets(config.sinkTopics());
        ends.forEach((c, end) -> {
            if (end > 0) ownOutputs.advanceTo(c, end - 1);
        });

        if (recorded != null) {
            rescope(recorded);
        }
        writeScope();
        LOG.info("{}: initialised: frontier {}, carried ancestry {}, send counters {}",
                config.nodeId(), frontier, carriedAncestry, sendSeq);
    }

    private record Scope(Set<Channel> consumed, Set<UUID> sinks) {}

    // ------------------------------------------------------------------ receive path

    @Override
    public List<Delivery> onRecord(InboundRecord r) {
        Channel c = r.channel();
        if (!config.consumed().contains(c)) {
            throw new IllegalStateException(config.nodeId() + ": record on unconsumed channel " + c);
        }
        long f = frontierOf(c);
        if (r.offset() <= f) {
            replaysSkipped++;
            return List.of(); // replay of an already-covered offset (rescope growth refetch)
        }

        // Everything below this record was fetched before it or consumer-skipped.
        notePosition(c, r.offset());
        Queue q = queue(c);
        if (q.held.isEmpty() && r.offset() - 1 > f) {
            // Seed (first sighting) or bridge (skipped run): fold the clean prefix.
            advanceFrontier(c, r.offset() - 1);
        }

        // The inbound clock feeds this channel's advertised view (the stamp side) at receive:
        // required at or before delivery for the ignore branch's soundness, and safe now
        // (custody claims name really-appended offsets; over-claims are delay-only).
        if (r.clock() != null && !r.clock().isEmpty()) {
            VectorClock cc = channelClocks.computeIfAbsent(c, k -> new VectorClock());
            cc.mergeMax(r.clock());
            putClock(PREFIX_CHANNEL_CLOCK + c.key(), cc);
        }

        Held h = new Held(r.offset(), r.clock() == null ? new VectorClock() : r.clock().copy(),
                r.senderId(), r.senderSeq(), r.key(), r.value(), r.timestamp());
        enqueue(c, h);
        LOG.debug("{}: held {} offset {}: deps {}, sender {} seq {}",
                config.nodeId(), c, h.offset(), h.clock(), h.senderId(), h.senderSeq());
        notePosition(c, r.offset() + 1);
        return cascade();
    }

    @Override
    public List<Delivery> positionAdvance(Channel c, long position) {
        if (!config.consumed().contains(c)) {
            throw new IllegalStateException(config.nodeId() + ": position advance on unconsumed channel " + c);
        }
        notePosition(c, position);
        if (queue(c).held.isEmpty() && position - 1 > frontierOf(c)) {
            advanceFrontier(c, position - 1);
            return cascade();
        }
        return List.of();
    }

    /** Runs deliveries to fixpoint. Any frontier advance may release another channel's head. */
    private List<Delivery> cascade() {
        List<Delivery> out = new ArrayList<>();
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Channel c : consumedInOrder) {
                Queue q = queue(c);
                while (!q.held.isEmpty() && deliverable(c, q.held.peekFirst())) {
                    Held h = q.held.pollFirst();
                    store.delete(queueEntryKey(c, q.headIndex));
                    q.headIndex++;
                    putQueueMeta(c, q);
                    advanceFrontier(c, h.offset());
                    if (h.senderId() != null) {
                        noteDeliveredSeq(c, h.senderId(), h.senderSeq(), h.offset());
                    }
                    out.add(new Delivery(c, h.offset(), h.key(), h.value(), h.timestamp()));
                    LOG.debug("{}: delivered {} offset {}", config.nodeId(), c, h.offset());
                    changed = true;
                }
                if (q.held.isEmpty()) {
                    long known = positionKnown.getOrDefault(c, 0L) - 1;
                    if (known > frontierOf(c)) {
                        // Trailing skipped run (markers/aborts) behind the drained queue.
                        advanceFrontier(c, known);
                        changed = true;
                    }
                } else {
                    // Skipped run between the frontier and a held head: every business record
                    // below the head has been delivered (FIFO), so the rest of the prefix was
                    // consumer-skipped and folds in exactly as the empty-queue bridge does.
                    // Without this, no later record and no position advance can ever cover it.
                    long belowHead = q.held.peekFirst().offset() - 1;
                    if (belowHead > frontierOf(c)) {
                        advanceFrontier(c, belowHead);
                        changed = true;
                    }
                }
            }
        }
        return out;
    }

    private boolean deliverable(Channel c, Held h) {
        VectorClock deps = h.clock();
        if (deps.isEmpty()) return true;
        boolean[] blocked = {false};
        deps.forEach((ch, off) -> {
            if (blocked[0]) return;
            if (ch.equals(c)) return; // own-channel entries: FIFO satisfies or self-cycle
            if (!config.consumed().contains(ch)) return; // ignore branch
            if (frontierOf(ch) < off) blocked[0] = true;
        });
        if (blocked[0]) return false;
        deps.forEachSeq((key, seq) -> {
            if (blocked[0]) return;
            Channel ch = key.channel();
            if (ch.equals(c)) return; // own-channel: FIFO satisfies or self-cycle
            if (!config.consumed().contains(ch)) return; // ignore branch
            SeqMark mark = seqMark(ch, key.sender());
            if (mark == null || mark.seq() < seq) blocked[0] = true;
        });
        return !blocked[0];
    }

    // ------------------------------------------------------------------ stamp path

    @Override
    public SendStamp prepareSend(Channel destination) {
        if (destination == null) throw new NullPointerException(
                "destination: sequence claims are per channel, so the host partitions before stamping");
        if (!config.sinkTopics().contains(destination.topicId())) {
            // Fail closed: an undeclared sink's offsets are never end-offset seeded at init,
            // so a restart would silently under-claim the node's own outputs there.
            throw new IllegalStateException(config.nodeId() + ": send to undeclared sink topic "
                    + destination.topicId());
        }

        VectorClock stamp = new VectorClock();
        frontier.forEach(stamp::advanceTo);
        channelClocks.values().forEach(stamp::mergeMax);
        stamp.mergeMax(carriedAncestry);
        stamp.mergeMax(ownOutputs);
        normalize(stamp);

        // Claim this incarnation's own sends in sequence space — assigned synchronously, so
        // nothing waits. Prior incarnations' sends are committed and covered by the init
        // end-offset seed in offset space.
        for (Map.Entry<Channel, Long> e : sendSeq.entrySet()) {
            Channel c = e.getKey();
            long issued = e.getValue();
            if (issued > baselineSeq.getOrDefault(c, VectorClock.NOTHING)) {
                stamp.advanceSeq(c, config.senderId(), issued);
            }
        }

        long seq = sendSeq.getOrDefault(destination, VectorClock.NOTHING) + 1;
        sendSeq.put(destination, seq);
        store.put(PREFIX_SEND_SEQ + destination.key(), longBytes(seq));
        return new SendStamp(stamp, config.senderId(), seq);
    }

    /**
     * Rewrites resolvable sequence claims into offset claims.
     *
     * <p>A claim this node can vouch for, meaning it delivered the sender's record at or past
     * the claimed sequence, becomes a claim on the delivered record's offset. That offset is at
     * or above the claimed record's own offset, an over-claim at worst, delay-only and
     * therefore sound.
     *
     * <p>Echoes of this node's own claims are dropped, because the fresh self-claims and
     * ownOutputs subsume them.
     */
    private void normalize(VectorClock stamp) {
        List<VectorClock.SeqKey> subsumed = new ArrayList<>();
        Map<VectorClock.SeqKey, Long> upgrades = new HashMap<>();
        stamp.forEachSeq((key, seq) -> {
            if (key.sender().equals(config.senderId())) {
                long baseline = baselineSeq.getOrDefault(key.channel(), VectorClock.NOTHING);
                if (seq <= baseline) {
                    // A prior incarnation's echoed claim: its record is committed and covered
                    // by the end-offset seed, so it upgrades to offset space. On a channel that
                    // is no longer a declared sink the seed did not run; the rescope heal put
                    // the same bound in carried ancestry, and a destroyed topic is unclaimable
                    // by anyone, so the claim drops.
                    long covered = ownOutputs.get(key.channel());
                    if (covered < 0) covered = carriedAncestry.get(key.channel());
                    if (covered < 0) {
                        subsumed.add(key);
                    } else {
                        upgrades.put(key, covered);
                    }
                } else {
                    subsumed.add(key); // the fresh self-claim added afterwards dominates it
                }
                return;
            }
            SeqMark mark = seqMark(key.channel(), key.sender());
            if (mark != null && mark.seq() >= seq) {
                upgrades.put(key, mark.offset());
            }
        });
        upgrades.forEach(stamp::normalizeSeq);
        subsumed.forEach(stamp::removeSeq);
    }

    private SeqMark seqMark(Channel c, UUID sender) {
        Map<UUID, SeqMark> m = deliveredSeq.get(c);
        return m == null ? null : m.get(sender);
    }

    private void noteDeliveredSeq(Channel c, UUID sender, long seq, long offset) {
        Map<UUID, SeqMark> m = deliveredSeq.computeIfAbsent(c, k -> new HashMap<>());
        SeqMark cur = m.get(sender);
        if (cur == null || seq > cur.seq()) {
            m.put(sender, new SeqMark(seq, offset));
            ByteBuffer b = ByteBuffer.allocate(16);
            b.putLong(seq);
            b.putLong(offset);
            store.put(PREFIX_DELIVERED_SEQ + c.key() + "/" + sender, b.array());
        }
    }

    // ------------------------------------------------------------------ truncation

    @Override
    public Map<Channel, Long> resumePositions() {
        return Map.copyOf(resumePositions);
    }

    @Override
    public void truncate(VectorClock stability) {
        carriedAncestry.truncateAtOrBelow(stability);
        putClock(KEY_CARRIED, carriedAncestry);
        channelClocks.forEach((c, cc) -> {
            cc.truncateAtOrBelow(stability);
            putClock(PREFIX_CHANNEL_CLOCK + c.key(), cc);
        });
    }

    @Override
    public Set<Channel> stampChannels() {
        Set<Channel> out = new HashSet<>();
        carriedAncestry.forEach((c, o) -> out.add(c));
        channelClocks.values().forEach(cc -> cc.forEach((c, o) -> out.add(c)));
        return out;
    }

    /**
     * Builds the log-start stability bound and truncates with it.
     *
     * <p>The bound is {@code logStart - 1} for each present channel, and everything for a
     * channel absent from {@code logStarts} whose topic is confirmed destroyed.
     *
     * @param logStarts the log start of every channel in {@link #stampChannels()}
     * @param confirmedAbsent only channels whose topic definitively no longer exists, never a
     *         transient resolution failure, so that the sweep fails closed
     */
    public void truncateToLogStarts(Map<Channel, Long> logStarts, Set<Channel> confirmedAbsent) {
        VectorClock stability = new VectorClock();
        logStarts.forEach((c, logStart) -> {
            if (logStart > 0) stability.advanceTo(c, logStart - 1);
        });
        for (Channel c : confirmedAbsent) {
            stability.advanceTo(c, Long.MAX_VALUE);
            LOG.info("{}: topic of {} confirmed destroyed; its claims truncate entirely", config.nodeId(), c);
        }
        if (!stability.isEmpty()) truncate(stability);
        LOG.debug("{}: truncation sweep applied stability bound {}", config.nodeId(), stability);
    }

    // ------------------------------------------------------------------ observation

    /**
     * The two claim-kind widths of the stamp-side clock.
     *
     * @param offsetEntries how many offset claims the clock carries
     * @param sequenceEntries how many sequence claims it carries
     */
    record StampWidth(int offsetEntries, int sequenceEntries) {}

    /**
     * Returns the number of held records per consumed channel.
     *
     * @return the held count of each channel, a channel with an empty hold queue being absent
     */
    Map<Channel, Integer> heldCounts() {
        Map<Channel, Integer> out = new HashMap<>();
        queues.forEach((c, q) -> {
            if (!q.held.isEmpty()) out.put(c, q.held.size());
        });
        return out;
    }

    /**
     * Returns the offset of each non-empty hold queue's head, the record gating its channel.
     *
     * @return the head offset of each channel with a non-empty hold queue
     */
    Map<Channel, Long> headOffsets() {
        Map<Channel, Long> out = new HashMap<>();
        queues.forEach((c, q) -> {
            Held h = q.held.peekFirst();
            if (h != null) out.put(c, h.offset());
        });
        return out;
    }

    /**
     * Returns how many records were fed at or below the frontier and discarded, since init.
     *
     * <p>These arise from a rescope-growth refetch.
     *
     * @return the discarded-replay count
     */
    long replaysSkipped() {
        return replaysSkipped;
    }

    /**
     * One claim a held head is still waiting for, with the local watermarks it is measured
     * against.
     *
     * <p>An offset claim carries {@code claimedOffset} and a null sender. A sequence claim
     * carries {@code sender} and {@code claimedSeq}.
     *
     * @param channel the channel the missing cause sits on
     * @param claimedOffset the offset claimed, or {@code NOTHING} for a sequence claim
     * @param localFrontier the highest offset delivered locally on that channel
     * @param localPosition the local consumer position on that channel
     * @param sender the claimed record's sender, or null for an offset claim
     * @param claimedSeq the send sequence claimed, or {@code NOTHING} for an offset claim
     * @param deliveredSeq the highest sequence delivered locally from that sender on that
     *         channel, or {@code NOTHING} if none has been
     */
    record UnmetClaim(Channel channel, long claimedOffset, long localFrontier, long localPosition,
                      UUID sender, long claimedSeq, long deliveredSeq) {}

    /**
     * One channel's gating head.
     *
     * @param channel the channel the head gates
     * @param offset the head record's offset
     * @param queueDepth how many records wait on this channel, the head included
     * @param unmet the claims the head still waits on
     */
    record HeadHold(Channel channel, long offset, int queueDepth, List<UnmetClaim> unmet) {}

    /**
     * Explains why each non-empty hold queue's head is waiting.
     *
     * <p>The same predicate as {@link #deliverable}, collecting every unmet claim instead of
     * stopping at the first. Only heads are reported, because only heads are gated. Everything
     * behind a head waits on the head, not on causes of its own.
     *
     * <p>Read-only, and called from the host's punctuator on the stream thread.
     *
     * @return one entry per gating head, a head with no unmet claim being deliverable and so
     *         never appearing
     */
    List<HeadHold> explainHeads() {
        List<HeadHold> out = new ArrayList<>();
        queues.forEach((c, q) -> {
            Held h = q.held.peekFirst();
            if (h == null) return;
            List<UnmetClaim> unmet = new ArrayList<>();
            long position = positionKnown.getOrDefault(c, 0L);
            h.clock().forEach((ch, off) -> {
                if (ch.equals(c) || !config.consumed().contains(ch)) return;
                if (frontierOf(ch) < off) {
                    unmet.add(new UnmetClaim(ch, off, frontierOf(ch),
                            positionKnown.getOrDefault(ch, 0L), null, VectorClock.NOTHING,
                            VectorClock.NOTHING));
                }
            });
            h.clock().forEachSeq((key, seq) -> {
                Channel ch = key.channel();
                if (ch.equals(c) || !config.consumed().contains(ch)) return;
                SeqMark mark = seqMark(ch, key.sender());
                if (mark == null || mark.seq() < seq) {
                    unmet.add(new UnmetClaim(ch, VectorClock.NOTHING, frontierOf(ch),
                            positionKnown.getOrDefault(ch, 0L), key.sender(), seq,
                            mark == null ? VectorClock.NOTHING : mark.seq()));
                }
            });
            if (!unmet.isEmpty()) {
                out.add(new HeadHold(c, h.offset(), q.held.size(), unmet));
            } else {
                LOG.debug("{}: head {} offset {} has no unmet claim; it releases on the next"
                        + " cascade", config.nodeId(), c, h.offset());
            }
        });
        return out;
    }

    /**
     * Returns the width of the merged stamp-side clock, the claims {@link #prepareSend} stamps
     * before normalisation and the fresh self-claims.
     *
     * <p>Growth against a steady workload means truncation is not keeping up with the clock's
     * footprint.
     *
     * @return the offset-claim and sequence-claim counts
     */
    StampWidth stampWidth() {
        VectorClock vt = vectorTime();
        return new StampWidth(vt.offsetEntryCount(), vt.sequenceEntryCount());
    }

    // ------------------------------------------------------------------ state plumbing

    private long frontierOf(Channel c) {
        return frontier.getOrDefault(c, VectorClock.NOTHING);
    }

    private void advanceFrontier(Channel c, long offset) {
        long cur = frontierOf(c);
        if (offset <= cur) return;
        frontier.put(c, offset);
        store.put(PREFIX_FRONTIER + c.key(), longBytes(offset));
    }

    private void notePosition(Channel c, long position) {
        positionKnown.merge(c, position, Math::max);
    }

    private Queue queue(Channel c) {
        return queues.computeIfAbsent(c, k -> new Queue());
    }

    private void enqueue(Channel c, Held h) {
        Queue q = queue(c);
        store.put(queueEntryKey(c, q.tailIndex), serializeHeld(h));
        q.tailIndex++;
        putQueueMeta(c, q);
        q.held.addLast(h);
    }

    private String queueEntryKey(Channel c, long index) {
        return PREFIX_QUEUE + c.key() + "/e/" + String.format("%016x", index);
    }

    private void putQueueMeta(Channel c, Queue q) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(q.headIndex);
        b.putLong(q.tailIndex);
        store.put(PREFIX_QUEUE + c.key() + "/m", b.array());
    }

    private void putClock(String key, VectorClock k) {
        store.put(key, k.serialize());
    }

    private VectorClock readClock(String key) {
        byte[] b = store.get(key);
        return b == null ? new VectorClock() : VectorClock.deserialize(b);
    }

    private static byte[] longBytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static long bytesLong(byte[] b) {
        return ByteBuffer.wrap(b).getLong();
    }

    // ------------------------------------------------------------------ init: restore + rescope

    private void restoreState(Scope recorded) {
        store.forEachPrefix(PREFIX_FRONTIER, (k, v) ->
                frontier.put(Channel.fromKey(k.substring(PREFIX_FRONTIER.length())), bytesLong(v)));
        store.forEachPrefix(PREFIX_CHANNEL_CLOCK, (k, v) ->
                channelClocks.put(Channel.fromKey(k.substring(PREFIX_CHANNEL_CLOCK.length())), VectorClock.deserialize(v)));
        store.forEachPrefix(PREFIX_SEND_SEQ, (k, v) ->
                sendSeq.put(Channel.fromKey(k.substring(PREFIX_SEND_SEQ.length())), bytesLong(v)));
        // Every committed send is appended (EOS commit awaits sends), and the end-offset seed
        // covers those offsets; the restored counters are this incarnation's baseline.
        baselineSeq.putAll(sendSeq);
        store.forEachPrefix(PREFIX_DELIVERED_SEQ, (k, v) -> {
            String rest = k.substring(PREFIX_DELIVERED_SEQ.length());
            int slash = rest.lastIndexOf('/');
            Channel c = Channel.fromKey(rest.substring(0, slash));
            UUID sender = UUID.fromString(rest.substring(slash + 1));
            ByteBuffer b = ByteBuffer.wrap(v);
            deliveredSeq.computeIfAbsent(c, x -> new HashMap<>())
                    .put(sender, new SeqMark(b.getLong(), b.getLong()));
        });

        // Hold queues: meta gives [head, tail); entries restore in index order.
        Map<Channel, TreeMap<Long, Held>> loose = new HashMap<>();
        store.forEachPrefix(PREFIX_QUEUE, (k, v) -> {
            String rest = k.substring(PREFIX_QUEUE.length());
            int slash = rest.lastIndexOf('/');
            String tail = rest.substring(slash + 1);
            if (tail.equals("m")) {
                Channel c = Channel.fromKey(rest.substring(0, slash));
                ByteBuffer b = ByteBuffer.wrap(v);
                Queue q = queue(c);
                q.headIndex = b.getLong();
                q.tailIndex = b.getLong();
            } else {
                String chKey = rest.substring(0, rest.lastIndexOf("/e/"));
                Channel c = Channel.fromKey(chKey);
                loose.computeIfAbsent(c, x -> new TreeMap<>()).put(Long.parseLong(tail, 16), deserializeHeld(v));
            }
        });
        loose.forEach((c, entries) -> {
            Queue q = queue(c);
            entries.forEach((idx, h) -> {
                if (idx >= q.headIndex && idx < q.tailIndex) q.held.addLast(h);
            });
            if (!q.held.isEmpty()) LOG.info("{}: restored {} held records on {}", config.nodeId(), q.held.size(), c);
            if (q.held.size() != q.tailIndex - q.headIndex) {
                throw new IllegalStateException(config.nodeId() + ": hold queue for " + c
                        + " restored " + q.held.size() + " of " + (q.tailIndex - q.headIndex) + " entries");
            }
        });
    }

    private void rescope(Scope recorded) {
        // Former sinks: their last claims must survive in the stamp. End-offset over-claim
        // heals them; a destroyed topic (absent from resolution) is unclaimable by anyone.
        Set<UUID> formerSinks = new HashSet<>(recorded.sinks());
        formerSinks.removeAll(config.sinkTopics());
        if (!formerSinks.isEmpty()) {
            broker.endOffsets(formerSinks).forEach((c, end) -> {
                if (end > 0) carriedAncestry.advanceTo(c, end - 1);
            });
            putClock(KEY_CARRIED, carriedAncestry);
            LOG.info("{}: rescope: former sinks {} healed into carried ancestry",
                    config.nodeId(), formerSinks);
        }

        // Shrunk inputs: the delivered past may be skipped, never dropped — max-merge into
        // carried ancestry so every later stamp still claims it.
        for (Channel gone : recorded.consumed()) {
            if (config.consumed().contains(gone)) continue;
            Queue q = queues.get(gone);
            if (q != null && !q.held.isEmpty()) {
                throw new IllegalStateException(config.nodeId() + ": " + q.held.size()
                        + " held records on removed input " + gone
                        + "; redeclare the input or perform a full reset (fail closed)");
            }
            long f = frontierOf(gone);
            if (f >= 0) carriedAncestry.advanceTo(gone, f);
            VectorClock cc = channelClocks.remove(gone);
            if (cc != null) carriedAncestry.mergeMax(cc);
            frontier.remove(gone);
            store.delete(PREFIX_FRONTIER + gone.key());
            store.delete(PREFIX_CHANNEL_CLOCK + gone.key());
            store.delete(PREFIX_QUEUE + gone.key() + "/m");
            Map<UUID, SeqMark> goneMarks = deliveredSeq.remove(gone);
            if (goneMarks != null) {
                for (UUID sender : goneMarks.keySet()) {
                    store.delete(PREFIX_DELIVERED_SEQ + gone.key() + "/" + sender);
                }
            }
            putClock(KEY_CARRIED, carriedAncestry);
            LOG.info("{}: rescope: input {} removed; its delivered past is carried ancestry",
                    config.nodeId(), gone);
        }

        // Grown inputs: seed at the node's carried knowledge — skip what was already ignored,
        // never restart at log-start. The host starts fetching one past the seed.
        for (Channel grown : consumedInOrder) {
            if (recorded.consumed().contains(grown)) continue;
            VectorClock vt = vectorTime();
            long seed = vt.get(grown);
            if (seed >= 0) {
                advanceFrontier(grown, seed);
                resumePositions.put(grown, seed + 1);
                LOG.info("{}: rescope: input {} added; frontier seeded at {}",
                        config.nodeId(), grown, seed);
            }
        }
    }

    private VectorClock vectorTime() {
        VectorClock vt = new VectorClock();
        frontier.forEach(vt::advanceTo);
        channelClocks.values().forEach(vt::mergeMax);
        vt.mergeMax(carriedAncestry);
        vt.mergeMax(ownOutputs);
        return vt;
    }

    private Scope readScope() {
        byte[] b = store.get(KEY_SCOPE);
        if (b == null) return null;
        ByteBuffer buf = ByteBuffer.wrap(b);
        int nc = buf.getInt();
        Set<Channel> consumed = new HashSet<>();
        for (int i = 0; i < nc; i++) {
            consumed.add(new Channel(new UUID(buf.getLong(), buf.getLong()), buf.getInt()));
        }
        int ns = buf.getInt();
        Set<UUID> sinks = new HashSet<>();
        for (int i = 0; i < ns; i++) {
            sinks.add(new UUID(buf.getLong(), buf.getLong()));
        }
        return new Scope(consumed, sinks);
    }

    private void writeScope() {
        ByteBuffer buf = ByteBuffer.allocate(8 + config.consumed().size() * 20 + config.sinkTopics().size() * 16);
        buf.putInt(config.consumed().size());
        for (Channel c : consumedInOrder) {
            buf.putLong(c.topicId().getMostSignificantBits());
            buf.putLong(c.topicId().getLeastSignificantBits());
            buf.putInt(c.partition());
        }
        buf.putInt(config.sinkTopics().size());
        for (UUID s : sinkTopicsInOrder) {
            buf.putLong(s.getMostSignificantBits());
            buf.putLong(s.getLeastSignificantBits());
        }
        store.put(KEY_SCOPE, buf.array());
    }

    // ------------------------------------------------------------------ held-record wire form

    private static byte[] serializeHeld(Held h) {
        byte[] clock = h.clock().serialize();
        int keyLen = h.key() == null ? -1 : h.key().length;
        int valLen = h.value() == null ? -1 : h.value().length;
        ByteBuffer b = ByteBuffer.allocate(8 + 8 + 1 + 16 + 8 + 4 + clock.length
                + 4 + Math.max(keyLen, 0) + 4 + Math.max(valLen, 0));
        b.putLong(h.offset());
        b.putLong(h.timestamp());
        b.put((byte) (h.senderId() == null ? 0 : 1));
        b.putLong(h.senderId() == null ? 0 : h.senderId().getMostSignificantBits());
        b.putLong(h.senderId() == null ? 0 : h.senderId().getLeastSignificantBits());
        b.putLong(h.senderSeq());
        b.putInt(clock.length);
        b.put(clock);
        b.putInt(keyLen);
        if (keyLen > 0) b.put(h.key());
        b.putInt(valLen);
        if (valLen > 0) b.put(h.value());
        return b.array();
    }

    private static Held deserializeHeld(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes);
        long offset = b.getLong();
        long timestamp = b.getLong();
        boolean tagged = b.get() != 0;
        long msb = b.getLong();
        long lsb = b.getLong();
        long seq = b.getLong();
        byte[] clock = new byte[b.getInt()];
        b.get(clock);
        int keyLen = b.getInt();
        byte[] key = keyLen < 0 ? null : new byte[keyLen];
        if (keyLen > 0) b.get(key);
        int valLen = b.getInt();
        byte[] value = valLen < 0 ? null : new byte[valLen];
        if (valLen > 0) b.get(value);
        return new Held(offset, VectorClock.deserialize(clock),
                tagged ? new UUID(msb, lsb) : null, tagged ? seq : -1, key, value, timestamp);
    }
}
