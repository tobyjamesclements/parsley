package io.github.tobyjamesclements.parsley.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason;

/**
 * The ordering engine for one process (SPEC: one unit that receives, delivers and sends on channels; on Kafka, one
 * Streams task). Host-independent: the Kafka Streams adapter and the test simulator drive this same class.
 *
 * <p>See {@code docs/DESIGN.md}. Per received channel the engine tracks {@code fedUpTo} — the highest position such
 * that every position at or below it was either fed to this process or will never arrive as a message — and a
 * hold-back buffer of received-but-undelivered messages in position order. The settled frontier derived from these
 * feeds the pure decision unit {@link Deliverability#decide}, which the engine invokes for every delivery. The causal
 * frontier accumulates causes from deliveries and from the metadata of everything received (SPEC Structural 15), and
 * stamps every emission.</p>
 *
 * <p>All mutations are written through to the {@link OrderingStore}; under Kafka Streams they commit atomically with
 * the step, so an aborted step rolls the engine's durable state back with everything else. One instance serves one
 * execution: the host constructs a fresh engine over the restored store at each initialisation.</p>
 *
 * <p>Not thread-safe; a process is single-threaded by definition of the host.</p>
 */
public final class ProcessEngine {

    public enum ReceiveOutcome {
        /** Accepted into the hold-back buffer (possibly immediately deliverable). */
        ACCEPTED,
        /** A re-feed of a position already covered by a committed step of an earlier execution; already delivered, dropped (SPEC Safety 2). */
        DUPLICATE_DROPPED
    }

    /** Marks a channel whose topic no longer exists: every remaining position will never yield a message. */
    private static final long FED_TO_END_OF_CHANNEL = Long.MAX_VALUE;

    /** Default bound on encoded causal metadata, well under Kafka's default 1 MiB record-size wall: reaching the
     * wall is a permanent stop inside the producer with no parsley diagnosis (SPEC Operational 4; ASSESSMENT 2.4),
     * so the budget fails closed attributably first. 256 KiB ≈ 9,300 frontier entries. */
    public static final int DEFAULT_METADATA_BUDGET_BYTES = 256 * 1024;

    private final String processName;
    private final TreeSet<ChannelId> receivedChannels;
    private final OrderingStore store;
    private final int metadataBudgetBytes;
    private final Sabotage sabotage;

    private final Map<ChannelId, Long> fedUpTo = new HashMap<>();
    private final TreeMap<ChannelId, Long> frontier = new TreeMap<>();
    /** Per channel, the highest position in the *delivered* causal past: delivered positions and the expressed causes
     * of delivered messages — never causes learned only from still-held metadata. This is what a joining channel must
     * not re-enter (SPEC Structural 16). */
    private final Map<ChannelId, Long> deliveredPast = new HashMap<>();
    private final Map<ChannelId, ArrayDeque<Hold>> held = new HashMap<>();
    /** fedUpTo as restored at initialisation: the boundary between this execution's feed and re-feeds of the past. */
    private final Map<ChannelId, Long> sessionFloor;
    /** The highest position the host has fed on each channel within this execution (in-memory by design: an
     * execution's feed order is the thing being checked). Within one execution the host must feed each channel in
     * increasing position order (Host obligation 1); a regression is a breach, never a replay. */
    private final Map<ChannelId, Long> fedThisExecution = new HashMap<>();

    private static final class Hold {
        final long position;
        final long timestamp;
        final Causes causes;
        boolean persisted;
        byte[] key;
        byte[] value;
        List<HeaderKV> headers;

        Hold(long position, long timestamp, Causes causes, boolean persisted,
             byte[] key, byte[] value, List<HeaderKV> headers) {
            this.position = position;
            this.timestamp = timestamp;
            this.causes = causes;
            this.persisted = persisted;
            this.key = key;
            this.value = value;
            this.headers = headers;
        }
    }

    public ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store) {
        this(processName, receivedChannels, store, DEFAULT_METADATA_BUDGET_BYTES, Sabotage.NONE);
    }

    public ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store,
                         int metadataBudgetBytes) {
        this(processName, receivedChannels, store, metadataBudgetBytes, Sabotage.NONE);
    }

    ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store, Sabotage sabotage) {
        this(processName, receivedChannels, store, DEFAULT_METADATA_BUDGET_BYTES, sabotage);
    }

    ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store,
                  int metadataBudgetBytes, Sabotage sabotage) {
        this.processName = processName;
        this.receivedChannels = new TreeSet<>(receivedChannels.keySet());
        this.store = store;
        this.metadataBudgetBytes = metadataBudgetBytes;
        this.sabotage = sabotage;

        byte[] version = store.get(StoreCodec.versionKey());
        if (version == null) {
            store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        } else if (version.length != 1 || version[0] != StoreCodec.STORE_FORMAT_VERSION) {
            throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "process " + processName + ": ordering store format not understood by this build");
        }

        // SPEC Assumption 2 / Safety 8: a topic deleted and recreated under the same name is a different channel,
        // but the group's committed read positions are keyed by name and would be silently adopted for the new
        // incarnation. Bind each declared name to the channel identity first seen under it; a changed binding means
        // the name's read positions belong to a dead channel, so refuse rather than resume mid-log.
        receivedChannels.forEach((channel, topicName) -> {
            byte[] nameKey = StoreCodec.channelNameKey(topicName);
            byte[] bound = store.get(nameKey);
            if (bound == null) {
                store.put(nameKey, channel.toBytes());
            } else if (!java.util.Arrays.equals(bound, channel.toBytes())) {
                throw new ParsleyFailClosedException(Reason.CHANNEL_IDENTITY_CHANGED,
                        "process " + processName + ": topic '" + topicName + "' now resolves to " + channel
                                + " but this process's state was built against a previous incarnation; its read"
                                + " positions for that name cannot be trusted. Reset the process's state and group"
                                + " offsets deliberately to proceed.");
            }
        });

        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_FED_UP_TO),
                (key, value) -> fedUpTo.put(StoreCodec.channelOfKey(key), StoreCodec.decodeLong(value)));
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_FRONTIER),
                (key, value) -> frontier.put(StoreCodec.channelOfKey(key), StoreCodec.decodeLong(value)));
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_DELIVERED_PAST),
                (key, value) -> deliveredPast.put(StoreCodec.channelOfKey(key), StoreCodec.decodeLong(value)));
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_HELD), (key, value) -> {
            ChannelId channel = StoreCodec.channelOfKey(key);
            long position = StoreCodec.positionOfHeldKey(key);
            if (!this.receivedChannels.contains(channel) && !sabotage.has(Sabotage.Mode.IGNORE_REMOVED_CHANNELS)) {
                // SPEC Structural 16: refuse an execution whose declaration removes a channel with undelivered
                // received messages. Bodies stay in the store; nothing is lost by refusing.
                throw new ParsleyFailClosedException(Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES,
                        "process " + processName + ": held message at " + channel + "@" + position
                                + " but the channel is no longer in the declared received-channel set");
            }
            StoreCodec.HeldBlob blob = StoreCodec.decodeHeld(value);
            // Bodies are lazily reloaded from the store at delivery; only the ordering facts stay in memory.
            held.computeIfAbsent(channel, c -> new ArrayDeque<>())
                    .addLast(new Hold(position, blob.timestamp(), blob.causes(), true, null, null, null));
        });

        // SPEC Structural 16: causal past already delivered must not be re-entered when a channel joins the
        // received-channel set. Positions at or below the delivered causal past on a joining channel were causes of
        // messages this process has already delivered; delivering them now would invert lifetime causal order
        // (SPEC Safety 1), so they are treated as settled before the session floor is taken.
        for (ChannelId channel : this.receivedChannels) {
            Long past = deliveredPast.get(channel);
            if (past != null) {
                advanceFedUpTo(channel, past);
            }
        }
        this.sessionFloor = Map.copyOf(fedUpTo);
    }

    public Set<ChannelId> receivedChannelSet() {
        return Collections.unmodifiableSet(receivedChannels);
    }

    /** The settled frontier the decision unit reads; also usable directly with {@link Deliverability#decide}. */
    public Deliverability.SettledView settledView() {
        return channel -> {
            ArrayDeque<Hold> channelHeld = held.get(channel);
            if (channelHeld != null && !channelHeld.isEmpty()) {
                return OptionalLong.of(channelHeld.peekFirst().position - 1);
            }
            Long fed = fedUpTo.get(channel);
            return fed == null ? OptionalLong.empty() : OptionalLong.of(fed);
        };
    }

    /**
     * Receive one message from a channel. Fails closed on undecodable metadata (SPEC Safety 7) and on feeds that
     * contradict positions this execution already covered (Host obligation 1/2 breach). Does not deliver; callers
     * drain via {@link #nextDeliverable()} / {@link #markDelivered}.
     */
    public ReceiveOutcome onReceive(ReceivedMessage message) {
        ChannelId channel = message.channel();
        if (!receivedChannels.contains(channel)) {
            throw new IllegalArgumentException(
                    "process " + processName + " received on undeclared channel " + channel);
        }

        Causes causes = extractCauses(message);

        // Host obligation 1: within one execution, each channel is fed in increasing position order. A feed at or
        // below a position this execution already fed is a breach, wherever it lies relative to the session floor —
        // only in-order feeds at or below the floor are replays of a committed past. (Observed in the wild as a
        // recreated topic's records arriving under the old channel's identity: fail loudly, never drop silently.)
        Long fedBefore = fedThisExecution.get(channel);
        if (fedBefore != null && message.position() <= fedBefore) {
            throw new ParsleyFailClosedException(Reason.OUT_OF_ORDER_FEED,
                    "process " + processName + ": fed " + channel + "@" + message.position()
                            + " after this execution was already fed position " + fedBefore
                            + " of the same channel — the host must feed each channel in increasing position order");
        }
        fedThisExecution.put(channel, message.position());

        if (sabotage.has(Sabotage.Mode.SILENT_DROP) && message.position() == 3) {
            return ReceiveOutcome.DUPLICATE_DROPPED;
        }

        if (!sabotage.has(Sabotage.Mode.SKIP_RECEIPT_MERGE)) {
            // Happened-before passes through receipt: causes of anything received are causes of every subsequent
            // send, delivered or not (SPEC Structural 15). This must precede the duplicate-drop below — a message
            // dropped because a joining channel must not re-enter delivered past (D31) was still *received*, and
            // its metadata may carry causes this process has never seen.
            causes.byChannel().forEach(this::mergeFrontier);
        }

        Long fed = fedUpTo.get(channel);
        if (fed != null && message.position() <= fed && !sabotage.has(Sabotage.Mode.REDELIVER_REFEEDS)) {
            if (fed == FED_TO_END_OF_CHANNEL) {
                // A dead channel never legitimately feeds again — topic IDs are not reused — so this is not a
                // replay of a delivered past but evidence the substrate or facts were wrong. Fail loudly.
                throw new ParsleyFailClosedException(Reason.OUT_OF_ORDER_FEED,
                        "process " + processName + ": fed " + channel + "@" + message.position()
                                + " on a channel recorded as no longer existing");
            }
            Long floor = sessionFloor.get(channel);
            if (floor == null || message.position() > floor) {
                throw new ParsleyFailClosedException(Reason.OUT_OF_ORDER_FEED,
                        "process " + processName + ": fed " + channel + "@" + message.position()
                                + " which this execution already covered as fed-or-never-arriving (fedUpTo=" + fed + ")");
            }
            // Below the session floor: a committed step of an earlier execution already consumed this position, so
            // it was delivered then; delivering again would breach SPEC Safety 2, which binds across restarts.
            return ReceiveOutcome.DUPLICATE_DROPPED;
        }

        advanceFedUpTo(channel, message.position());
        held.computeIfAbsent(channel, c -> new ArrayDeque<>()).addLast(new Hold(
                message.position(), message.timestamp(), causes, false,
                message.key(), message.value(), message.headers()));
        return ReceiveOutcome.ACCEPTED;
    }

    private Causes extractCauses(ReceivedMessage message) {
        byte[] headerValue = null;
        boolean present = false;
        for (HeaderKV header : message.headers()) {
            if (CausesCodec.HEADER_KEY.equals(header.key())) {
                if (present) {
                    return failUndecodable(message, "duplicate " + CausesCodec.HEADER_KEY + " headers");
                }
                present = true;
                headerValue = header.value();
            }
        }
        if (!present) {
            return Causes.none(); // SPEC Safety 6: no metadata, no causes.
        }
        if (headerValue != null && headerValue.length > metadataBudgetBytes) {
            // SPEC Operational 4: refuse metadata beyond the configured budget on receipt, with parsley's own
            // diagnosis, before it merges into the frontier and rides toward the substrate's record-size wall.
            throw new ParsleyFailClosedException(Reason.METADATA_BUDGET_EXCEEDED,
                    "process " + processName + ": " + message.channel() + "@" + message.position()
                            + " carries " + headerValue.length + " bytes of causal metadata; the configured budget"
                            + " is " + metadataBudgetBytes + " bytes");
        }
        try {
            return CausesCodec.decode(headerValue);
        } catch (CausesCodec.UndecodableMetadataException e) {
            return failUndecodable(message, e.getMessage());
        }
    }

    private Causes failUndecodable(ReceivedMessage message, String detail) {
        if (sabotage.has(Sabotage.Mode.UNDECODABLE_AS_ABSENT)) {
            return Causes.none();
        }
        throw new ParsleyFailClosedException(Reason.UNDECODABLE_METADATA,
                "process " + processName + ": " + message.channel() + "@" + message.position() + ": " + detail);
    }

    /**
     * Ingest reported position facts. Order of operations matters: the read-position report is applied first — a
     * reported position covers everything below it as fed-or-never-arriving, so the truncation check must compare
     * the earliest retained position against the state *including* this batch's report, or a retention pass over an
     * already-covered never-yielding run would fail closed spuriously (and permanently, since restarts replay the
     * same facts). Pruning never touches a cause above the reported earliest retained position.
     */
    public void onFacts(PositionFacts facts) {
        // SPEC Assumption 2: a topic deleted and recreated under the same name is a different channel. For a
        // received channel this is checked before anything else: the feed path subscribes by name, so from the
        // moment of recreation it may carry the new channel's records under the old identity — the only safe
        // response is to stop delivering, mid-run, not merely at the next restart (D33 covers restarts).
        if (!sabotage.has(Sabotage.Mode.IGNORE_RECREATION)) {
            for (ChannelId channel : facts.recreatedChannels()) {
                if (receivedChannels.contains(channel)) {
                    throw new ParsleyFailClosedException(Reason.CHANNEL_IDENTITY_CHANGED,
                            "process " + processName + ": the topic of received channel " + channel
                                    + " was deleted and recreated under the same name while this process ran;"
                                    + " records fed under the old identity can no longer be trusted. Reset the"
                                    + " process's state and group offsets deliberately to proceed.");
                }
            }
        }
        facts.committedNextRead().forEach((channel, nextRead) -> {
            if (receivedChannels.contains(channel)) {
                advanceFedUpTo(channel, nextRead - 1);
            }
        });
        for (ChannelId channel : facts.deadChannels()) {
            if (receivedChannels.contains(channel)) {
                ArrayDeque<Hold> channelHeld = held.get(channel);
                if (channelHeld != null && !channelHeld.isEmpty() && !sabotage.has(Sabotage.Mode.DELIVER_PAST_DEAD_HOLDS)) {
                    // SPEC Safety 9: this process retains received-but-undelivered messages from a channel that no
                    // longer exists. Upstream processes that delivered from it may legally discard its causes from
                    // their metadata the moment they learn of the death (Structural 13), so arriving effects can no
                    // longer be ordered against the held messages — their place in causal order cannot be
                    // preserved locally, and delivering past them is the one thing Safety 9 forbids. Stop.
                    throw new ParsleyFailClosedException(Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES,
                            "process " + processName + ": " + channelHeld.size() + " received message(s) from "
                                    + channel + " remain undelivered but the channel's topic no longer exists;"
                                    + " their place in causal order can no longer be preserved (SPEC Safety 9)."
                                    + " The deletion breached the deletion-hygiene assumption (SPEC Assumption 17);"
                                    + " an operator must reset this process's state deliberately to proceed.");
                }
                // With nothing held from it, the dead channel only settles: the topic is gone and topic IDs are
                // never reused, so every remaining position will never yield a message.
                advanceFedUpTo(channel, FED_TO_END_OF_CHANNEL);
            }
        }
        for (ChannelId channel : receivedChannels) {
            Long logStart = facts.logStart().get(channel);
            Long base = fedUpTo.get(channel);
            if (logStart != null && base != null && base != FED_TO_END_OF_CHANNEL && logStart > base + 1
                    && !sabotage.has(Sabotage.Mode.IGNORE_TRUNCATION)) {
                // Positions in (base, logStart) were discarded by the substrate before this process covered them;
                // they cannot be treated as positions that never carried messages (SPEC Safety 8).
                throw new ParsleyFailClosedException(Reason.POSITIONS_DISCARDED_UNREAD,
                        "process " + processName + ": " + channel + " earliest retained position " + logStart
                                + " is beyond this process's covered position " + base);
            }
        }

        // SPEC Structural 13: discard exactly the causes that can no longer matter — position below the channel's
        // earliest retained position, or channel no longer existing. Stale facts under-prune, never over-prune.
        List<ChannelId> prune = null;
        for (var entry : frontier.entrySet()) {
            ChannelId channel = entry.getKey();
            Long logStart = facts.logStart().get(channel);
            // A recreated topic's old channel is dead by affirmative evidence: its name now denotes another id,
            // and topic IDs are never reused, so the old incarnation cannot come back.
            boolean dead = facts.deadChannels().contains(channel) || facts.recreatedChannels().contains(channel);
            if (dead || (logStart != null && entry.getValue() < logStart)) {
                if (prune == null) {
                    prune = new ArrayList<>();
                }
                prune.add(channel);
            }
        }
        if (prune != null) {
            for (ChannelId channel : prune) {
                frontier.remove(channel);
                store.delete(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, channel));
            }
        }
        // Delivered-past entries age out on the same terms: below the earliest retained position a joining channel
        // could not read them anyway, and a dead channel's identity is never reused.
        List<ChannelId> pastPrune = null;
        for (var entry : deliveredPast.entrySet()) {
            ChannelId channel = entry.getKey();
            Long logStart = facts.logStart().get(channel);
            if (facts.deadChannels().contains(channel) || facts.recreatedChannels().contains(channel)
                    || (logStart != null && entry.getValue() < logStart)) {
                if (pastPrune == null) {
                    pastPrune = new ArrayList<>();
                }
                pastPrune.add(channel);
            }
        }
        if (pastPrune != null) {
            for (ChannelId channel : pastPrune) {
                deliveredPast.remove(channel);
                store.delete(StoreCodec.channelKey(StoreCodec.TAG_DELIVERED_PAST, channel));
            }
        }
    }

    private void advanceFedUpTo(ChannelId channel, long position) {
        Long current = fedUpTo.get(channel);
        if (current == null || position > current) {
            fedUpTo.put(channel, position);
            store.put(StoreCodec.channelKey(StoreCodec.TAG_FED_UP_TO, channel), StoreCodec.encodeLong(position));
        }
    }

    private void mergeFrontier(ChannelId channel, long position) {
        Long current = frontier.get(channel);
        if (current == null || position > current) {
            frontier.put(channel, position);
            store.put(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, channel), StoreCodec.encodeLong(position));
        }
        // SPEC Operational 4 at the point where the bounded quantity actually grows: the frontier is a union
        // across everything received and delivered, so per-message header checks alone would let a process that
        // only receives balloon its persisted frontier past the budget without ever refusing (D52 promises it
        // stops attributably). The size is affine in the entry count, so this costs no encode.
        if (CausesCodec.encodedSize(frontier.size()) > metadataBudgetBytes) {
            throw new ParsleyFailClosedException(Reason.METADATA_BUDGET_EXCEEDED,
                    "process " + processName + ": the causal frontier reached " + CausesCodec.encodedSize(frontier.size())
                            + " bytes (" + frontier.size() + " channels); the configured budget is "
                            + metadataBudgetBytes + " bytes. The frontier's growth law is documented in"
                            + " docs/DESIGN.md §2.");
        }
    }

    /**
     * The next message that may be delivered, or empty when every held head is blocked. Deterministic: channels are
     * scanned in their total order, so identical state yields identical delivery order across executions
     * (SPEC 2-safety 2). Only channel heads are offered to the decision (SPEC Safety 3).
     */
    public Optional<DeliverableMessage> nextDeliverable() {
        Deliverability.SettledView settled = settledView();
        for (ChannelId channel : receivedChannels) {
            ArrayDeque<Hold> channelHeld = held.get(channel);
            if (channelHeld == null || channelHeld.isEmpty()) {
                continue;
            }
            if (sabotage.has(Sabotage.Mode.NO_FIFO)) {
                for (Hold hold : channelHeld) {
                    if (decide(hold, settled)) {
                        return Optional.of(materialize(channel, hold));
                    }
                }
            } else {
                Hold head = channelHeld.peekFirst();
                if (decide(head, settled)) {
                    return Optional.of(materialize(channel, head));
                }
            }
        }
        return Optional.empty();
    }

    private boolean decide(Hold hold, Deliverability.SettledView settled) {
        Causes causes = sabotage.has(Sabotage.Mode.IGNORE_CAUSES) ? Causes.none() : hold.causes;
        return Deliverability.decide(causes, receivedChannels, settled).isDeliverable();
    }

    private DeliverableMessage materialize(ChannelId channel, Hold hold) {
        if (hold.persisted && hold.headers == null) {
            StoreCodec.HeldBlob blob = StoreCodec.decodeHeld(store.get(StoreCodec.heldKey(channel, hold.position)));
            hold.key = blob.key();
            hold.value = blob.value();
            hold.headers = blob.headers();
        }
        return new DeliverableMessage(
                channel, hold.position, hold.timestamp, hold.key, hold.value, hold.headers, hold.causes);
    }

    /**
     * Record that the message returned by {@link #nextDeliverable()} is being delivered in this step. From this point
     * its delivery happened-before every send of this process, so it joins the causal frontier.
     */
    public void markDelivered(ChannelId channel, long position) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        Hold head = channelHeld == null ? null : channelHeld.peekFirst();
        if (head == null || (head.position != position && !sabotage.has(Sabotage.Mode.NO_FIFO))) {
            throw new IllegalStateException("process " + processName + ": markDelivered(" + channel + "@" + position
                    + ") is not the head of the hold-back buffer");
        }
        if (head.position == position) {
            channelHeld.pollFirst();
        } else {
            channelHeld.removeIf(hold -> hold.position == position);
        }
        Hold delivered = head.position == position ? head : null;
        if (delivered == null || delivered.persisted) {
            store.delete(StoreCodec.heldKey(channel, position));
        }
        mergeFrontier(channel, position);
        // The delivered causal past now covers this message and everything its metadata expressed; a channel joining
        // the received set later must start above this (SPEC Structural 16, Safety 1 across the lifetime).
        mergeDeliveredPast(channel, position);
        if (delivered != null) {
            delivered.causes.byChannel().forEach(this::mergeDeliveredPast);
        }
    }

    private void mergeDeliveredPast(ChannelId channel, long position) {
        Long current = deliveredPast.get(channel);
        if (current == null || position > current) {
            deliveredPast.put(channel, position);
            store.put(StoreCodec.channelKey(StoreCodec.TAG_DELIVERED_PAST, channel), StoreCodec.encodeLong(position));
        }
    }

    /** The causes every emission of this step must express: the current causal frontier, canonically encoded.
     * Fails closed when the encoding exceeds the metadata budget (SPEC Operational 4): the alternative is the
     * substrate's record-size wall, a permanent stop with no parsley diagnosis. */
    public byte[] causesHeaderForEmission() {
        byte[] encoded = CausesCodec.encode(frontierSnapshot());
        if (encoded.length > metadataBudgetBytes) {
            throw new ParsleyFailClosedException(Reason.METADATA_BUDGET_EXCEEDED,
                    "process " + processName + ": expressing the causal frontier needs " + encoded.length
                            + " bytes (" + frontier.size() + " channels); the configured budget is "
                            + metadataBudgetBytes + " bytes. The frontier's growth law is documented in"
                            + " docs/DESIGN.md §2.");
        }
        return encoded;
    }

    /** The number of channels the causal frontier currently names (SPEC Operational 5). */
    public int frontierSize() {
        return frontier.size();
    }

    /** The encoded size, in bytes, of the metadata every emission currently carries (SPEC Operational 5).
     * Computed, not encoded: the codec's size is affine in the entry count, and this runs on the stream thread
     * every punctuation. */
    public int frontierBytes() {
        return CausesCodec.encodedSize(frontier.size());
    }

    public Causes frontierSnapshot() {
        if (sabotage.has(Sabotage.Mode.OVEREXPRESS)) {
            TreeMap<ChannelId, Long> inflated = new TreeMap<>(frontier);
            fedUpTo.forEach((channel, fed) -> {
                if (fed >= 0 && fed != FED_TO_END_OF_CHANNEL) {
                    inflated.merge(channel, fed, Math::max);
                }
            });
            return Causes.of(inflated);
        }
        return Causes.of(frontier);
    }

    /**
     * Persist any held messages received in this step and not delivered within it. The adapter must call this before
     * the step can commit — at the end of every {@code process()} — so that a committed read position never covers a
     * message the store does not hold (SPEC Liveness 5).
     */
    public void flushHolds() {
        if (sabotage.has(Sabotage.Mode.DROP_HELD)) {
            return;
        }
        held.forEach((channel, channelHeld) -> {
            for (Hold hold : channelHeld) {
                if (!hold.persisted) {
                    store.put(StoreCodec.heldKey(channel, hold.position),
                            StoreCodec.encodeHeld(hold.timestamp, hold.key, hold.value, hold.headers, hold.causes));
                    hold.persisted = true;
                    // The body is reloaded from the store at delivery; free the memory.
                    hold.key = null;
                    hold.value = null;
                    hold.headers = null;
                }
            }
        });
    }

    // Observability for tests and diagnostics.

    public OptionalLong fedUpTo(ChannelId channel) {
        Long fed = fedUpTo.get(channel);
        return fed == null ? OptionalLong.empty() : OptionalLong.of(fed);
    }

    public int heldCount(ChannelId channel) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        return channelHeld == null ? 0 : channelHeld.size();
    }

    public int heldCountTotal() {
        return held.values().stream().mapToInt(ArrayDeque::size).sum();
    }
}
