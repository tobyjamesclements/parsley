package io.github.tobyjamesclements.parsley.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason;

/**
 * The protocol, driven by a host.
 *
 * <p>The engine holds a causal frontier, a per-channel hold-back buffer, and the record of
 * what it has already delivered. A host feeds it messages, asks it what is deliverable, and
 * tells it what was delivered; at initialisation it also tells the engine where each channel's
 * feed will start and which channels no longer exist. Nothing is reported between deliveries:
 * a cause names the position of a message that was sent (wire-format constraint 8), so
 * receiving that message is what satisfies it (D115). The engine names no host type, consults
 * no clock and opens no connection, so the same engine runs under a simulator and under Kafka
 * Streams.
 *
 * <p>The delivery rule itself lives in {@link Deliverability#decide}, which is a pure
 * function. This class holds the state that rule reads.
 *
 * <p>Instances are not thread-safe. A host drives one engine from one thread.
 *
 * @see Deliverability
 * @see OrderingStore
 */
public final class ProcessEngine {

    /** What became of a message the host fed in. */
    public enum ReceiveOutcome {
        /** Taken into the hold-back buffer, or immediately deliverable. */
        ACCEPTED,
        /** Already delivered in a previous execution, so ignored. */
        DUPLICATE_DROPPED
    }

    private static final long FED_TO_END_OF_CHANNEL = Long.MAX_VALUE;

    /** Metadata budget applied where a configuration names none. */
    public static final int DEFAULT_METADATA_BUDGET_BYTES = 256 * 1024;

    private final String processName;
    private final TreeSet<ChannelId> receivedChannels;
    private final OrderingStore store;
    private final int metadataBudgetBytes;
    private final Sabotage sabotage;

    private final Map<ChannelId, Long> fedUpTo = new HashMap<>();
    private final TreeMap<ChannelId, Long> frontier = new TreeMap<>();

    // The frontier's encoded width, maintained incrementally so the budget check in
    // mergeFrontier stays O(1) now that size is a function of the frontier's shape rather
    // than its entry count (D98). frontierBodyBytes counts everything after the version
    // byte and the topic-count varint; the per-topic partition counts supply the
    // varint-width deltas as groups grow, shrink, appear and empty. Kept in step at the
    // frontier's three mutation sites — restore, merge, prune; positions update in place
    // without changing size — and pinned against CausesCodec.encode by ProcessEngineTest.
    private final Map<UUID, Integer> frontierTopicPartitions = new HashMap<>();
    private int frontierBodyBytes;

    private final Map<ChannelId, Long> deliveredPast = new HashMap<>();
    private final Map<ChannelId, ArrayDeque<Hold>> held = new HashMap<>();

    // Holds taken in since the last flush, in receipt order. A flush persists exactly these,
    // so its cost follows the holds added since the previous flush rather than the depth of
    // every buffer, which is what keeps a deep hold-back buffer from taxing every later
    // receipt (D102).
    private final ArrayDeque<Hold> unpersisted = new ArrayDeque<>();

    // The frontier's encoded form, built on first use and dropped at the frontier's
    // mutation sites, so a step that emits several messages encodes once and a step that
    // emits none never encodes (D102).
    private byte[] encodedFrontier;

    private final Map<ChannelId, Long> sessionFloor;

    private final Map<ChannelId, Long> fedThisExecution = new HashMap<>();

    /**
     * One held message.
     *
     * <p>The decoded form — causes, key, value and headers — is in memory in exactly two
     * cases: the hold has not been persisted yet, or it is the head of its channel's buffer.
     * Everything else lives only in the store, because the delivery decision reads the head
     * and nothing but the head; a hold is decoded from the store when it becomes head, and a
     * flush drops the decoded form of every hold behind one. That keeps a deep buffer at
     * O(held) references plus O(channels) decoded messages, rather than O(held × frontier)
     * decoded entries (D102). {@code causes == null} is the one spelling of "not in
     * memory"; the four decoded fields are loaded and dropped together.
     */
    private static final class Hold {
        final ChannelId channel;
        final long position;
        final long timestamp;
        Causes causes;
        boolean persisted;
        /** Delivered, or otherwise out of its buffer, so a later flush must not write it. */
        boolean removed;
        byte[] key;
        byte[] value;
        List<HeaderKV> headers;

        Hold(ChannelId channel, long position, long timestamp, Causes causes, boolean persisted,
             byte[] key, byte[] value, List<HeaderKV> headers) {
            this.channel = channel;
            this.position = position;
            this.timestamp = timestamp;
            this.causes = causes;
            this.persisted = persisted;
            this.key = key;
            this.value = value;
            this.headers = headers;
        }

        boolean decoded() {
            return causes != null;
        }
    }

    /**
     * Builds an engine with the default metadata budget and no start positions.
     *
     * @param processName      the process name, used in diagnostics
     * @param receivedChannels the channels this process receives, mapped to their topic names
     * @param store            durable ordering state, which may already hold a previous run's
     * @throws ParsleyFailClosedException if the store carries a format version this build
     *         cannot read
     */
    public ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store) {
        this(processName, receivedChannels, store, DEFAULT_METADATA_BUDGET_BYTES, Sabotage.NONE, Map.of());
    }

    /**
     * Builds an engine with an explicit metadata budget and no start positions.
     *
     * @param processName         the process name, used in diagnostics
     * @param receivedChannels    the channels this process receives, mapped to topic names
     * @param store               durable ordering state
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @throws ParsleyFailClosedException if the store carries a format version this build
     *         cannot read
     */
    public ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store,
                         int metadataBudgetBytes) {
        this(processName, receivedChannels, store, metadataBudgetBytes, Sabotage.NONE, Map.of());
    }

    /**
     * Builds an engine with an explicit metadata budget and the positions the host feeds
     * from.
     *
     * <p>{@code startPositions} is, per received channel, the first position the host will
     * feed in this execution: every position below it was fed to an earlier execution of
     * this process and committed, or lies below the position the process was started at. A
     * cause naming a position below it is therefore treated as satisfied (SPEC Structural
     * 12, Host obligation 2), which is what lets a process started at a channel's end, or
     * one whose channel joined the received set above discarded history, deliver an effect
     * whose cause it will never receive. Coverage the store already holds is never lowered;
     * a channel absent from the map, or at position zero, is restored unchanged.
     *
     * @param processName         the process name, used in diagnostics
     * @param receivedChannels    the channels this process receives, mapped to topic names
     * @param store               durable ordering state
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @param startPositions      per received channel, the first position the host feeds
     * @throws ParsleyFailClosedException if the store carries a format version this build
     *         cannot read
     */
    public ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store,
                         int metadataBudgetBytes, Map<ChannelId, Long> startPositions) {
        this(processName, receivedChannels, store, metadataBudgetBytes, Sabotage.NONE, startPositions);
    }

    ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store, Sabotage sabotage) {
        this(processName, receivedChannels, store, DEFAULT_METADATA_BUDGET_BYTES, sabotage, Map.of());
    }

    ProcessEngine(String processName, Map<ChannelId, String> receivedChannels, OrderingStore store,
                  int metadataBudgetBytes, Sabotage sabotage, Map<ChannelId, Long> startPositions) {
        this.processName = processName;
        this.receivedChannels = new TreeSet<>(receivedChannels.keySet());
        this.store = store;
        this.metadataBudgetBytes = metadataBudgetBytes;
        this.sabotage = sabotage;

        byte[] version = store.get(StoreCodec.versionKey());
        if (version == null) {
            refuseUnversionedState();
            store.put(StoreCodec.versionKey(), new byte[] {StoreCodec.STORE_FORMAT_VERSION});
        } else if (version.length != 1 || version[0] != StoreCodec.STORE_FORMAT_VERSION) {
            throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "process " + processName + ": ordering store format not understood by this build");
        }

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
                (key, value) -> fedUpTo.put(StoreCodec.channelOfEntryKey(key), StoreCodec.decodeLong(value)));
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_FRONTIER), (key, value) -> {
            ChannelId channel = StoreCodec.channelOfEntryKey(key);
            // The reserved zero topic id can only have entered a frontier through a forged
            // header absorbed before wire-format constraint 5 refused it at receipt: no
            // substrate query can ever answer for it, so restoring it would re-express and
            // re-persist untrustworthy state forever. Stored state that cannot be trusted
            // is a reason to stop (D88).
            if (ChannelId.isZeroTopicId(channel.topicId())) {
                throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                        "process " + processName + ": restored frontier names the reserved zero topic id;"
                                + " this state absorbed a forged causes header before receipt refused it"
                                + " (wire-format constraint 5). Reset the process's state and group offsets"
                                + " deliberately to proceed.");
            }
            frontier.put(channel, requireAssignablePosition(channel, StoreCodec.decodeLong(value), "frontier"));
            frontierSizeAdd(channel);
        });
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_DELIVERED_PAST), (key, value) -> {
            ChannelId channel = StoreCodec.channelOfEntryKey(key);
            deliveredPast.put(channel,
                    requireAssignablePosition(channel, StoreCodec.decodeLong(value), "delivered past"));
        });
        store.scanPrefix(StoreCodec.tagPrefix(StoreCodec.TAG_HELD), (key, value) -> {
            ChannelId channel = StoreCodec.channelOfHeldKey(key);
            long position = StoreCodec.positionOfHeldKey(key);
            if (!this.receivedChannels.contains(channel) && !sabotage.has(Sabotage.Mode.IGNORE_REMOVED_CHANNELS)) {
                throw new ParsleyFailClosedException(Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES,
                        "process " + processName + ": held message at " + channel + "@" + position
                                + " but the channel is no longer in the declared received-channel set");
            }
            // Decoded here to refuse a corrupt blob at start rather than at delivery; only
            // the skeleton is retained, the decoded form is reloaded when the hold reaches
            // the head of its buffer (D102).
            StoreCodec.HeldBlob blob = StoreCodec.decodeHeld(value);

            // Everything downstream treats the deque head as the minimum held position, so
            // the scan order the store promises is verified rather than assumed.
            ArrayDeque<Hold> buffer = held.computeIfAbsent(channel, c -> new ArrayDeque<>());
            Hold last = buffer.peekLast();
            if (last != null && last.position >= position) {
                throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                        "process " + processName + ": held messages restored out of position order on "
                                + channel + " (" + last.position + " before " + position
                                + "); the store's scan broke its ordering contract");
            }
            buffer.addLast(new Hold(channel, position, blob.timestamp(), null, true, null, null, null));
        });

        for (ChannelId channel : this.receivedChannels) {
            Long past = deliveredPast.get(channel);
            if (past != null) {
                advanceFedUpTo(channel, past);
            }
        }
        // The host's start position is the one position it reports (Host obligation 2):
        // everything below it was fed and committed by an earlier execution, or was skipped
        // by the initial position the process was started at. Raising coverage to just
        // below it is Structural 12's baseline, taken here so that it lies within the
        // session floor: a feed below the start position is the host re-feeding a
        // committed past, a replay to drop, never a contradiction (D10, D115).
        startPositions.forEach((channel, start) -> {
            if (this.receivedChannels.contains(channel) && start > 0) {
                advanceFedUpTo(channel, start - 1);
            }
        });
        this.sessionFloor = Map.copyOf(fedUpTo);
    }

    /**
     * Refuses a restored position no channel can assign (D105): the reserved maximum, which
     * the engine uses in-band as its fed-to-end marker, and any negative value. The maximum
     * can only have entered the store through a forged header absorbed before receipt
     * refused it; a negative row is store corruption, since receipt has always refused
     * negative positions. Restored into the frontier either would be re-expressed on every
     * emission — the emission path encodes the frontier directly and validates nothing, so
     * this is the one check between the store and the wire — and restored into the
     * delivered past the maximum would reach {@code fedUpTo} through the join clamp and
     * read as "this channel no longer exists" for a topic that is alive.
     */
    private long requireAssignablePosition(ChannelId channel, long position, String what) {
        if (position == Long.MAX_VALUE) {
            throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "process " + processName + ": restored " + what + " names position " + position + " on "
                            + channel + ", which no channel can assign; this state absorbed a forged causes"
                            + " header before receipt refused it (wire-format constraint 7). Reset the process's"
                            + " state and group offsets deliberately to proceed.");
        }
        if (position < 0) {
            throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "process " + processName + ": restored " + what + " names position " + position + " on "
                            + channel + ", which no channel can assign; receipt refuses negative positions, so"
                            + " this row is corrupt ordering state. Reset the process's state and group offsets"
                            + " deliberately to proceed.");
        }
        return position;
    }

    /**
     * Stops construction if the store holds ordering state without its version entry.
     *
     * <p>The version entry is written before any state, in the store's earliest transaction,
     * so state without it is unambiguous evidence that the head of the changelog has been
     * lost — and with it an unknowable amount of the state itself. Stamping a fresh version
     * here would adopt the remainder as complete and silently under-express causes.
     */
    private void refuseUnversionedState() {
        for (byte tag : StoreCodec.stateTags()) {
            store.scanPrefix(StoreCodec.tagPrefix(tag), (key, value) -> {
                throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                        "process " + processName + ": ordering state present without its format version entry;"
                                + " the earliest records of the ordering changelog have been lost, so this state"
                                + " is incomplete and cannot be trusted. Check the changelog topic's"
                                + " cleanup.policy, then reset the process's state and group offsets"
                                + " deliberately to proceed.");
            });
        }
    }

    /**
     * Returns the channels this process receives, in {@link ChannelId} order.
     *
     * @return the channels this process receives, in {@link ChannelId} order
     */
    public Set<ChannelId> receivedChannelSet() {
        return Collections.unmodifiableSet(receivedChannels);
    }

    /**
     * Exposes settled positions to the delivery decision.
     *
     * @return how far each channel has settled, for {@link Deliverability#decide}
     */
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
     * Takes one message from the host.
     *
     * <p>Causes carried by a message are merged into the frontier on receipt, before any
     * question of delivering it. Happened-before passes through receipt, so a message this
     * process has seen but not yet delivered still constrains what it may send.
     *
     * @param message the message, whose position must exceed any already fed for its channel
     * @return whether the message was accepted or recognised as already delivered
     * @throws ParsleyFailClosedException if the host fed out of order within this execution,
     *         if the message's position lies above the session floor yet inside coverage
     *         this execution already recorded as fed or never arriving — a contradiction
     *         between the host's feed and the engine's own record, kept as an invariant
     *         guard though no path in this tree reaches it (D115) — if the metadata cannot
     *         be decoded, or if the metadata exceeds the configured budget
     */
    public ReceiveOutcome onReceive(ReceivedMessage message) {
        ChannelId channel = message.channel();
        if (!receivedChannels.contains(channel)) {
            throw new IllegalArgumentException(
                    "process " + processName + " received on undeclared channel " + channel);
        }

        Causes causes = extractCauses(message);

        Long fedBefore = fedThisExecution.get(channel);
        if (fedBefore != null && message.position() <= fedBefore) {
            throw new ParsleyFailClosedException(Reason.OUT_OF_ORDER_FEED,
                    "process " + processName + ": fed " + channel + "@" + message.position()
                            + " after this execution was already fed position " + fedBefore
                            + " of the same channel; the host must feed each channel in increasing position order");
        }
        fedThisExecution.put(channel, message.position());

        if (sabotage.has(Sabotage.Mode.SILENT_DROP) && message.position() == 3) {
            return ReceiveOutcome.DUPLICATE_DROPPED;
        }

        if (!sabotage.has(Sabotage.Mode.SKIP_RECEIPT_MERGE)) {
            causes.byChannel().forEach(this::mergeFrontier);
        }

        Long fed = fedUpTo.get(channel);
        if (fed != null && message.position() <= fed && !sabotage.has(Sabotage.Mode.REDELIVER_REFEEDS)) {
            if (fed == FED_TO_END_OF_CHANNEL) {
                throw new ParsleyFailClosedException(Reason.OUT_OF_ORDER_FEED,
                        "process " + processName + ": fed " + channel + "@" + message.position()
                                + " on a channel recorded as no longer existing");
            }
            Long floor = sessionFloor.get(channel);
            if (floor == null || message.position() > floor) {
                // Not a feed-order violation: in-execution order is checked against
                // fedThisExecution above. Coverage above the session floor is only ever
                // raised by this execution's own receipts, which fedThisExecution already
                // guards, so this branch is an invariant guard with no known trigger (D115):
                // it is kept so that a contradiction between the host's feed and the
                // engine's record can never fall through to a silent drop or a delivery.
                throw new ParsleyFailClosedException(Reason.COVERED_POSITION_FED,
                        "process " + processName + ": fed " + channel + "@" + message.position()
                                + " which this execution's own coverage already records as fed or never"
                                + " arriving (fedUpTo=" + fed + "), above the session floor of " + floor
                                + "; the host's feed and the engine's record contradict each other."
                                + " A restart resumes from the committed record, and this refusal then"
                                + " does not recur.");
            }

            return ReceiveOutcome.DUPLICATE_DROPPED;
        }

        advanceFedUpTo(channel, message.position());
        Hold hold = new Hold(channel, message.position(), message.timestamp(), causes, false,
                message.key(), message.value(), message.headers());
        held.computeIfAbsent(channel, c -> new ArrayDeque<>()).addLast(hold);
        unpersisted.addLast(hold);
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
            return Causes.none();
        }
        if (headerValue != null && headerValue.length > metadataBudgetBytes) {
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
     * Takes what the host learned of channel identity when it initialised this process.
     *
     * <p>A received channel whose topic was deleted settles to its end: nothing more can
     * arrive on it, so every cause on it is satisfied (D21) — unless messages from it are
     * still held, whose place in causal order can then no longer be preserved, which refuses
     * (SPEC Safety 9, D46). A received channel whose topic was recreated under its name
     * refuses: records fed under the old identity can no longer be trusted (SPEC Assumption
     * 2). Frontier and delivered-past entries for dead and recreated channels are pruned —
     * the one discarding Structural 13 permits, since a cause on a channel that no longer
     * exists can no longer matter. Nothing is pruned by retention: a cause names a message
     * that was sent, its holder keeps it until its own causes arrive, and its senders keep
     * expressing it, so retention crossing a held message costs nothing but the message's
     * copy on its topic (D115 supersedes D104).
     *
     * @param report the host's identity report
     * @throws ParsleyFailClosedException if a received topic was recreated under its name,
     *         or deleted while messages from it remain held
     */
    public void onIdentityReport(IdentityReport report) {
        if (!sabotage.has(Sabotage.Mode.IGNORE_RECREATION)) {
            for (ChannelId channel : report.recreatedChannels()) {
                if (receivedChannels.contains(channel)) {
                    throw new ParsleyFailClosedException(Reason.CHANNEL_IDENTITY_CHANGED,
                            "process " + processName + ": the topic of received channel " + channel
                                    + " was deleted and recreated under the same name while this process ran;"
                                    + " records fed under the old identity can no longer be trusted. Reset the"
                                    + " process's state and group offsets deliberately to proceed.");
                }
            }
        }
        for (ChannelId channel : report.deadChannels()) {
            if (receivedChannels.contains(channel)) {
                ArrayDeque<Hold> channelHeld = held.get(channel);
                if (channelHeld != null && !channelHeld.isEmpty() && !sabotage.has(Sabotage.Mode.DELIVER_PAST_DEAD_HOLDS)) {
                    throw new ParsleyFailClosedException(Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES,
                            "process " + processName + ": " + channelHeld.size() + " received message(s) from "
                                    + channel + " remain undelivered but the channel's topic no longer exists;"
                                    + " their place in causal order can no longer be preserved (SPEC Safety 9)."
                                    + " The deletion breached the deletion-hygiene assumption (SPEC Assumption 17);"
                                    + " an operator must reset this process's state deliberately to proceed.");
                }

                advanceFedUpTo(channel, FED_TO_END_OF_CHANNEL);
            }
        }

        List<ChannelId> prune = null;
        for (ChannelId channel : frontier.keySet()) {
            if (report.deadChannels().contains(channel) || report.recreatedChannels().contains(channel)) {
                if (prune == null) {
                    prune = new ArrayList<>();
                }
                prune.add(channel);
            }
        }
        if (prune != null) {
            for (ChannelId channel : prune) {
                frontier.remove(channel);
                frontierSizeRemove(channel);
                store.delete(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, channel));
            }
            encodedFrontier = null;
        }

        List<ChannelId> pastPrune = null;
        for (ChannelId channel : deliveredPast.keySet()) {
            if (report.deadChannels().contains(channel) || report.recreatedChannels().contains(channel)) {
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
            encodedFrontier = null;
            if (current == null) {
                frontierSizeAdd(channel);
            }
            store.put(StoreCodec.channelKey(StoreCodec.TAG_FRONTIER, channel), StoreCodec.encodeLong(position));
        }

        if (frontierBytes() > metadataBudgetBytes) {
            throw new ParsleyFailClosedException(Reason.METADATA_BUDGET_EXCEEDED,
                    "process " + processName + ": the causal frontier reached " + frontierBytes()
                            + " bytes (" + frontier.size() + " channels); the configured budget is "
                            + metadataBudgetBytes + " bytes. The frontier's growth law is documented in"
                            + " docs/model.md.");
        }
    }

    private void frontierSizeAdd(ChannelId channel) {
        int count = frontierTopicPartitions.merge(channel.topicId(), 1, Integer::sum);
        frontierBodyBytes += (count == 1
                ? 2 * Long.BYTES + CausesCodec.unsignedVarintSize(1)
                : CausesCodec.unsignedVarintSize(count) - CausesCodec.unsignedVarintSize(count - 1))
                + CausesCodec.unsignedVarintSize(channel.partition()) + Long.BYTES;
    }

    private void frontierSizeRemove(ChannelId channel) {
        int count = frontierTopicPartitions.merge(channel.topicId(), -1, Integer::sum);
        if (count == 0) {
            frontierTopicPartitions.remove(channel.topicId());
        }
        frontierBodyBytes -= (count == 0
                ? 2 * Long.BYTES + CausesCodec.unsignedVarintSize(1)
                : CausesCodec.unsignedVarintSize(count + 1) - CausesCodec.unsignedVarintSize(count))
                + CausesCodec.unsignedVarintSize(channel.partition()) + Long.BYTES;
    }

    /**
     * Offers the next message whose causes are all satisfied.
     *
     * <p>Only the head of each channel's hold-back buffer is considered, which preserves the
     * order the channel itself established. A host calls this repeatedly until it returns
     * empty.
     *
     * @return the next deliverable message, or empty when every channel is blocked or idle
     * @see #markDelivered(ChannelId, long)
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
                        return Optional.of(materialize(hold));
                    }
                }
            } else {
                Hold head = channelHeld.peekFirst();
                if (decide(head, settled)) {
                    return Optional.of(materialize(head));
                }
            }
        }
        return Optional.empty();
    }

    private boolean decide(Hold hold, Deliverability.SettledView settled) {
        return verdict(hold, settled).isDeliverable();
    }

    private Deliverability.Verdict verdict(Hold hold, Deliverability.SettledView settled) {
        Causes causes = sabotage.has(Sabotage.Mode.IGNORE_CAUSES) ? Causes.none() : load(hold).causes;
        return Deliverability.decide(causes, receivedChannels, settled);
    }

    /**
     * Reports the position of the oldest held message on a channel, the one the decision
     * reads.
     *
     * @param channel the channel to report on
     * @return the head's position, or empty when nothing is held on that channel
     */
    public OptionalLong headPosition(ChannelId channel) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        return channelHeld == null || channelHeld.isEmpty()
                ? OptionalLong.empty() : OptionalLong.of(channelHeld.peekFirst().position);
    }

    /**
     * Reports the decision for the oldest held message on a channel, as diagnosis: the same
     * verdict {@link #nextDeliverable()} would act on, with every outstanding cause named
     * when it is held.
     *
     * @param channel the channel to report on
     * @return the head's verdict, or empty when nothing is held on that channel
     */
    public Optional<Deliverability.Verdict> headVerdict(ChannelId channel) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        if (channelHeld == null || channelHeld.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(verdict(channelHeld.peekFirst(), settledView()));
    }

    /**
     * Brings a hold's decoded form into memory from the store, where a flush or a restore
     * left it (D102). A hold that is still in memory is returned as it is.
     *
     * @throws ParsleyFailClosedException if the store no longer holds the message: the
     *         buffer says it is held, so ordering state contradicts itself
     */
    private Hold load(Hold hold) {
        if (hold.decoded()) {
            return hold;
        }
        byte[] blob = store.get(StoreCodec.heldKey(hold.channel, hold.position));
        if (blob == null) {
            throw new ParsleyFailClosedException(Reason.UNKNOWN_ORDERING_STATE_FORMAT,
                    "process " + processName + ": held message " + hold.channel + "@" + hold.position
                            + " is in the hold-back buffer but absent from the store");
        }
        StoreCodec.HeldBlob decoded = StoreCodec.decodeHeld(blob);
        hold.key = decoded.key();
        hold.value = decoded.value();
        hold.headers = decoded.headers();
        hold.causes = decoded.causes();
        return hold;
    }

    private DeliverableMessage materialize(Hold hold) {
        load(hold);
        return new DeliverableMessage(
                hold.channel, hold.position, hold.timestamp, hold.key, hold.value, hold.headers, hold.causes);
    }

    /**
     * Records that a message offered by {@link #nextDeliverable()} was delivered.
     *
     * <p>Delivery advances the frontier with the delivered position, and advances the
     * delivered causal past with the delivered message's causes, which is the clamp a
     * channel joining the received set later must start above. The delivered position itself
     * is not written to the delivered past: for a received channel {@code fedUpTo} already
     * covers it, was advanced at receipt, and is never pruned, so the clamp — a maximum over
     * both — could never be raised by it (D106 closes D67's first gap on this basis).
     *
     * @param channel  the channel the message arrived on
     * @param position its position within that channel
     * @throws IllegalStateException if {@code position} is not the head of that channel's
     *         hold-back buffer
     */
    public void markDelivered(ChannelId channel, long position) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        Hold head = channelHeld == null ? null : channelHeld.peekFirst();
        if (head == null || (head.position != position && !sabotage.has(Sabotage.Mode.NO_FIFO))) {
            throw new IllegalStateException("process " + processName + ": markDelivered(" + channel + "@" + position
                    + ") is not the head of the hold-back buffer");
        }
        Hold delivered = null;
        if (head.position == position) {
            delivered = channelHeld.pollFirst();
        } else {
            for (Iterator<Hold> holds = channelHeld.iterator(); holds.hasNext();) {
                Hold hold = holds.next();
                if (hold.position == position) {
                    holds.remove();
                    delivered = hold;
                    break;
                }
            }
        }
        if (delivered != null) {
            // Its causes are needed below, and they may live only in the store: read them
            // before the entry goes.
            load(delivered);
            delivered.removed = true;
        }
        if (delivered == null || delivered.persisted) {
            store.delete(StoreCodec.heldKey(channel, position));
        }
        mergeFrontier(channel, position);

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

    /**
     * The metadata every message sent by this step must carry.
     *
     * @return the encoded frontier, for {@link CausesCodec#HEADER_KEY}
     * @throws ParsleyFailClosedException if the encoded frontier exceeds the configured
     *         budget, which stops the process rather than letting the message meet the
     *         broker's record-size limit with no diagnosis
     */
    public byte[] causesHeaderForEmission() {
        byte[] encoded;
        if (sabotage.has(Sabotage.Mode.OVEREXPRESS)) {
            encoded = CausesCodec.encode(frontierSnapshot());
        } else {
            if (encodedFrontier == null) {
                encodedFrontier = CausesCodec.encode(frontier);
            }
            encoded = encodedFrontier;
        }
        if (encoded.length > metadataBudgetBytes) {
            throw new ParsleyFailClosedException(Reason.METADATA_BUDGET_EXCEEDED,
                    "process " + processName + ": expressing the causal frontier needs " + encoded.length
                            + " bytes (" + frontier.size() + " channels); the configured budget is "
                            + metadataBudgetBytes + " bytes. The frontier's growth law is documented in"
                            + " docs/model.md.");
        }
        // A copy per emission: the cached bytes are this engine's, and a header handed to a
        // host is the host's to keep or alter.
        return encoded.clone();
    }

    /**
     * Returns how many channels the frontier names.
     *
     * @return how many channels the frontier names
     */
    public int frontierSize() {
        return frontier.size();
    }

    /**
     * Returns the encoded width of the frontier, in bytes.
     *
     * <p>Maintained incrementally at the frontier's mutation sites, so the per-merge budget
     * check stays O(1) even though encoded size is a function of the frontier's shape.
     *
     * @return the encoded width of the frontier, in bytes
     */
    public int frontierBytes() {
        return 1 + CausesCodec.unsignedVarintSize(frontierTopicPartitions.size()) + frontierBodyBytes;
    }

    /**
     * Returns the frontier as it stands, for diagnostics and tests.
     *
     * @return the frontier as it stands, for diagnostics and tests
     */
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
     * Persists every held message not yet written.
     *
     * <p>A host calls this before committing, so that a message held at the moment of a crash
     * is still held after the restart.
     */
    public void flushHolds() {
        if (sabotage.has(Sabotage.Mode.DROP_HELD)) {
            unpersisted.clear();
            return;
        }
        Hold hold;
        while ((hold = unpersisted.pollFirst()) != null) {
            if (hold.removed) {
                // Delivered within the step that received it: it was never in the store
                // and must not enter it now.
                continue;
            }
            store.put(StoreCodec.heldKey(hold.channel, hold.position),
                    StoreCodec.encodeHeld(hold.timestamp, hold.key, hold.value, hold.headers, hold.causes));
            hold.persisted = true;
            if (held.get(hold.channel).peekFirst() != hold) {
                // Only the head is read before it is delivered; everything behind it waits
                // in the store and is decoded again on reaching the head (D102).
                hold.key = null;
                hold.value = null;
                hold.headers = null;
                hold.causes = null;
            }
        }
    }

    /**
     * How many holds carry their decoded form in memory: at most the unflushed ones plus
     * one per channel with a non-empty buffer (D102). For tests pinning that rule.
     */
    int decodedHoldCount() {
        int decoded = 0;
        for (ArrayDeque<Hold> channelHeld : held.values()) {
            for (Hold hold : channelHeld) {
                if (hold.decoded()) {
                    decoded++;
                }
            }
        }
        return decoded;
    }

    /**
     * Reports how far one channel has been fed.
     *
     * @param channel the channel to report on
     * @return the highest position fed for that channel, or empty when none has been
     */
    public OptionalLong fedUpTo(ChannelId channel) {
        Long fed = fedUpTo.get(channel);
        return fed == null ? OptionalLong.empty() : OptionalLong.of(fed);
    }

    /**
     * Reports the depth of one channel's hold-back buffer.
     *
     * @param channel the channel to report on
     * @return how many messages are held on that channel
     */
    public int heldCount(ChannelId channel) {
        ArrayDeque<Hold> channelHeld = held.get(channel);
        return channelHeld == null ? 0 : channelHeld.size();
    }

    /**
     * Returns how many messages are held across every channel.
     *
     * @return how many messages are held across every channel
     */
    public int heldCountTotal() {
        return held.values().stream().mapToInt(ArrayDeque::size).sum();
    }
}
