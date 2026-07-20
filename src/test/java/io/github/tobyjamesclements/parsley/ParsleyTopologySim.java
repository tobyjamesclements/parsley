package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An in-memory multi-node topology simulator — the T2.4 property harness. Each node is a real
 * {@link ParsleyChannels} + {@link ParsleyCausalBroadcast} pair over store-backed persistence
 * (restarts are genuine reconstructions from the {@code "f"} blob and the durable buffer /
 * candidate / forwarded-index stores), driven through the production entry points:
 * {@code receive} for business records, {@code ParsleyGossip.receive} for null messages, {@code broadcast}
 * for every stamped emission, {@code rescope} at every (re)start, and the
 * {@code bindOwnOutputSource} seams for producer acks and the crossing wait. Topics are
 * single-partition append logs; the scheduler interleaves per-node channel polls, ack arrivals,
 * external production, and restarts under a seeded {@link Random}, so each seed is one
 * deterministic, reproducible interleaving.
 *
 * <p><strong>Ground truth.</strong> Alongside the protocol state the sim tracks, per record, the
 * exact <em>delegate-visible causal history</em> as a set of business coordinates (Schwarz–Mattern
 * causal histories, restricted to information flow: what the emitting task's delegate had actually
 * seen): a delivered business record contributes its own coordinate plus its history; a delivered
 * null message contributes nothing (its carried clock is knowledge transfer — it feeds stamps,
 * never obligations — and no delegate anywhere observes its delivery). Own business sends are part
 * of the sender's subsequent histories (process order). Against this ground truth the sim asserts,
 * continuously:
 *
 * <ul>
 *   <li><strong>I2 (both forms).</strong> At every emission the attached stamp dominates (a) the
 *       merge of the raw dependency clocks and coordinates of everything this node has delivered —
 *       the doc's literal I2 — and (b) the node's ground-truth causal past, which is the form the
 *       D1/D7 transitivity proof actually quantifies over.</li>
 *   <li><strong>I3.</strong> A node's successive stamps are vector-monotone (which implies
 *       per-partition monotonicity for every destination).</li>
 *   <li><strong>I9.</strong> Ground-truth dominance is asserted at nodes whose consumption sets
 *       differ — ancestry on channels a node does not consume must still be claimed by its stamps
 *       (the custody chain), fed to it either by carried clocks or by carried ancestry.</li>
 *   <li><strong>Causal delivery order (I1 over ground truth).</strong> At every business delivery,
 *       every consumed-topic coordinate in the record's true history has already been delivered
 *       at this node — the end-to-end no-effect-before-cause check.</li>
 *   <li><strong>Own-output coverage (D2).</strong> A business emission's stamp claims every one of
 *       the node's prior business sends (the crossing wait forces their acks); any emission's
 *       stamp claims at least all acked sends.</li>
 * </ul>
 *
 * <p>Business consumers' scopes may genuinely differ (T3.1, D1): a record whose clock names
 * topics outside the receiving node's consumption scope is handled by the two-branch gate —
 * consumed coordinates gate on the node's own frontier, everything else is ignored — so
 * differing-scope chains exercise the ignore branch end to end, and unconsumed-channel custody
 * (I9) is additionally exercised through null-message carried clocks and rescope carried
 * ancestry.
 *
 * <p>Deliberate simplifications, each an explicit non-goal here: restarts are clean (pending sends
 * ack before the store reconstruct — mid-transaction crash tears are broker-IT territory,
 * {@code ParsleyProducerAckMechanicsIT}); topics are single-partition (the cross-partition funnel
 * is T2.3's wire IT and T3.1's delivery IT); there is no retention, so {@code seedIfFirstSeen}'s
 * expired-prefix path is out of scope (E2 is enforced elsewhere).
 */
final class ParsleyTopologySim {

    private static final int PARTITION = 0;

    /** One business coordinate — the unit of the ground-truth history sets. */
    record SimCoord(Uuid topicId, long offset) {}

    /** One appended record on a simulated topic-partition log. */
    record SimRecord(String topic, Uuid topicId, long offset, String value, ParsleyVectorClock stamp,
                     boolean nullMessage, Set<SimCoord> causalHistory) {}

    private final class SimTopic {
        final String name;
        final Uuid id = Uuid.randomUuid();
        final List<SimRecord> log = new ArrayList<>();

        SimTopic(String name) {
            this.name = name;
        }
    }

    /** The hand-rolled producer-side double: acked maxima + pending sends per destination. */
    private final class SimProducer implements ParsleyChannels.AckedOutputs, ParsleyChannels.PendingSends {
        final Map<String, Long> ackedMax = new HashMap<>();
        final Map<TopicPartition, Long> pendingMax = new HashMap<>();

        @Override
        public void forEachAcked(Consumer consumer) {
            ackedMax.forEach((topic, offset) -> consumer.accept(topic, PARTITION, offset));
        }

        @Override
        public void awaitQuiescentExcept(Set<TopicPartition> exceptDestinations, long timeoutMs) {
            // The single-threaded stand-in for the crossing wait: by the time the real wait
            // returns, every non-excepted pending send has been acknowledged — so ack them now.
            ackAllExcept(exceptDestinations);
        }

        void ackAllExcept(Set<TopicPartition> except) {
            pendingMax.entrySet().removeIf(entry -> {
                if (except.contains(entry.getKey())) {
                    return false;
                }
                ackedMax.merge(entry.getKey().topic(), entry.getValue(), Math::max);
                return true;
            });
        }
    }

    final class SimNode {
        final String name;
        Set<String> inputs;
        final List<String> sinks;
        final double businessOutputProbability;

        // Durable state — survives restarts; the live instances below are rebuilt from it.
        private final TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        private final TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        private final TestKeyValueStore<byte[], byte[]> candidateStore =
                new TestKeyValueStore<>(Arrays::compareUnsigned, "candidates");
        private final TestKeyValueStore<byte[], byte[]> forwardedStore =
                new TestKeyValueStore<>(Arrays::compareUnsigned, "forwarded");
        final Map<String, Integer> cursors = new HashMap<>();

        ParsleyChannels channels;
        ParsleyCausalBroadcast<String, String> core;
        ParsleyGossip<String, String> gossip;
        SimProducer producer = new SimProducer();

        // Ground truth (logical node state — survives restarts, exactly like the durable stores).
        final Set<SimCoord> delivered = new HashSet<>();
        final Set<SimCoord> truePast = new HashSet<>();
        final Set<SimCoord> ownBusinessSends = new HashSet<>();
        ParsleyVectorClock obligation = ParsleyVectorClock.empty();
        ParsleyVectorClock lastStamp = ParsleyVectorClock.empty();
        // The frontier as of the most recent (re)start: everything at or below it is causal past
        // this node has either genuinely delivered (also in `delivered`) or deliberately skipped —
        // a scope-growth seed's prefix (A5, "skip what you already ignored/claimed"). The
        // ground-truth order check exempts causes at or below it: skipped past is never
        // re-entered, so its absence from `delivered` is by design, not a reorder.
        ParsleyVectorClock frontierAtStart = ParsleyVectorClock.empty();
        final Map<String, List<Long>> deliveredOffsetsByTopic = new HashMap<>();
        int carriedClocksFolded;

        private SimNode(String name, Set<String> inputs, List<String> sinks, double businessOutputProbability) {
            this.name = name;
            this.inputs = Set.copyOf(inputs);
            this.sinks = List.copyOf(sinks);
            this.businessOutputProbability = businessOutputProbability;
            inputs.forEach(input -> cursors.put(input, 0));
            start(this.inputs);
        }

        /**
         * (Re)builds the live protocol instances from the durable stores — the production init
         * sequence: channels restore (with the forwarded-index reconstruction), {@code rescope}
         * against the declared inputs, sink end-offset seeding of {@code ownOutputs}, producer
         * seam binding, then the broadcast core's own restore pass over the buffer.
         */
        private void start(Set<String> declaredInputs) {
            Map<String, Uuid> inputIds = new HashMap<>();
            declaredInputs.forEach(input -> inputIds.put(input, topic(input).id));
            channels = new ParsleyChannels(frontierStore, new StoreBackedForwardedIndex(forwardedStore));
            channels.rescope(inputIds, PARTITION);
            Map<String, Uuid> sinkIds = new HashMap<>();
            for (String sink : sinks) {
                SimTopic topic = topic(sink);
                sinkIds.put(sink, topic.id);
                if (!topic.log.isEmpty()) {
                    channels.acknowledge(topic.id, PARTITION, topic.log.size() - 1);
                }
            }
            channels.bindOwnOutputSource(producer, producer, sinkIds, 60_000L);
            Set<Uuid> inputUuids = new HashSet<>(inputIds.values());
            Set<Uuid> sinkUuids = new HashSet<>(sinkIds.values());
            core = new ParsleyCausalBroadcast<>(channels,
                    new StoreBackedBufferStore<>(bufferStore,
                            new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()))),
                    new StoreBackedCandidateIndex(candidateStore),
                    ParsleyMetrics.NOOP, () -> ++simTimeMs,
                    (topicId, partition) -> partition == PARTITION && inputUuids.contains(topicId),
                    (topicId, partition) -> sinkUuids.contains(topicId));
            gossip = new ParsleyGossip<>(channels, core, Set.of());
            frontierAtStart = channels.frontier();
        }

        boolean hasPollableInput() {
            return inputs.stream().anyMatch(input -> cursors.get(input) < topic(input).log.size());
        }
    }

    private final Map<String, SimTopic> topics = new LinkedHashMap<>();
    private final Map<Uuid, String> topicNamesById = new HashMap<>();
    private final Map<String, SimNode> nodes = new LinkedHashMap<>();
    private final List<String> externalTopics = new ArrayList<>();
    private final Random random;
    private final String runLabel;
    private long simTimeMs;
    private long externalSequence;
    private boolean restartsEnabled;

    // Vacuity counters — property tests assert these to prove the paths were genuinely exercised.
    int businessDeliveries;
    int recordsHeld;
    int nullMessagesDelivered;
    int outOfOrderDeliveries;
    int emissions;
    int restarts;

    ParsleyTopologySim(long seed) {
        this.random = new Random(seed);
        this.runLabel = "[seed " + seed + "] ";
    }

    // --- topology construction -----------------------------------------------------------------

    /** Declares a topic fed only by an external, clockless (plain-Kafka) producer. */
    ParsleyTopologySim externalTopic(String name) {
        topic(name);
        externalTopics.add(name);
        return this;
    }

    /** Adds a node; {@code businessOutputProbability} is the delegate's chance of forwarding per delivery. */
    SimNode node(String name, Set<String> inputs, List<String> sinks, double businessOutputProbability) {
        SimNode node = new SimNode(name, inputs, sinks, businessOutputProbability);
        nodes.put(name, node);
        return node;
    }

    ParsleyTopologySim withRestarts() {
        this.restartsEnabled = true;
        return this;
    }

    SimNode nodeNamed(String name) {
        SimNode node = nodes.get(name);
        if (node == null) {
            throw new IllegalArgumentException("no node named " + name);
        }
        return node;
    }

    private SimTopic topic(String name) {
        return topics.computeIfAbsent(name, n -> {
            SimTopic created = new SimTopic(n);
            topicNamesById.put(created.id, n);
            return created;
        });
    }

    /**
     * Fails fast (with the seed) on a runaway emission loop — a supercritical cycle (a node whose
     * own sink feeds back at output probability ~1) would otherwise grow a log without bound and
     * OOM the run instead of naming the misconfigured topology.
     */
    private void guardRunaway(SimTopic sink) {
        if (sink.log.size() > 20_000) {
            throw new AssertionError(runLabel + "runaway emission loop: topic " + sink.name
                    + " exceeded 20000 records — a cyclic node's output probability must be subcritical");
        }
    }

    // --- the scheduler -------------------------------------------------------------------------

    /** Runs {@code steps} random scheduler steps, draining all remaining channel backlog at the end. */
    void run(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
        drain();
    }

    private void step() {
        int action = random.nextInt(10);
        if (action == 0 && !externalTopics.isEmpty()) {
            produceExternal(externalTopics.get(random.nextInt(externalTopics.size())));
            return;
        }
        if (action == 1) {
            SimNode node = randomNode();
            node.producer.ackAllExcept(Set.of());
            return;
        }
        if (action == 2 && restartsEnabled) {
            restart(randomNode().name);
            return;
        }
        pollOnce(randomNode());
    }

    /** Polls every node until no channel has undelivered backlog — lets every seed end settled. */
    void drain() {
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (SimNode node : nodes.values()) {
                node.producer.ackAllExcept(Set.of());
                while (node.hasPollableInput()) {
                    pollOnce(node);
                    progressed = true;
                }
            }
        }
    }

    void produceExternal(String topicName) {
        SimTopic topic = topic(topicName);
        topic.log.add(new SimRecord(topic.name, topic.id, topic.log.size(),
                "ext-" + externalSequence++, ParsleyVectorClock.empty(), false, Set.of()));
    }

    /** Restarts a node in place with its current input set (a clean stop + store-backed rebuild). */
    void restart(String nodeName) {
        restartWithInputs(nodeName, nodeNamed(nodeName).inputs);
    }

    /**
     * Restarts a node with a (possibly different) declared input set — the A5/A6 scope-change
     * path. A newly added input's cursor starts at log-start, exactly like a consumer group with
     * no committed offset for it; the receive-path skip guard is what must keep the carried
     * prefix out of the delegate.
     */
    void restartWithInputs(String nodeName, Set<String> newInputs) {
        SimNode node = nodeNamed(nodeName);
        // Clean close: every pending send acks and folds into the persisted blob before the stores
        // are re-read (mid-transaction crash tears are broker-IT territory, not this harness's).
        node.producer.ackAllExcept(Set.of());
        node.channels.foldAcknowledgedOutputs();
        // A fresh producer: in-memory ack state does not survive a restart; the init-time sink
        // end-offset seed re-covers everything previously acked (I8).
        node.producer = new SimProducer();
        node.inputs = Set.copyOf(newInputs);
        // An added input has no committed consumer offset — it re-fetches from log-start, and the
        // receive-path skip guard (not the cursor) must keep the carried prefix from the delegate.
        newInputs.forEach(input -> node.cursors.putIfAbsent(input, 0));
        node.start(node.inputs);
        restarts++;
    }

    private SimNode randomNode() {
        List<SimNode> all = new ArrayList<>(nodes.values());
        return all.get(random.nextInt(all.size()));
    }

    private void pollOnce(SimNode node) {
        List<String> pollable = node.inputs.stream()
                .filter(input -> node.cursors.get(input) < topic(input).log.size())
                .toList();
        if (pollable.isEmpty()) {
            return;
        }
        String input = pollable.get(random.nextInt(pollable.size()));
        SimTopic topic = topic(input);
        SimRecord record = topic.log.get(node.cursors.merge(input, 1, Integer::sum) - 1);
        if (record.nullMessage()) {
            deliverNullMessage(node, record);
        } else {
            deliverBusiness(node, record);
        }
    }

    // --- the receive paths ---------------------------------------------------------------------

    private void deliverBusiness(SimNode node, SimRecord record) {
        ParsleyVectorClock completenessBefore = node.core.completeness();
        ParsleyCausalBroadcast.Outcome<String, String> outcome = node.core.receive(new ParsleyMessage<>(
                record.topic(), record.topicId(), PARTITION, record.offset(), 0L,
                "k", record.value(), List.of(), record.stamp()));
        if (outcome.delivered().isEmpty()) {
            recordsHeld++;
        }
        boolean anyBusinessOutput = processDeliveries(node, outcome.delivered());
        // The production heartbeat: consumed-but-buffered (or delivered with a silent delegate)
        // still advertises genuinely advanced completeness downstream via a null message.
        if (!anyBusinessOutput && !node.core.completeness().equals(completenessBefore)) {
            emitNullMessage(node);
        }
    }

    private void deliverNullMessage(SimNode node, SimRecord record) {
        ParsleyGossip.Reception<String, String> outcome =
                node.gossip.receive(record.topicId(), PARTITION, record.offset(), record.stamp());
        nullMessagesDelivered++;
        node.carriedClocksFolded++;
        // The carried clock folds into the stamp (knowledge), so the stamp must dominate it from
        // now on — but it creates no delegate-visible obligation (no truePast contribution).
        node.obligation = node.obligation.merge(record.stamp());
        boolean anyBusinessOutput = processDeliveries(node, outcome.delivered());
        // The I6 relay (strictly new knowledge propagates onward), and the silent-delegate
        // advertisement for any released records — at most one null message per poll.
        if (outcome.learnedSomethingNew() || (!anyBusinessOutput && !outcome.delivered().isEmpty())) {
            emitNullMessage(node);
        }
    }

    /** Delivers each released record to the ground-truth delegate; returns whether any business output was emitted. */
    private boolean processDeliveries(SimNode node, List<ParsleyMessage<String, String>> released) {
        boolean anyBusinessOutput = false;
        for (ParsleyMessage<String, String> message : released) {
            SimRecord record = topic(message.topic()).log.get((int) message.offset());
            assertCausalDeliveryOrder(node, record);
            SimCoord coord = new SimCoord(record.topicId(), record.offset());
            node.delivered.add(coord);
            node.truePast.add(coord);
            node.truePast.addAll(record.causalHistory());
            node.obligation = node.obligation
                    .merge(record.stamp())
                    .observe(record.topicId(), PARTITION, record.offset());
            node.deliveredOffsetsByTopic.computeIfAbsent(record.topic(), t -> new ArrayList<>()).add(record.offset());
            businessDeliveries++;
            if (record.offset() > node.channels.frontier().offsetFor(record.topicId(), PARTITION)) {
                outOfOrderDeliveries++;
            }
            if (!node.sinks.isEmpty() && random.nextDouble() < node.businessOutputProbability) {
                emitBusiness(node, node.sinks.get(random.nextInt(node.sinks.size())), record.value());
                anyBusinessOutput = true;
            }
        }
        return anyBusinessOutput;
    }

    /**
     * The end-to-end no-effect-before-cause check: every consumed-topic coordinate in the true
     * history of a record being delivered must already have been delivered here — own-sink topics
     * this node also consumes included (the two-branch gate genuinely gates a self-consumer's
     * claims about its own sink; the interim strip that once vacuously satisfied them died at
     * T3.1). Causes at or below the node's frontier as of its most recent (re)start are exempt:
     * that prefix is causal past the node either delivered in an earlier incarnation (then it is
     * in {@code delivered} anyway) or deliberately skipped at a scope-growth seed (A5) — skipped
     * past is never re-entered, so its absence from {@code delivered} is by design.
     */
    private void assertCausalDeliveryOrder(SimNode node, SimRecord record) {
        for (SimCoord cause : record.causalHistory()) {
            String causeTopic = topicNamesById.get(cause.topicId());
            if (!node.inputs.contains(causeTopic)) {
                continue;
            }
            if (cause.offset() <= node.frontierAtStart.offsetFor(cause.topicId(), PARTITION)) {
                continue; // delivered-or-skipped causal past as of the last (re)start — see Javadoc.
            }
            assertTrue(node.delivered.contains(cause),
                    runLabel + node.name + ": delivered " + record.topic() + "@" + record.offset()
                            + " before its consumed cause " + causeTopic + "@" + cause.offset()
                            + " — effect before cause at the delegate");
        }
    }

    // --- the emission paths --------------------------------------------------------------------

    private void emitBusiness(SimNode node, String sinkName, String parentValue) {
        Set<SimCoord> history = emissionHistory(node);
        ParsleyVectorClock ownSendsBefore = clockOver(node.ownBusinessSends);
        ParsleyVectorClock stamp = stampThrough(node, Set.of());
        assertTrue(stamp.dominates(ownSendsBefore),
                runLabel + node.name + ": a business emission's stamp must claim every prior own "
                        + "business send — the crossing wait forces their acks before the stamp (D2/O1)");
        SimTopic sink = topic(sinkName);
        guardRunaway(sink);
        long offset = sink.log.size();
        sink.log.add(new SimRecord(sink.name, sink.id, offset, parentValue + ">" + node.name,
                stamp, false, history));
        node.ownBusinessSends.add(new SimCoord(sink.id, offset));
        node.producer.pendingMax.merge(new TopicPartition(sink.name, PARTITION), offset, Math::max);
    }

    private void emitNullMessage(SimNode node) {
        if (node.sinks.isEmpty()) {
            return;
        }
        // Production fans a marker out to every sink at the task's own partition, excluding that
        // exact destination set from the crossing wait (O4's recorded null-message exemption).
        Set<TopicPartition> destinations = new HashSet<>();
        node.sinks.forEach(sink -> destinations.add(new TopicPartition(sink, PARTITION)));
        ParsleyVectorClock stamp = stampThrough(node, destinations);
        for (String sinkName : node.sinks) {
            SimTopic sink = topic(sinkName);
            guardRunaway(sink);
            sink.log.add(new SimRecord(sink.name, sink.id, sink.log.size(), "∅",
                    stamp, true, Set.of()));
            // A null message's own coordinate is protocol bookkeeping, never a delegate-visible
            // cause — it enters neither ownBusinessSends nor any causal history.
        }
    }

    /** Routes an emission through {@code broadcast()} — the single stamping site — and checks I2/I3/I9. */
    private ParsleyVectorClock stampThrough(SimNode node, Set<TopicPartition> destinations) {
        Record<String, String> stamped = node.core.broadcast(new Record<>("k", "v", 0L), destinations);
        ParsleyVectorClock stamp = ParsleyVectorClock.fromBytes(
                stamped.headers().lastHeader(ParsleyHeader.CAUSAL_CLOCK).value());
        emissions++;
        assertTrue(stamp.dominates(node.obligation),
                runLabel + node.name + ": stamp must dominate the dependency clocks and coordinates "
                        + "of everything this node has delivered (I2, doc form)");
        assertClockCovers(stamp, node.truePast, node.name
                + ": stamp must dominate the node's ground-truth causal past (I2 as the D1/D7 proof "
                + "uses it, including unconsumed-channel ancestry — I9)");
        assertClockCovers(stamp, ackedCoords(node), node.name
                + ": stamp must claim every acknowledged own send (the pre-stamp ack fold, D2)");
        assertTrue(stamp.dominates(node.lastStamp),
                runLabel + node.name + ": successive stamps must be vector-monotone (I3)");
        node.lastStamp = stamp;
        return stamp;
    }

    private Set<SimCoord> emissionHistory(SimNode node) {
        Set<SimCoord> history = new HashSet<>(node.truePast);
        history.addAll(node.ownBusinessSends);
        return Set.copyOf(history);
    }

    private Set<SimCoord> ackedCoords(SimNode node) {
        Set<SimCoord> acked = new HashSet<>();
        node.producer.ackedMax.forEach((topicName, offset) -> acked.add(new SimCoord(topic(topicName).id, offset)));
        return acked;
    }

    // --- assertions and helpers ----------------------------------------------------------------

    private void assertClockCovers(ParsleyVectorClock stamp, Set<SimCoord> coords, String message) {
        for (SimCoord coord : coords) {
            assertTrue(stamp.offsetFor(coord.topicId(), PARTITION) >= coord.offset(),
                    runLabel + message + " — missing " + topicNamesById.get(coord.topicId())
                            + "@" + coord.offset());
        }
    }

    private static ParsleyVectorClock clockOver(Set<SimCoord> coords) {
        ParsleyVectorClock clock = ParsleyVectorClock.empty();
        for (SimCoord coord : coords) {
            clock = clock.observe(coord.topicId(), PARTITION, coord.offset());
        }
        return clock;
    }

    Uuid topicId(String name) {
        return topic(name).id;
    }

    int logSize(String topicName) {
        return topic(topicName).log.size();
    }
}
