package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
 * <p><strong>Trace and replay.</strong> In generate mode (the seeded constructors) every scheduler
 * step is recorded into a {@link ParsleySimTrace} action list as it executes — including the
 * settle {@code drain()} and any actions driven directly by a test — with each {@code Poll}
 * carrying the delegate decisions drawn inside it, so an action is self-contained.
 * {@link #replay(ParsleySimTrace.SimSpec, List)} re-executes a recorded (or shrunken — see
 * {@link ParsleySimShrinker}) trace without touching the PRNG at all: sub-decisions come from the
 * recorded {@code Emit}s, an unpollable recorded input is a no-op, and a missing recorded
 * decision means no emission. A full-trace replay reproduces the generate-mode run exactly
 * ({@link #stateDigest()} is the equality witness); divergence is only possible on deliberately
 * edited traces, where it is the shrinker's problem. For cross-JVM seed reproducibility the
 * pollable-input list is sorted: {@code Set} iteration order is salted per JVM and must never
 * reach the schedule.
 *
 * <p><strong>Fault modes</strong>, each off by default (legacy seeds keep their exact schedules —
 * a disabled mode's scheduler slot falls through to a poll, drawing identically): restarts
 * ({@link #withRestarts()}, clean rebuilds), crashes ({@link #withCrashes()} — no final ack, no
 * {@code foldAcknowledgedOutputs}; the I8 end-offset seed must re-cover appended-but-unfolded
 * sends), consumer rollbacks ({@link #withRollbacks()} — at-least-once redelivery, which the
 * protocol must absorb without a duplicate delegate delivery, A11), and random scope changes
 * ({@link #withScopeChanges()} — A5/A6 grow/shrink restarts mid-schedule; a shrink that would
 * orphan held records is skipped, its loud-failure contract being pinned in
 * {@code ParsleyScopeChangePropertyTest}).
 *
 * <p>Deliberate simplifications, each an explicit non-goal here: crash tears are bounded by the
 * sim's instantly-committed stores (a {@code Crash} is the crash-after-commit-before-fold shape;
 * the aborted-transaction tear that rewinds stores to an earlier commit is broker-IT territory,
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
            core = ParsleyTestFixtures.broadcast(channels,
                    new StoreBackedBufferStore<>(bufferStore,
                            new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()))),
                    new StoreBackedCandidateIndex(candidateStore),
                    ParsleyMetrics.NOOP, () -> ++simTimeMs,
                    (topicId, partition) -> partition == PARTITION && inputUuids.contains(topicId),
                    (topicId, partition) -> sinkUuids.contains(topicId));
            gossip = new ParsleyGossip<>(core, Set.of());
            frontierAtStart = channels.frontier();
            // The production init sequence ends with the post-init punctuator's one-shot
            // drainAfterRestore: the full gate re-runs over every restored held record under the
            // NEW consumption set, releasing records whose gating dependencies were satisfied —
            // or left scope — across the restart (a rescope-shrink can turn a consumed-gated
            // dependency into an ignored one, leaving a candidate with no unsatisfied
            // coordinates and therefore no frontier-advance trigger; only this pass can free
            // it). Skipping it stranded such records forever — an explorer-surfaced sim
            // infidelity, not a protocol defect. Restore releases are processed without the
            // delegate's random forward decision so restarts stay PRNG-silent (legacy seeds keep
            // their schedules; replays need no recorded decisions here).
            processDeliveries(SimNode.this, core.drainAfterRestore().delivered(), true);
        }

        boolean hasPollableInput() {
            return inputs.stream().anyMatch(input -> cursors.get(input) < topic(input).log.size());
        }

        /**
         * The source topic names of this node's currently held (buffered, undelivered) records —
         * read straight from the durable buffer store, for the held-record disposition properties.
         */
        Set<String> heldSourceTopics() {
            Set<String> topics = new HashSet<>();
            new StoreBackedBufferStore<>(bufferStore,
                    new ParsleySerializer<String, String>(
                            new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String())))
                    .indexEntries().forEach(entry -> topics.add(entry.topic()));
            return topics;
        }
    }

    private enum Mode { GENERATE, REPLAY }

    private final Map<String, SimTopic> topics = new LinkedHashMap<>();
    private final Map<Uuid, String> topicNamesById = new HashMap<>();
    private final Map<String, SimNode> nodes = new LinkedHashMap<>();
    private final List<String> externalTopics = new ArrayList<>();
    private final Random random;
    private final String runLabel;
    private final Mode mode;
    private final List<ParsleySimTrace.Action> trace = new ArrayList<>();
    private final List<ParsleySimTrace.Emit> currentEmits = new ArrayList<>();
    private @Nullable Iterator<ParsleySimTrace.Emit> replayEmits;
    private long simTimeMs;
    private long externalSequence;
    private boolean traceRecording = true;
    private boolean groundTruthHistories = true;
    private boolean restartsEnabled;
    private boolean crashesEnabled;
    private boolean rollbacksEnabled;
    private boolean scopeChangesEnabled;

    // Vacuity counters — property tests assert these to prove the paths were genuinely exercised.
    int businessDeliveries;
    int recordsHeld;
    int nullMessagesDelivered;
    int outOfOrderDeliveries;
    int emissions;
    int restarts;
    int crashes;
    int rollbacks;
    int rescopes;

    ParsleyTopologySim(long seed) {
        this(seed, Mode.GENERATE, "[seed " + seed + "] ");
    }

    private ParsleyTopologySim(long seed, Mode mode, String runLabel) {
        this.random = new Random(seed);
        this.mode = mode;
        this.runLabel = runLabel;
    }

    /** Builds a sim from a topology-by-value — the entry the generator and the shrinker share. */
    static ParsleyTopologySim fromSpec(ParsleySimTrace.SimSpec spec, long seed) {
        return fromSpec(spec, seed, Mode.GENERATE, "[seed " + seed + "] ");
    }

    /**
     * Re-executes a recorded schedule against a fresh sim built from {@code spec}, never touching
     * the PRNG — see the class Javadoc. Throws whatever the schedule's failing action threw, so a
     * shrinker oracle is just a try/catch around this call.
     */
    static ParsleyTopologySim replay(ParsleySimTrace.SimSpec spec, List<ParsleySimTrace.Action> trace) {
        ParsleyTopologySim sim = fromSpec(spec, 0L, Mode.REPLAY, "[replay] ");
        for (ParsleySimTrace.Action action : trace) {
            sim.execute(action);
        }
        return sim;
    }

    private static ParsleyTopologySim fromSpec(ParsleySimTrace.SimSpec spec, long seed, Mode mode, String label) {
        ParsleyTopologySim sim = new ParsleyTopologySim(seed, mode, label);
        spec.externalTopics().forEach(sim::externalTopic);
        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            sim.node(node.name(), Set.copyOf(node.inputs()), node.sinks(), node.outputProbability());
        }
        return sim;
    }

    /** The schedule recorded so far — complete from construction, including drain() polls. */
    List<ParsleySimTrace.Action> trace() {
        return List.copyOf(trace);
    }

    /**
     * Disables trace recording — for soak-length runs, where holding millions of actions would
     * dominate the heap and a failure is reproduced from its seed rather than a shrunken trace.
     */
    ParsleyTopologySim withNoTrace() {
        this.traceRecording = false;
        return this;
    }

    /**
     * Disables the O(causal-past)-per-event ground-truth machinery — per-record history
     * snapshots and the invariant checks that walk them (ground-truth I2 dominance, the
     * causal-delivery-order check, D2 over the full own-send set) — whose cost is quadratic in
     * run length and makes soak-scale runs impractical. Everything with bounded per-event cost
     * stays armed: doc-form I2 (obligation dominance), I3 monotonicity, acked-send coverage,
     * the duplicate-delivery check, and the runaway guard. Those exhaustive invariants are the
     * CI and deep tiers' job, on run lengths where they are affordable; the soak hunts leaks
     * and stalls.
     */
    ParsleyTopologySim withoutGroundTruthHistories() {
        this.groundTruthHistories = false;
        return this;
    }

    private void record(ParsleySimTrace.Action action) {
        if (mode == Mode.GENERATE && traceRecording) {
            trace.add(action);
        }
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

    /** Enables random dirty restarts — no final ack, no fold; see the class Javadoc. */
    ParsleyTopologySim withCrashes() {
        this.crashesEnabled = true;
        return this;
    }

    /** Enables random consumer-position rollbacks — at-least-once redelivery (A11). */
    ParsleyTopologySim withRollbacks() {
        this.rollbacksEnabled = true;
        return this;
    }

    /** Enables random A5/A6 scope-change restarts (grow or shrink by one input). */
    ParsleyTopologySim withScopeChanges() {
        this.scopeChangesEnabled = true;
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
     * A topology whose feedback loops amplify faster than they damp — a configuration problem
     * (the random explorer skips and counts these), not a protocol defect. Distinguished from a
     * relay loop, which IS a protocol defect: see {@link #guardRunaway}.
     */
    static final class SupercriticalTopologyException extends RuntimeException {
        SupercriticalTopologyException(String message) {
            super(message);
        }
    }

    /**
     * Fails fast (with the seed) on a runaway emission loop, classifying it: a log tail of
     * almost entirely null messages means the gossip relay itself is looping — I6's
     * knowledge-based relay must quiesce, so that is a genuine protocol failure — while a tail
     * with substantial business traffic means the topology's feedback amplification is
     * supercritical (every consumer-with-sinks appends ~one record per delivered record), which
     * is a misconfigured-run signal the explorer may skip, never a defect.
     */
    private void guardRunaway(SimTopic sink) {
        // Legitimate volume is linear in external input (each external record cascades a
        // topology-bounded number of appends), so the cap scales with it — a soak's fiftieth
        // thousand healthy record is not a runaway. Pathological growth is decoupled from
        // externals (a storm appends unboundedly while externals stand still, e.g. during a
        // drain), so it crosses any linear-in-externals bound almost as fast as a fixed one.
        if (sink.log.size() <= 20_000 + 50 * externalSequence) {
            return;
        }
        long nullTail = sink.log.subList(sink.log.size() - 1_000, sink.log.size()).stream()
                .filter(SimRecord::nullMessage)
                .count();
        if (nullTail >= 950) {
            throw new AssertionError(runLabel + "relay loop: topic " + sink.name + " grew past "
                    + "20000 records of almost entirely null messages — the I6 knowledge-based "
                    + "relay must quiesce");
        }
        throw new SupercriticalTopologyException(runLabel + "supercritical topology: topic "
                + sink.name + " exceeded 20000 records under business amplification");
    }

    // --- the scheduler -------------------------------------------------------------------------

    /** Runs {@code steps} random scheduler steps, draining all remaining channel backlog at the end. */
    void run(int steps) {
        runWithoutDrain(steps);
        drain();
    }

    /**
     * As {@link #run} but without the final settle {@link #drain} — buffered records stay held, so
     * a scope-change property can exercise the held-record disposition at the following restart.
     */
    void runWithoutDrain(int steps) {
        for (int i = 0; i < steps; i++) {
            step();
        }
    }

    private void step() {
        int action = random.nextInt(10);
        if (action == 0 && !externalTopics.isEmpty()) {
            produceExternal(externalTopics.get(random.nextInt(externalTopics.size())));
            return;
        }
        if (action == 1) {
            ackAll(randomNode());
            return;
        }
        if (action == 2 && restartsEnabled) {
            restart(randomNode().name);
            return;
        }
        // Fault-mode slots: when a mode is off its slot falls through to the poll below, drawing
        // exactly what the pre-fault-mode scheduler drew — legacy seeds keep their schedules.
        if (action == 3 && crashesEnabled) {
            crash(randomNode().name);
            return;
        }
        if (action == 4 && rollbacksEnabled) {
            rollbackRandom(randomNode());
            return;
        }
        if (action == 5 && scopeChangesEnabled) {
            rescopeRandom(randomNode());
            return;
        }
        pollOnce(randomNode());
    }

    /**
     * Polls every node until no channel has undelivered backlog — lets every seed end settled.
     * Acks are folded promptly after every poll that left sends pending: in production a broker
     * ack lands well inside a produce→consume→relay round-trip, and it is that ack (folding the
     * send into {@code ownOutputs}, hence the stamp) that quenches the I6 relay — two nodes
     * gossiping across a topic cycle otherwise keep "teaching" each other about the receiver's
     * own not-yet-acked relays, each relay appending the coordinate that sustains the next: an
     * ack-starvation artifact of lazy modelling, not a protocol loop. (The stepped phase keeps
     * lazy random acks — bounded there by the step budget, and latency variation is exactly what
     * it explores.)
     */
    void drain() {
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (SimNode node : nodes.values()) {
                ackAll(node);
                while (node.hasPollableInput()) {
                    pollOnce(node);
                    if (!node.producer.pendingMax.isEmpty()) {
                        ackAll(node);
                    }
                    progressed = true;
                }
            }
        }
    }

    private void ackAll(SimNode node) {
        record(new ParsleySimTrace.AckAll(node.name));
        node.producer.ackAllExcept(Set.of());
    }

    void produceExternal(String topicName) {
        record(new ParsleySimTrace.ProduceExternal(topicName));
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
        // Recorded up front, not on success: a Rescope that fails init (the held-record
        // disposition rule) must be in the trace, or a replay could never reproduce the failure.
        record(new ParsleySimTrace.Rescope(nodeName, newInputs.stream().sorted().toList()));
        SimNode node = nodeNamed(nodeName);
        boolean scopeChanged = !Set.copyOf(newInputs).equals(node.inputs);
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
        if (scopeChanged) {
            rescopes++;
        }
    }

    /**
     * A dirty restart: the process dies with unfolded acks and un-awaited pending sends — no
     * {@code ackAllExcept}, no {@code foldAcknowledgedOutputs}. Sends already appended stay in
     * the topic logs (committed, just never folded into the "f" blob); the init-time sink
     * end-offset seed (I8) must re-cover them so post-crash stamps still claim them. The sim's
     * stores commit instantly, so this is exactly the crash-after-commit-before-fold shape — the
     * store-rewinding transaction tear is a documented non-goal (class Javadoc).
     */
    void crash(String nodeName) {
        record(new ParsleySimTrace.Crash(nodeName));
        SimNode node = nodeNamed(nodeName);
        node.producer = new SimProducer();
        node.start(node.inputs);
        crashes++;
    }

    /**
     * Rewinds one consumed channel's cursor to a random earlier position inside the
     * <em>already-delivered region</em> — the redelivery shape reachable under the library's
     * unconditional {@code exactly_once_v2} requirement (an operator offset reset, a stale
     * committed offset: exactly the arrival A11's fail-safe net exists for). Every re-polled
     * record must then be absorbed: skipped by {@code alreadyDelivered} (frontier or forwarded
     * mark), with the duplicate-delivery assertion in {@code processDeliveries} catching any
     * that reach the delegate twice. The rewind never enters the held-record region: a cursor
     * behind a surviving buffered copy is unreachable under EOS (offsets, buffer, and frontier
     * commit atomically), so re-buffering there would be the sim inventing a state, not the
     * protocol failing — {@link #lowestSafeRollback} is the fence.
     */
    private void rollbackRandom(SimNode node) {
        List<String> rewindable = node.inputs.stream()
                .filter(input -> lowestSafeRollback(node, input) < node.cursors.getOrDefault(input, 0))
                .sorted()
                .toList();
        if (rewindable.isEmpty()) {
            return;
        }
        String input = rewindable.get(random.nextInt(rewindable.size()));
        int lowest = lowestSafeRollback(node, input);
        int cursor = node.cursors.get(input);
        int toCursor = lowest + random.nextInt(cursor - lowest);
        record(new ParsleySimTrace.Rollback(node.name, input, toCursor));
        node.cursors.put(input, toCursor);
        rollbacks++;
    }

    /**
     * The lowest cursor position on {@code input} whose whole re-fetched suffix is
     * already-delivered state: every record at or above it and below the current cursor is
     * either at-or-below the node's frontier for the channel, or a business record the delegate
     * has genuinely seen (a forwarded above-gap delivery). A held (buffered, undelivered) record
     * blocks the walk — rewinding past it is EOS-unreachable, per {@link #rollbackRandom}.
     */
    private int lowestSafeRollback(SimNode node, String input) {
        SimTopic topic = topic(input);
        long frontierOffset = node.channels.frontier().offsetFor(topic.id, PARTITION);
        int cursor = node.cursors.getOrDefault(input, 0);
        int lowest = cursor;
        // The rewind window is capped: real offset resets are shallow, and an uncapped walk (plus
        // the re-polling it licenses) turns quadratic in the log over a soak-length run.
        int windowFloor = Math.max(0, cursor - 200);
        while (lowest > windowFloor) {
            int offset = lowest - 1;
            SimRecord record = topic.log.get(offset);
            boolean absorbed = offset <= frontierOffset
                    || (!record.nullMessage() && node.delivered.contains(new SimCoord(topic.id, offset)));
            if (!absorbed) {
                break;
            }
            lowest--;
        }
        return lowest;
    }

    /**
     * A random A5/A6 scope-change restart: grow by one topic from the pool, or shrink by one
     * input (never below one). A shrink that would orphan held records from the removed input is
     * skipped — the loud init failure it would (correctly) raise is the disposition rule's own
     * contract, pinned in {@code ParsleyScopeChangePropertyTest}, not schedule noise.
     */
    private void rescopeRandom(SimNode node) {
        if (random.nextBoolean()) {
            // Cycle-closing growth is barred: a topic another node produces downstream of this
            // one would create a feedback loop whose amplification (every consumer-with-sinks
            // appends ~one record per delivery, business forward or completeness advert) can go
            // supercritical regardless of the forward probability. Cyclic scope growth stays
            // covered where its parameters are controlled: the generator's capped static cycles
            // and ParsleyScopeChangePropertyTest's former-own-sink property.
            List<String> candidates = topics.keySet().stream()
                    .filter(topicName -> !node.inputs.contains(topicName))
                    .filter(topicName -> !growthWouldCloseCycle(node, topicName))
                    .sorted()
                    .toList();
            if (candidates.isEmpty()) {
                return;
            }
            Set<String> grown = new HashSet<>(node.inputs);
            grown.add(candidates.get(random.nextInt(candidates.size())));
            restartWithInputs(node.name, grown);
            return;
        }
        if (node.inputs.size() <= 1) {
            return;
        }
        List<String> current = node.inputs.stream().sorted().toList();
        String removed = current.get(random.nextInt(current.size()));
        if (node.heldSourceTopics().contains(removed)) {
            return;
        }
        Set<String> shrunk = new HashSet<>(node.inputs);
        shrunk.remove(removed);
        restartWithInputs(node.name, shrunk);
    }

    /**
     * Whether adding {@code topicName} to {@code grower}'s inputs closes a directed cycle: true
     * iff some producer of the topic (the grower included) is reachable from the grower through
     * the produces→consumes graph as it stands.
     */
    private boolean growthWouldCloseCycle(SimNode grower, String topicName) {
        Set<String> reachable = new HashSet<>();
        reachable.add(grower.name);
        List<SimNode> frontier = new ArrayList<>(List.of(grower));
        while (!frontier.isEmpty()) {
            SimNode current = frontier.remove(frontier.size() - 1);
            for (String sink : current.sinks) {
                for (SimNode consumer : nodes.values()) {
                    if (consumer.inputs.contains(sink) && reachable.add(consumer.name)) {
                        frontier.add(consumer);
                    }
                }
            }
        }
        return nodes.values().stream()
                .anyMatch(producer -> producer.sinks.contains(topicName) && reachable.contains(producer.name));
    }

    private SimNode randomNode() {
        List<SimNode> all = new ArrayList<>(nodes.values());
        return all.get(random.nextInt(all.size()));
    }

    private void pollOnce(SimNode node) {
        // Sorted: Set iteration order is salted per JVM and must never decide what a seed polls.
        List<String> pollable = node.inputs.stream()
                .filter(input -> node.cursors.get(input) < topic(input).log.size())
                .sorted()
                .toList();
        if (pollable.isEmpty()) {
            return;
        }
        String input = pollable.get(random.nextInt(pollable.size()));
        currentEmits.clear();
        try {
            consume(node, input);
        } finally {
            // finally, not on success: a poll whose invariant assertion fires mid-delivery must
            // still enter the trace (with the decisions drawn so far), or replay stops one
            // action short of the failure.
            record(new ParsleySimTrace.Poll(node.name, input, List.copyOf(currentEmits)));
        }
    }

    /** Advances the cursor and routes the record down its receive path — shared by both modes. */
    private void consume(SimNode node, String input) {
        SimTopic topic = topic(input);
        SimRecord record = topic.log.get(node.cursors.merge(input, 1, Integer::sum) - 1);
        if (record.nullMessage()) {
            deliverNullMessage(node, record);
        } else {
            deliverBusiness(node, record);
        }
    }

    /** Replay-side executor — the inverse of the recording sites, PRNG-free by construction. */
    private void execute(ParsleySimTrace.Action action) {
        switch (action) {
            case ParsleySimTrace.ProduceExternal(String topicName) -> produceExternal(topicName);
            case ParsleySimTrace.AckAll(String nodeName) -> ackAll(nodeNamed(nodeName));
            case ParsleySimTrace.Rescope(String nodeName, List<String> inputs) ->
                    restartWithInputs(nodeName, Set.copyOf(inputs));
            case ParsleySimTrace.Crash(String nodeName) -> crash(nodeName);
            case ParsleySimTrace.Rollback(String nodeName, String input, int toCursor) -> {
                SimNode node = nodeNamed(nodeName);
                // Clamped to the safe fence: identical state replays the recorded rewind exactly;
                // a shrunken trace whose state diverged must still never enter EOS-unreachable
                // territory (see rollbackRandom), or the shrinker would chase sim artifacts.
                int clamped = Math.max(toCursor, node.inputs.contains(input)
                        ? lowestSafeRollback(node, input) : Integer.MAX_VALUE);
                if (node.inputs.contains(input) && clamped < node.cursors.getOrDefault(input, 0)) {
                    node.cursors.put(input, clamped);
                    rollbacks++;
                }
            }
            case ParsleySimTrace.Poll(String nodeName, String input, List<ParsleySimTrace.Emit> emits) -> {
                SimNode node = nodeNamed(nodeName);
                // Divergence tolerance for shrunken traces: an input the node no longer consumes
                // or has no record on is a no-op, exactly as the generate-side poll would skip it.
                if (!node.inputs.contains(input)
                        || node.cursors.getOrDefault(input, 0) >= topic(input).log.size()) {
                    return;
                }
                replayEmits = emits.iterator();
                try {
                    consume(node, input);
                } finally {
                    replayEmits = null;
                }
            }
        }
    }

    // --- the receive paths ---------------------------------------------------------------------

    private void deliverBusiness(SimNode node, SimRecord record) {
        ParsleyVectorClock completenessBefore = node.core.completeness();
        int bufferedBefore = node.core.bufferSize();
        ParsleyCausalBroadcast.Outcome<String, String> outcome = node.core.receive(new ParsleyMessage<>(
                record.topic(), record.topicId(), PARTITION, record.offset(), 0L,
                "k", record.value(), List.of(), record.stamp()));
        // Buffer growth, not an empty delivered list: an absorbed redelivery (rollback replay,
        // A11) and a skipped scope-growth prefix (A5) also return empty, and neither is a hold.
        if (node.core.bufferSize() > bufferedBefore) {
            recordsHeld++;
        }
        boolean anyBusinessOutput = processDeliveries(node, outcome.delivered(), false);
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
        boolean anyBusinessOutput = processDeliveries(node, outcome.delivered(), false);
        // The I6 relay (strictly new knowledge propagates onward), and the silent-delegate
        // advertisement for any released records — at most one null message per poll.
        if (outcome.learnedSomethingNew() || (!anyBusinessOutput && !outcome.delivered().isEmpty())) {
            emitNullMessage(node);
        }
    }

    /** Delivers each released record to the ground-truth delegate; returns whether any business output was emitted. */
    private boolean processDeliveries(SimNode node, List<ParsleyMessage<String, String>> released,
                                      boolean restoreDrain) {
        boolean anyBusinessOutput = false;
        for (ParsleyMessage<String, String> message : released) {
            SimRecord record = topic(message.topic()).log.get((int) message.offset());
            if (groundTruthHistories) {
                assertCausalDeliveryOrder(node, record);
            }
            SimCoord coord = new SimCoord(record.topicId(), record.offset());
            assertTrue(node.delivered.add(coord),
                    runLabel + node.name + ": " + record.topic() + "@" + record.offset()
                            + " delivered to the delegate twice — a redelivery (rollback replay, "
                            + "A11) must be absorbed by the protocol, never re-released");
            node.truePast.add(coord);
            if (groundTruthHistories) {
                node.truePast.addAll(record.causalHistory());
            }
            node.obligation = node.obligation
                    .merge(record.stamp())
                    .observe(record.topicId(), PARTITION, record.offset());
            node.deliveredOffsetsByTopic.computeIfAbsent(record.topic(), t -> new ArrayList<>()).add(record.offset());
            businessDeliveries++;
            if (record.offset() > node.channels.frontier().offsetFor(record.topicId(), PARTITION)) {
                outOfOrderDeliveries++;
            }
            String sink = restoreDrain ? null : chooseEmission(node);
            if (sink != null) {
                emitBusiness(node, sink, record.value());
                anyBusinessOutput = true;
            }
        }
        return anyBusinessOutput;
    }

    /**
     * The delegate's forward decision for one released record: drawn from the PRNG and recorded
     * into the current {@link ParsleySimTrace.Poll} in generate mode; read back from it in
     * replay. A missing replay decision (a shrunken trace released more records than the
     * recording did) means no emission; a recorded sink the node does not have (impossible on
     * faithful replays — sinks never change) is likewise dropped.
     */
    private @Nullable String chooseEmission(SimNode node) {
        if (mode == Mode.REPLAY) {
            ParsleySimTrace.Emit emit = replayEmits != null && replayEmits.hasNext()
                    ? replayEmits.next()
                    : ParsleySimTrace.Emit.NONE;
            return emit.business() && node.sinks.contains(emit.sink()) ? emit.sink() : null;
        }
        if (!node.sinks.isEmpty() && random.nextDouble() < node.businessOutputProbability) {
            String sink = node.sinks.get(random.nextInt(node.sinks.size()));
            currentEmits.add(new ParsleySimTrace.Emit(true, sink));
            return sink;
        }
        currentEmits.add(ParsleySimTrace.Emit.NONE);
        return null;
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
        ParsleyVectorClock stamp;
        if (groundTruthHistories) {
            ParsleyVectorClock ownSendsBefore = clockOver(node.ownBusinessSends);
            stamp = stampThrough(node, Set.of());
            assertTrue(stamp.dominates(ownSendsBefore),
                    runLabel + node.name + ": a business emission's stamp must claim every prior own "
                            + "business send — the crossing wait forces their acks before the stamp (D2/O1)");
        } else {
            stamp = stampThrough(node, Set.of());
        }
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
            long offset = sink.log.size();
            sink.log.add(new SimRecord(sink.name, sink.id, offset, "∅",
                    stamp, true, Set.of()));
            // A null message's own coordinate is protocol bookkeeping, never a delegate-visible
            // cause — it enters neither ownBusinessSends nor any causal history. Its SEND is
            // still a real produced record whose ack folds into ownOutputs like any other (O4
            // exempts null destinations from the crossing WAIT, not from acking): without this
            // pending entry a node's stamp never claims its own null sends, a peer's echo of
            // them always reads as news, and every two-node gossip cycle spins forever — a sim
            // infidelity the explorer's first sweep surfaced.
            node.producer.pendingMax.merge(new TopicPartition(sink.name, PARTITION), offset, Math::max);
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
        if (groundTruthHistories) {
            assertClockCovers(stamp, node.truePast, node.name
                    + ": stamp must dominate the node's ground-truth causal past (I2 as the D1/D7 proof "
                    + "uses it, including unconsumed-channel ancestry — I9)");
        }
        assertClockCovers(stamp, ackedCoords(node), node.name
                + ": stamp must claim every acknowledged own send (the pre-stamp ack fold, D2)");
        assertTrue(stamp.dominates(node.lastStamp),
                runLabel + node.name + ": successive stamps must be vector-monotone (I3)");
        node.lastStamp = stamp;
        return stamp;
    }

    private Set<SimCoord> emissionHistory(SimNode node) {
        if (!groundTruthHistories) {
            return Set.of();
        }
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

    /**
     * A canonical rendering of the run's complete observable state — logs (values, stamps,
     * histories), per-node protocol and ground-truth state, and every counter — with topic UUIDs
     * projected back to names, since UUIDs are freshly random per sim instance. Two runs of the
     * same schedule over the same spec must produce byte-identical digests; this is the equality
     * witness the replay-fidelity test stands on.
     */
    String stateDigest() {
        StringBuilder digest = new StringBuilder();
        topics.forEach((name, topic) -> {
            digest.append("topic ").append(name).append('\n');
            for (SimRecord record : topic.log) {
                digest.append("  ").append(record.nullMessage() ? "null-message" : record.value())
                        .append(" stamp=").append(projectClock(record.stamp()))
                        .append(" history=").append(projectCoords(record.causalHistory()))
                        .append('\n');
            }
        });
        nodes.forEach((name, node) -> digest.append("node ").append(name)
                .append(" inputs=").append(node.inputs.stream().sorted().toList())
                .append(" cursors=").append(new TreeMap<>(node.cursors))
                .append(" delivered=").append(projectCoords(node.delivered))
                .append(" truePast=").append(projectCoords(node.truePast))
                .append(" ownSends=").append(projectCoords(node.ownBusinessSends))
                .append(" stamp=").append(projectClock(node.channels.stamp()))
                .append(" frontier=").append(projectClock(node.channels.frontier()))
                .append(" buffered=").append(node.core.bufferSize())
                .append('\n'));
        digest.append("counters deliveries=").append(businessDeliveries)
                .append(" held=").append(recordsHeld)
                .append(" nulls=").append(nullMessagesDelivered)
                .append(" outOfOrder=").append(outOfOrderDeliveries)
                .append(" emissions=").append(emissions)
                .append(" restarts=").append(restarts)
                .append(" crashes=").append(crashes)
                .append(" rollbacks=").append(rollbacks)
                .append(" rescopes=").append(rescopes)
                .append('\n');
        return digest.toString();
    }

    private String projectClock(ParsleyVectorClock clock) {
        StringBuilder projected = new StringBuilder("{");
        topics.forEach((name, topic) -> {
            long offset = clock.offsetFor(topic.id, PARTITION);
            if (offset >= 0) {
                projected.append(name).append('@').append(offset).append(' ');
            }
        });
        return projected.append('}').toString();
    }

    private String projectCoords(Set<SimCoord> coords) {
        return coords.stream()
                .map(coord -> topicNamesById.get(coord.topicId()) + "@" + coord.offset())
                .sorted()
                .collect(Collectors.joining(",", "[", "]"));
    }

    Uuid topicId(String name) {
        return topic(name).id;
    }

    int logSize(String topicName) {
        return topic(topicName).log.size();
    }
}
