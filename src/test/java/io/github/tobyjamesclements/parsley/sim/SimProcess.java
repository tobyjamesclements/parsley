package io.github.tobyjamesclements.parsley.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.DeliverableMessage;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.PositionFacts;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;
import io.github.tobyjamesclements.parsley.core.ReceivedMessage;
import io.github.tobyjamesclements.parsley.sim.SimWorld.SimChannel;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory;

/**
 * One process under a simulated host that honours the Host obligations: feeds each channel in position order below
 * the LSO barrier, commits state + sends + read positions atomically, aborts and re-feeds on crash, restarts through
 * full initialisation, and reports read positions as committed offsets. Drives a real {@link ProcessEngine}.
 *
 * <p>Every message fed is reported to the oracle before the engine sees it, so an engine that consumes a position
 * and discards its message cannot erase the evidence of the loss. Engine operations still throw
 * {@link ParsleyFailClosedException} out of these methods — targeted tests assert on that directly; the scenario
 * harness converts the throw into {@link #failClosed}, which aborts the step exactly as the real host's transaction
 * abort would and leaves the process stopped with its refusal recorded.</p>
 */
public final class SimProcess {

    public enum FeedResult { FED, NOTHING, STALLED }

    final String name;
    private final SimWorld world;
    private final Oracle oracle;
    private final EngineTestFactory.SabotageMode sabotage;
    private final MemoryOrderingStore store = new MemoryOrderingStore();

    private Map<ChannelId, SimChannel> received;
    private List<SimChannel> sendChannels;
    private final SimLogic logic;

    private ProcessEngine engine;
    private ParsleyFailClosedException failure;
    private Object openTxn;
    private final List<Oracle.Sent> stepAppends = new ArrayList<>();
    private final Map<ChannelId, Long> committedNextRead = new HashMap<>();
    private final Map<ChannelId, Long> workingNextRead = new HashMap<>();
    /** The read position first established when each channel entered the declaration — the pre-committed initial
     * position of docs/DESIGN.md §7. Messages below it were never this process's to read. */
    private final Map<ChannelId, Long> initialNextRead = new HashMap<>();
    /** The highest read position ever committed per channel: host-side truth of what this process has covered,
     * unaffected by operator rewinds. What the substrate discards below it was not lost unread. */
    private final Map<ChannelId, Long> highWaterNextRead = new HashMap<>();
    /** Ids this process has seen recreated: sticky, exactly as the production facts source's confirmedRecreated
     * set — a later death of the new incarnation must not downgrade the verdict back to plain dead. */
    private final java.util.Set<ChannelId> confirmedRecreated = new java.util.TreeSet<>();

    /** Application logic in the simulation: a pure function of the delivered message (SPEC Assumption 16). */
    @FunctionalInterface
    public interface SimLogic {
        List<SimChannel> emitTargets(Instance delivered);
    }

    public SimProcess(String name, SimWorld world, Oracle oracle, List<SimChannel> received,
                      List<SimChannel> sendChannels, SimLogic logic, EngineTestFactory.SabotageMode sabotage) {
        this.name = name;
        this.world = world;
        this.oracle = oracle;
        this.sabotage = sabotage;
        this.logic = logic;
        this.sendChannels = List.copyOf(sendChannels);
        redeclare(received);
        workingNextRead.putAll(committedNextRead);
    }

    /** Change the declaration; takes effect at the next {@link #start()} (SPEC Structural 16). */
    public void redeclare(List<SimChannel> newReceived) {
        this.received = newReceived.stream()
                .collect(Collectors.toMap(SimChannel::id, c -> c, (a, b) -> a, LinkedHashMap::new));
        for (SimChannel channel : newReceived) {
            // The runtime pre-commits an initial read position for channels never read before (docs/DESIGN.md §7).
            committedNextRead.putIfAbsent(channel.id(), channel.logStart);
            workingNextRead.putIfAbsent(channel.id(), channel.logStart);
            initialNextRead.putIfAbsent(channel.id(), channel.logStart);
            // putIfAbsent, never a bump: on a REJOIN the process's coverage is its old committed read, and
            // crediting it with the current log start would excuse exactly the truncation-while-away gap the
            // Safety 8 obligation exists to catch (review finding S10: the bump masked seeds the sweep owns).
            highWaterNextRead.putIfAbsent(channel.id(), channel.logStart);
        }
    }

    public void setSendChannels(List<SimChannel> channels) {
        this.sendChannels = List.copyOf(channels);
    }

    /** Full initialisation, as after a restart: a fresh engine over the last committed state (Host obligation 4/5). */
    public void start() {
        if (engine != null) {
            throw new IllegalStateException(name + " already started");
        }
        failure = null;
        Map<ChannelId, String> names = new LinkedHashMap<>();
        received.forEach((id, channel) -> names.put(id, channel.name));
        try {
            engine = EngineTestFactory.create(name, names, store, sabotage);
        } catch (ParsleyFailClosedException e) {
            // The refusal aborts initialisation: nothing it wrote may survive, as on the real host where the
            // task's transaction never commits.
            store.rollback();
            throw e;
        }
        oracle.onStart(name);
        ingestFacts();
    }

    /**
     * Crash: the open step aborts and the process stops. There is deliberately no way to abort a step and carry on
     * with the same initialisation — the host must restart a process through its full initialisation (Host
     * obligation 4), and the engine's in-memory state is entitled to that.
     */
    public void crash() {
        if (openTxn != null) {
            world.abortTxn(openTxn);
            openTxn = null;
        }
        oracle.rollbackStep(name);
        stepAppends.clear();
        store.rollback();
        workingNextRead.clear();
        workingNextRead.putAll(committedNextRead);
        engine = null;
    }

    /** A fail-closed refusal surfaced: abort the step (its events have not occurred) and stop, keeping the reason. */
    public void failClosed(ParsleyFailClosedException e) {
        crash();
        failure = e;
    }

    public boolean failedClosed() {
        return failure != null;
    }

    public ParsleyFailClosedException failure() {
        return failure;
    }

    public void stopCleanly() {
        commitStep();
        engine = null;
    }

    public boolean isRunning() {
        return engine != null;
    }

    public ProcessEngine engine() {
        return engine;
    }

    private void ensureTxn() {
        if (engine == null) {
            throw new IllegalStateException(name + " is not running");
        }
        if (openTxn == null) {
            openTxn = new Object();
        }
    }

    /** Commit the open step: sends become visible, state and read positions become durable, atomically. */
    public void commitStep() {
        if (openTxn == null) {
            return;
        }
        engine.flushHolds();
        world.commitTxn(openTxn);
        store.commit();
        committedNextRead.clear();
        committedNextRead.putAll(workingNextRead);
        committedNextRead.forEach((id, next) -> highWaterNextRead.merge(id, next, Math::max));
        oracle.commitStep(name, List.copyOf(stepAppends));
        stepAppends.clear();
        openTxn = null;
    }


    /** Feed the next message of one channel, advancing the read position past dead slots as a Kafka fetch would. */
    public FeedResult feedOne(SimChannel channel) {
        ensureTxn();
        ChannelId id = channel.id();
        if (channel.dead) {
            return FeedResult.NOTHING; // a deleted topic feeds nothing, ever again.
        }
        long position = workingNextRead.get(id);
        if (position < channel.logStart) {
            return FeedResult.STALLED; // out of range: the host cannot read here; facts ingestion fails closed.
        }
        long lso = world.lso(channel);
        while (position < lso && world.slot(channel, position) instanceof SimWorld.DeadSlot) {
            position++;
        }
        if (position >= lso) {
            workingNextRead.put(id, position); // the read position still advances past a trailing dead run.
            return FeedResult.NOTHING;
        }
        SimWorld.MessageSlot slot = (SimWorld.MessageSlot) world.slot(channel, position);
        workingNextRead.put(id, position + 1);
        Instance instance = slot.instance();
        // The oracle observes the feed itself, not the engine's acceptance: from here the delivery is owed unless
        // the oracle's own bookkeeping excuses it. If the engine throws, the step's abort rolls this back too.
        oracle.onFed(name, instance);
        engine.onReceive(new ReceivedMessage(id, position, position, instance.key, instance.value, instance.headers));
        return FeedResult.FED;
    }

    /** Deliver everything deliverable, invoking logic and sending its emissions within the step. */
    public int drain() {
        ensureTxn();
        int delivered = 0;
        while (true) {
            Optional<DeliverableMessage> next = engine.nextDeliverable();
            if (next.isEmpty()) {
                return delivered;
            }
            DeliverableMessage message = next.get();
            SimChannel channel = received.get(message.channel());
            Instance instance = ((SimWorld.MessageSlot) world.slot(channel, message.position())).instance();
            assertContentFidelity(message, instance);
            engine.markDelivered(message.channel(), message.position());
            oracle.onDelivered(name, instance);
            delivered++;
            for (SimChannel target : logic.emitTargets(instance)) {
                send(target, instance.uid + ">" + name + ">" + target.name);
            }
        }
    }

    /** What reaches application logic must be exactly what was received — key, value, headers and timestamp — no
     * matter whether the message was delivered in its arrival step or restored from the ordering store after a
     * restart (SPEC Safety 4; Host obligation 5's state fidelity). */
    private void assertContentFidelity(DeliverableMessage delivered, Instance instance) {
        if (!Arrays.equals(delivered.key(), instance.key)
                || !Arrays.equals(delivered.value(), instance.value)
                || delivered.timestamp() != instance.position
                || !headersEqual(delivered.headers(), instance.headers)) {
            throw new AssertionError(name + " delivered " + instance + " with altered content: key/value/timestamp/"
                    + "headers must reach application logic exactly as received (restored holds included)");
        }
    }

    private static boolean headersEqual(List<HeaderKV> actual, List<HeaderKV> expected) {
        if (actual.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < actual.size(); i++) {
            if (!actual.get(i).key().equals(expected.get(i).key())
                    || !Arrays.equals(actual.get(i).value(), expected.get(i).value())) {
                return false;
            }
        }
        return true;
    }

    private void send(SimChannel target, String uid) {
        byte[] causesHeader = engine.causesHeaderForEmission();
        Set<Instance> trueCauses = oracle.causalPastSnapshot(name);
        Map<ChannelId, Long> upperBound = oracle.expressionUpperBound(name);
        // Which causes were already discardable (SPEC Structural 13) at this moment: judged now, not at commit,
        // so a kill or truncation landing between send and commit cannot retroactively excuse under-expression.
        Set<Instance> excused = new java.util.HashSet<>();
        for (Instance cause : trueCauses) {
            SimChannel causeChannel = world.channel(cause.channel);
            if (causeChannel != null && (causeChannel.dead || cause.position < causeChannel.logStart)) {
                excused.add(cause);
            }
        }
        io.github.tobyjamesclements.parsley.core.Causes meta = decodeMeta(causesHeader);
        // Snapshot, per expressed channel, the highest position assigned at this moment — before this send's own
        // append — so the Structural 12 check judges the send against the world it was stamped in.
        Map<ChannelId, Long> lastAssigned = new TreeMap<>();
        meta.byChannel().keySet().forEach(channelId -> {
            SimChannel simChannel = world.channel(channelId);
            if (simChannel != null) {
                lastAssigned.put(channelId, world.lastAssigned(simChannel));
            }
        });
        long position = world.appendPending(target, openTxn, (channelId, pos) -> new Instance(
                channelId, pos, uid, uid.getBytes(), uid.getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, causesHeader)), meta, trueCauses));
        stepAppends.add(new Oracle.Sent(
                ((SimWorld.PendingSlot) world.slot(target, position)).instance(), upperBound, lastAssigned,
                excused));
    }

    private static io.github.tobyjamesclements.parsley.core.Causes decodeMeta(byte[] causesHeader) {
        try {
            return CausesCodec.decode(causesHeader);
        } catch (CausesCodec.UndecodableMetadataException e) {
            throw new AssertionError("engine emitted an undecodable causes header", e);
        }
    }

    /** Ingest position facts, as the runtime's punctuator does: committed offsets, log starts, dead topics, and
     * channels whose name now denotes a different topic (recreation — affirmative identity evidence). */
    public void ingestFacts() {
        ensureTxn();
        Map<ChannelId, Long> committed = new TreeMap<>();
        Map<ChannelId, Long> logStarts = new TreeMap<>();
        Set<ChannelId> dead = new java.util.TreeSet<>();
        Set<ChannelId> recreated = new java.util.TreeSet<>();
        for (SimChannel channel : world.allChannels()) {
            if (channel.dead) {
                SimChannel current = world.currentByName(channel.name);
                if (current != null && !current.id().equals(channel.id())) {
                    confirmedRecreated.add(channel.id());
                }
                // The verdict is sticky, exactly as the production facts source's: a recreated id stays
                // recreated even after the new incarnation's own death — never downgraded to plain dead —
                // and the sweep must certify margins against the fact shape production produces.
                (confirmedRecreated.contains(channel.id()) ? recreated : dead).add(channel.id());
                // Production gather reports no offsets for a dead or recreated id (its name no longer denotes
                // it, so there is nothing to safely query): neither committedNextRead nor logStart is emitted.
                continue;
            }
            logStarts.put(channel.id(), channel.logStart);
            Long next = committedNextRead.get(channel.id());
            if (received.containsKey(channel.id()) && next != null) {
                committed.put(channel.id(), next);
            }
        }
        engine.onFacts(new PositionFacts(committed, logStarts, dead, recreated));
    }

    /** An operator offset rewind while the process is stopped: the host will re-feed already-committed positions. */
    public void rewindCommitted(SimChannel channel, int back) {
        if (engine != null) {
            throw new IllegalStateException(name + " must be stopped to rewind offsets");
        }
        ChannelId id = channel.id();
        long target = Math.max(channel.logStart, committedNextRead.get(id) - back);
        committedNextRead.put(id, target);
        workingNextRead.put(id, target);
    }

    public long committedNextRead(SimChannel channel) {
        Long next = committedNextRead.get(channel.id());
        return next == null ? Long.MIN_VALUE : next;
    }

    public long highWaterNextRead(SimChannel channel) {
        Long highWater = highWaterNextRead.get(channel.id());
        return highWater == null ? Long.MIN_VALUE : highWater;
    }

    public long initialNextRead(SimChannel channel) {
        Long initial = initialNextRead.get(channel.id());
        return initial == null ? Long.MAX_VALUE : initial;
    }

    public long workingNextRead(SimChannel channel) {
        return workingNextRead.get(channel.id());
    }

    public Iterable<SimChannel> receivedChannels() {
        return received.values();
    }

    public List<SimChannel> sendChannels() {
        return sendChannels;
    }
}
