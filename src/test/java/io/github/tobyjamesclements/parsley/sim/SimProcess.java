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
import io.github.tobyjamesclements.parsley.core.IdentityReport;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;
import io.github.tobyjamesclements.parsley.core.ReceivedMessage;
import io.github.tobyjamesclements.parsley.sim.SimWorld.SimChannel;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory;

/**
 * A simulated process: its declaration, its engine, and its lifecycle.
 *
 * <p>The simulated host honours the host obligations the way the Kafka Streams host does
 * after D115: it feeds each channel in order from its committed read position, hands the
 * engine that position at start (Host obligation 2), reports channel identity at start
 * and never between deliveries, and refuses a fetch below a channel's earliest retained
 * position the way {@code auto.offset.reset=none} does — as a {@code POSITIONS_DISCARDED_UNREAD}
 * stop raised by the host, not by the engine (SPEC Safety 8, Assumption 15).
 */
public final class SimProcess {
    public enum FeedResult { FED, NOTHING }

    /**
     * A deliberate host fault, the host-side counterpart of {@link EngineTestFactory.SabotageMode}:
     * the engine no longer checks retention, so the proof that the harness catches a host
     * sailing past discarded positions has to break the host, not the engine.
     */
    public enum HostFault {
        NONE,
        /** Reset a read position below the log start to the log start, as {@code auto.offset.reset=earliest} would. */
        RESET_PAST_LOG_START
    }

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

    private final Map<ChannelId, Long> initialNextRead = new HashMap<>();

    private final Map<ChannelId, Long> highWaterNextRead = new HashMap<>();

    private HostFault hostFault = HostFault.NONE;

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

    public void redeclare(List<SimChannel> newReceived) {
        this.received = newReceived.stream()
                .collect(Collectors.toMap(SimChannel::id, c -> c, (a, b) -> a, LinkedHashMap::new));
        for (SimChannel channel : newReceived) {
            committedNextRead.putIfAbsent(channel.id(), channel.logStart);
            workingNextRead.putIfAbsent(channel.id(), channel.logStart);
            initialNextRead.putIfAbsent(channel.id(), channel.logStart);

            highWaterNextRead.putIfAbsent(channel.id(), channel.logStart);
        }
    }

    public void setSendChannels(List<SimChannel> channels) {
        this.sendChannels = List.copyOf(channels);
    }

    /** Breaks the host deliberately, for the sabotage meta-tests. */
    public void hostFault(HostFault fault) {
        this.hostFault = fault;
    }

    /**
     * Starts an execution: builds the engine over the committed store with the host's
     * committed read positions as its start positions, then reports channel identity — the
     * one thing the host tells the engine about the world outside the feed (D115).
     */
    public void start() {
        if (engine != null) {
            throw new IllegalStateException(name + " already started");
        }
        failure = null;
        Map<ChannelId, String> names = new LinkedHashMap<>();
        received.forEach((id, channel) -> names.put(id, channel.name));
        Map<ChannelId, Long> startPositions = new HashMap<>();
        received.keySet().forEach(id -> startPositions.put(id, committedNextRead.get(id)));
        try {
            engine = EngineTestFactory.create(name, names, store, sabotage, startPositions);
        } catch (ParsleyFailClosedException e) {
            store.rollback();
            throw e;
        }
        oracle.onStart(name);
        reportIdentity();
    }

    /**
     * Re-initialises a running process the way the Kafka Streams host re-creates a task
     * whose source topic went missing: the open step is abandoned, and the next start's
     * identity report sees the change. A refusal at that start leaves the process failed
     * closed, as the harness's guard records it.
     */
    public void reinitialise() {
        crash();
        start();
    }

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

    /**
     * Feeds the next message of a channel, or reports that none is fetchable. A read
     * position below the channel's earliest retained position is the substrate's to refuse
     * (D9's {@code auto.offset.reset=none}): the host raises {@code POSITIONS_DISCARDED_UNREAD}
     * for the fetch, exactly as {@code ParsleyRuntime.classifyFailure} names the consumer's
     * out-of-range stop, and the engine never sees the discarded positions.
     */
    public FeedResult feedOne(SimChannel channel) {
        ensureTxn();
        ChannelId id = channel.id();
        if (channel.dead) {
            return FeedResult.NOTHING;
        }
        long position = workingNextRead.get(id);
        if (position < channel.logStart) {
            if (hostFault == HostFault.RESET_PAST_LOG_START) {
                position = channel.logStart;
                workingNextRead.put(id, position);
            } else {
                throw new ParsleyFailClosedException(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD,
                        "process " + name + ": the substrate no longer retains this process's read position "
                                + position + " on " + channel.name + " (earliest retained " + channel.logStart
                                + "); positions were discarded before they were read (SPEC Safety 8)");
            }
        }
        long lso = world.lso(channel);
        while (position < lso && world.slot(channel, position) instanceof SimWorld.DeadSlot) {
            position++;
        }
        if (position >= lso) {
            workingNextRead.put(id, position);
            return FeedResult.NOTHING;
        }
        SimWorld.MessageSlot slot = (SimWorld.MessageSlot) world.slot(channel, position);
        workingNextRead.put(id, position + 1);
        Instance instance = slot.instance();

        oracle.onFed(name, instance);
        engine.onReceive(new ReceivedMessage(id, position, instance.timestamp, instance.key, instance.value,
                instance.headers));
        return FeedResult.FED;
    }

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
            oracle.onDelivered(name, instance, settledCauses(instance));
            delivered++;
            for (SimChannel target : logic.emitTargets(instance)) {
                send(target, instance.uid + ">" + name + ">" + target.name);
            }
        }
    }

    /**
     * The causes of {@code instance} that are settled by world truth at this moment: on a
     * dead or recreated channel, below the position this process first read the channel
     * from (SPEC Structural 12, Host obligation 2), or on a channel this process does not
     * receive and so will never deliver. Judged at delivery time for the oracle's
     * delivery-legality check. Retention excuses nothing here: a cause retention discarded
     * before this process read it is one the fetch refuses, never one that settles (D115).
     *
     * <p>A cause this process has received and still holds is never settled, whatever the
     * world says of its channel: the process owes its delivery, and delivering an effect
     * past it is the inversion D46 refuses. Without this the check was blind to exactly the
     * shape that record exists for.
     */
    private Set<Instance> settledCauses(Instance instance) {
        Set<Instance> settled = new java.util.HashSet<>();
        for (Instance cause : instance.trueCauses) {
            if (!received.containsKey(cause.channel)) {
                settled.add(cause);
                continue;
            }
            java.util.OptionalLong head = engine.headPosition(cause.channel);
            if (head.isPresent() && cause.position >= head.getAsLong()) {
                continue;
            }
            SimWorld.SimChannel causeChannel = world.channel(cause.channel);
            if (causeChannel != null && (causeChannel.dead || cause.position < initialNextRead(causeChannel))) {
                settled.add(cause);
            }
        }
        return settled;
    }

    private void assertContentFidelity(DeliverableMessage delivered, Instance instance) {
        if (!Arrays.equals(delivered.key(), instance.key)
                || !Arrays.equals(delivered.value(), instance.value)
                || delivered.timestamp() != instance.timestamp
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

        // Only a dead channel excuses an unexpressed cause (SPEC Structural 13, 15): nothing
        // is dropped for retention any more, so a cause below its channel's log start must
        // still be expressed (D115).
        Set<Instance> excused = new java.util.HashSet<>();
        for (Instance cause : trueCauses) {
            SimChannel causeChannel = world.channel(cause.channel);
            if (causeChannel != null && causeChannel.dead) {
                excused.add(cause);
            }
        }
        io.github.tobyjamesclements.parsley.core.Causes meta = decodeMeta(causesHeader);

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

    /**
     * The identity report the host gives the engine at start: every dead channel whose name
     * now resolves to another channel is a recreated one, the rest are deleted. Positions
     * are never reported (D115).
     */
    private void reportIdentity() {
        ensureTxn();
        Set<ChannelId> dead = new java.util.TreeSet<>();
        Set<ChannelId> recreated = new java.util.TreeSet<>();
        for (SimChannel channel : world.allChannels()) {
            if (!channel.dead) {
                continue;
            }
            SimChannel current = world.currentByName(channel.name);
            (current != null && !current.id().equals(channel.id()) ? recreated : dead).add(channel.id());
        }
        engine.onIdentityReport(new IdentityReport(dead, recreated));
    }

    /**
     * Moves the committed read position back, as an expiry restart re-establishing it from
     * coverage does. The target is never clamped to the log start: a position the substrate
     * no longer retains is the fetch's to refuse (SPEC Safety 8), never the host's to skip
     * past, which is exactly the resume-at-log-start D115 retired.
     */
    public void rewindCommitted(SimChannel channel, int back) {
        if (engine != null) {
            throw new IllegalStateException(name + " must be stopped to rewind offsets");
        }
        ChannelId id = channel.id();
        long target = Math.max(0, committedNextRead.get(id) - back);
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
