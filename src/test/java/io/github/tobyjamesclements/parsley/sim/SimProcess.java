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
 * A simulated process: its declaration, its engine, and its lifecycle.
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

    private final Map<ChannelId, Long> initialNextRead = new HashMap<>();

    private final Map<ChannelId, Long> highWaterNextRead = new HashMap<>();

    private final java.util.Set<ChannelId> confirmedRecreated = new java.util.TreeSet<>();

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
            store.rollback();
            throw e;
        }
        oracle.onStart(name);
        ingestFacts();
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

    public FeedResult feedOne(SimChannel channel) {
        ensureTxn();
        ChannelId id = channel.id();
        if (channel.dead) {
            return FeedResult.NOTHING;
        }
        long position = workingNextRead.get(id);
        if (position < channel.logStart) {
            return FeedResult.STALLED;
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
        engine.onReceive(new ReceivedMessage(id, position, position, instance.key, instance.value, instance.headers));
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
            oracle.onDelivered(name, instance);
            delivered++;
            for (SimChannel target : logic.emitTargets(instance)) {
                send(target, instance.uid + ">" + name + ">" + target.name);
            }
        }
    }

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

        Set<Instance> excused = new java.util.HashSet<>();
        for (Instance cause : trueCauses) {
            SimChannel causeChannel = world.channel(cause.channel);
            if (causeChannel != null && (causeChannel.dead || cause.position < causeChannel.logStart)) {
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

                (confirmedRecreated.contains(channel.id()) ? recreated : dead).add(channel.id());

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
