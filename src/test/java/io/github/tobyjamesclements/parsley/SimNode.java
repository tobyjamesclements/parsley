package io.github.tobyjamesclements.parsley;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Hosts one {@link DeliveryProtocol} instance the way the Kafka Streams adapter would: fetches
 * records in per-channel offset order, reports consumer position advances past markers, runs
 * the user behavior on each delivery, performs sends with protocol stamps, and commits
 * everything step-atomically. A step can be aborted mid-flight to model a crash inside an EOS
 * transaction.
 *
 * <p>{@link HostFault} switches one host duty off, for {@link HostContractSelfTest} only: the
 * conformance kit must flag a host that shirks a duty, and the only way to prove it can is to
 * run such a host. Every simulation outside that self-test runs with {@link HostFault#NONE}.
 */
final class SimNode {

    /** A deliberately broken host duty. See {@link HostContractSelfTest}. */
    enum HostFault {
        /** All duties upheld, which is every run outside the host self-test. */
        NONE,
        /** Breaks step-atomicity: a crash aborts the transaction but the store commits anyway. */
        COMMIT_STORE_ON_CRASH,
        /** Withholds the liveness signal: consumer position advances are never reported. */
        DROP_POSITION_ADVANCE
    }

    final String name;
    private final SimWorld world;
    private SimBehavior behavior;
    private final BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory;

    NodeConfig config;
    DeliveryProtocol protocol;
    final SimStateStore store = new SimStateStore();
    final SimBrokerOffsets offsets;
    boolean up;
    HostFault fault = HostFault.NONE;

    /**
     * The stage's own tick channel when it declares ticks: both consumed and produced, so a
     * tick is stamped at the ordinary door, appended to the node's own partition, and returns
     * through the gate as a record like any other. Null when the stage declares no ticks.
     */
    private Channel tickChannel;
    /** The logic a delivered tick runs, mirroring the adapter's tick branch. */
    private SimBehavior tickBehavior = SimBehavior.consumeOnly();

    /** Committed consumer positions (next offset to fetch). */
    private final Map<Channel, Long> positions = new HashMap<>();
    private final List<Object[]> appendedCoords = new ArrayList<>(); // {Channel, Long offset}
    private final Set<Channel> touchedThisStep = new HashSet<>();
    /**
     * Business records fed to the protocol but not yet returned as deliveries, which is the
     * hold queues' depth, counted host-side so it holds for any {@link DeliveryProtocol} and
     * not just the one that keeps queues. Staged like everything else, so an aborted step
     * restores the committed value, which is what a restart would restore from the store.
     */
    private int outstandingHolds;
    private int snapOutstandingHolds;

    SimNode(String name, SimWorld world, NodeConfig config, SimBehavior behavior,
            BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory) {
        this.name = name;
        this.world = world;
        this.config = config;
        this.behavior = behavior;
        this.protocolFactory = protocolFactory;
        this.offsets = new SimBrokerOffsets(world.broker);
    }

    /**
     * Declares ticks on the given channel, which the scenario must list both as a consumed
     * input and as a sink topic, the self-loop the real tick topic forms.
     */
    void ticksOn(Channel c, SimBehavior logic) {
        if (!config.consumed().contains(c) || !config.sinkTopics().contains(c.topicId())) {
            throw new IllegalArgumentException(
                    "a tick channel must be both consumed and a declared sink: " + c);
        }
        this.tickChannel = c;
        this.tickBehavior = logic;
    }

    boolean ticks() {
        return tickChannel != null;
    }

    /** Joins at the current log end on every consumed channel (a `latest` consumer). */
    void joinAtLatest() {
        for (Channel c : config.consumed()) {
            positions.put(c, world.broker.endOffset(c));
        }
    }

    /** Records still held: the depth a restart must restore from the store. */
    int outstandingHolds() {
        return outstandingHolds;
    }

    /** (Re)constructs the protocol from committed state, as a process start does. */
    void start() {
        protocol = protocolFactory.apply(config, this);
        store.commit(); // init-time writes (scope record, rescope) are idempotent re-runs
        up = true;
        // Everything still held at the last commit has just come back through the restore
        // path — the hold-queue deserializer, and the only route a held record's payload
        // takes through bytes.
        world.totalHeldRecordsRestored += outstandingHolds;
        Map<Channel, Long> resume = protocol.resumePositions();
        for (Channel c : config.consumed()) {
            long committed = positions.getOrDefault(c, 0L);
            // A position below the log start cannot be fetched; the consumer resets to the
            // first surviving offset (retention-deleted history is below every baseline).
            long position = Math.max(Math.max(committed, resume.getOrDefault(c, 0L)),
                    world.broker.logStart(c));
            positions.put(c, position);
            world.oracle.subscribed(name, c, position);
        }
    }

    void reconfigure(NodeConfig newConfig, SimBehavior newBehavior) {
        if (up) throw new IllegalStateException("reconfigure while up");
        this.config = newConfig;
        this.behavior = newBehavior;
    }

    void crashIdle() {
        up = false;
        store.discard();
    }

    boolean hasFetchWork(Channel c) {
        return up && world.broker.nextFetchable(c, positions.get(c)) >= 0;
    }

    /**
     * True when the consumer would advance its position without records: nothing fetchable at
     * the position but the log continues past it (trailing markers / aborted records).
     */
    boolean hasPositionAdvance(Channel c) {
        if (!up || fault == HostFault.DROP_POSITION_ADVANCE) return false;
        long pos = positions.get(c);
        return world.broker.nextFetchable(c, pos) < 0 && world.broker.endOffset(c) > pos;
    }

    /** Runs one fetch-process-commit step. {@code crash} aborts it mid-flight instead. */
    void step(Channel c, boolean crash) {
        long off = world.broker.nextFetchable(c, positions.get(c));
        if (off < 0) throw new IllegalStateException(name + ": no fetchable record on " + c);
        SimBroker.Entry e = world.broker.entry(c, off);
        InboundRecord inbound = new InboundRecord(
                c, off, e.clock() == null ? null : e.clock().copy(),
                e.senderId(), e.senderSeq(), e.key(), e.value(), e.timestamp());
        boolean business = e.kind() == SimBroker.Kind.BUSINESS;
        transactionalStep(c, off + 1, crash, () -> {
            List<Delivery> out = protocol.onRecord(inbound);
            if (business) {
                outstandingHolds++;
                if (out.stream().noneMatch(d -> d.channel().equals(c) && d.offset() == off)) {
                    world.totalHolds++;
                }
            }
            return out;
        });
    }

    /** Consumer position advances past trailing markers, and may release held records. */
    void stepPositionAdvance(Channel c, boolean crash) {
        long newPosition = world.broker.endOffset(c);
        transactionalStep(c, newPosition, crash, () -> protocol.positionAdvance(c, newPosition));
    }

    /**
     * Emits one tick, as the adapter's wall-clock punctuator does: stamped at the ordinary
     * send door with the task's current clock and appended to the node's own tick partition,
     * inside the step's transaction. It consumes nothing, so no consumer position moves. The
     * tick returns to this node later as an ordinary fetch.
     */
    void stepTick(boolean crash) {
        if (tickChannel == null) throw new IllegalStateException(name + ": no ticks declared");
        transactionalStep(null, -1, crash, () -> {
            byte[] key = java.nio.ByteBuffer.allocate(4).putInt(config.taskPartition()).array();
            sendTo(tickChannel, key, "0".getBytes(), world.tickTimestamp());
            return List.of();
        });
    }

    private void transactionalStep(Channel c, long newPosition, boolean crash,
                                   java.util.function.Supplier<List<Delivery>> action) {
        world.oracle.snapshot(name);
        touchedThisStep.clear();
        appendedCoords.clear();
        snapOutstandingHolds = outstandingHolds;

        boolean failed = false;
        try {
            for (Delivery d : action.get()) {
                outstandingHolds--;
                Long recordId = world.oracle.recordIdAt(d.channel(), d.offset());
                if (recordId == null) {
                    throw new AssertionError(name + ": delivered a non-business coordinate "
                            + d.channel() + "@" + d.offset());
                }
                world.oracle.onDelivery(name, config.consumed(), recordId);
                world.totalDeliveries++;
                // A tick returns through the gate like any record and runs the stage's tick
                // logic, exactly as the adapter's delivery loop branches on the tick channel.
                if (d.channel().equals(tickChannel)) {
                    world.totalTicksDelivered++;
                    tickBehavior.process(this, d);
                } else {
                    behavior.process(this, d);
                }
            }
        } catch (AssertionError ae) {
            throw ae;
        } catch (RuntimeException ex) {
            failed = true;
            world.taskFailure(name, ex);
        }

        if (crash || failed) {
            for (Object[] coord : appendedCoords) {
                world.broker.markAborted((Channel) coord[0], (Long) coord[1]);
            }
            world.broker.appendMarkers(touchedThisStep);
            // The broker side of the abort is reality either way; the faulty host's bug is
            // local — its store keeps the transaction the broker rolled back.
            if (fault == HostFault.COMMIT_STORE_ON_CRASH) {
                store.commit();
            } else {
                store.discard();
            }
            world.oracle.abort(name);
            outstandingHolds = snapOutstandingHolds;
            up = false;
        } else {
            world.broker.appendMarkers(touchedThisStep);
            store.commit();
            // A tick step consumes nothing, so it moves no consumer position.
            if (c != null) positions.put(c, newPosition);
            world.oracle.commit(name);
        }
    }

    /** Called by behaviors: forward one business record, stamped and tagged by the protocol. */
    void sendBusiness(String topic, byte[] key, byte[] value, long timestamp) {
        sendTo(world.routeByKey(topic, key), key, value, timestamp);
    }

    /** Sends to an already-chosen channel: ticks name their own partition rather than route. */
    void sendTo(Channel dest, byte[] key, byte[] value, long timestamp) {
        var stamp = protocol.prepareSend(dest);
        // Every offset claim must name a really-appended offset — the property the liveness
        // argument rests on (an unappended claim can wedge a gate forever).
        stamp.clock().forEach((ch, off) -> {
            if (off >= world.broker.endOffset(ch)) {
                throw new AssertionError(name + ": stamp claims unappended offset " + ch + "@" + off
                        + " (end " + world.broker.endOffset(ch) + ")");
            }
        });
        long offset = world.broker.endOffset(dest);
        long id = world.oracle.emitted(name, dest, offset);
        long assigned = world.broker.append(dest, new SimBroker.Entry(
                SimBroker.Kind.BUSINESS, id, stamp.clock(), stamp.senderId(), stamp.senderSeq(),
                key, value, timestamp));
        if (assigned != offset) throw new IllegalStateException("offset race in single-threaded sim");
        touchedThisStep.add(dest);
        appendedCoords.add(new Object[] {dest, offset});
    }

    long position(Channel c) {
        return positions.get(c);
    }
}
