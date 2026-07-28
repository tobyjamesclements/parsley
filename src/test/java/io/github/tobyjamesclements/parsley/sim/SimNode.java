package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.Delivery;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;
import io.github.tobyjamesclements.parsley.core.InboundRecord;
import io.github.tobyjamesclements.parsley.core.NodeConfig;

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
 */
final class SimNode {

    final String name;
    private final SimWorld world;
    private SimBehavior behavior;
    private final BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory;

    NodeConfig config;
    DeliveryProtocol protocol;
    final SimStateStore store = new SimStateStore();
    final SimSendTracker sends;
    boolean up;

    /** Committed consumer positions (next offset to fetch). */
    private final Map<Channel, Long> positions = new HashMap<>();
    private final List<Object[]> appendedCoords = new ArrayList<>(); // {Channel, Long offset}
    private final Set<Channel> touchedThisStep = new HashSet<>();

    SimNode(String name, SimWorld world, NodeConfig config, SimBehavior behavior,
            BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory) {
        this.name = name;
        this.world = world;
        this.config = config;
        this.behavior = behavior;
        this.protocolFactory = protocolFactory;
        this.sends = new SimSendTracker(world.broker, world.dropAcks);
    }

    /** Joins at the current log end on every consumed channel (a `latest` consumer). */
    void joinAtLatest() {
        for (Channel c : config.consumed()) {
            positions.put(c, world.broker.endOffset(c));
        }
    }

    /** (Re)constructs the protocol from committed state, as a process start does. */
    void start() {
        protocol = protocolFactory.apply(config, this);
        store.commit(); // init-time writes (scope record, rescope) are idempotent re-runs
        up = true;
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
        sends.reset();
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
        if (!up) return false;
        long pos = positions.get(c);
        return world.broker.nextFetchable(c, pos) < 0 && world.broker.endOffset(c) > pos;
    }

    /** Runs one fetch-process-commit step; {@code crash} aborts it mid-flight instead. */
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
            if (business && out.stream().noneMatch(d -> d.channel().equals(c) && d.offset() == off)) {
                world.totalHolds++;
            }
            return out;
        });
    }

    /** Consumer position advances past trailing markers; may release held records. */
    void stepPositionAdvance(Channel c, boolean crash) {
        long newPosition = world.broker.endOffset(c);
        transactionalStep(c, newPosition, crash, () -> protocol.positionAdvance(c, newPosition));
    }

    private void transactionalStep(Channel c, long newPosition, boolean crash,
                                   java.util.function.Supplier<List<Delivery>> action) {
        world.oracle.snapshot(name);
        touchedThisStep.clear();
        appendedCoords.clear();

        boolean failed = false;
        try {
            for (Delivery d : action.get()) {
                Long recordId = world.oracle.recordIdAt(d.channel(), d.offset());
                if (recordId == null) {
                    throw new AssertionError(name + ": delivered a non-business coordinate "
                            + d.channel() + "@" + d.offset());
                }
                world.oracle.onDelivery(name, config.consumed(), recordId);
                world.totalDeliveries++;
                behavior.process(this, d);
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
            store.discard();
            world.oracle.abort(name);
            sends.reset();
            up = false;
        } else {
            world.broker.appendMarkers(touchedThisStep);
            store.commit();
            positions.put(c, newPosition);
            world.oracle.commit(name);
        }
    }

    /** Called by behaviors: forward one business record, stamped and tagged by the protocol. */
    void sendBusiness(String topic, byte[] key, byte[] value, long timestamp) {
        Channel dest = world.routeByKey(topic, key);
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
        sends.sent(dest, offset);
        touchedThisStep.add(dest);
        appendedCoords.add(new Object[] {dest, offset});
    }

    long position(Channel c) {
        return positions.get(c);
    }
}
