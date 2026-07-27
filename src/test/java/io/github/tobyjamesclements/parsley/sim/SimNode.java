package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.Delivery;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;
import io.github.tobyjamesclements.parsley.core.InboundRecord;
import io.github.tobyjamesclements.parsley.core.NodeConfig;
import io.github.tobyjamesclements.parsley.core.NullEmission;
import io.github.tobyjamesclements.parsley.core.ProcessResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Hosts one {@link DeliveryProtocol} instance the way the Kafka Streams adapter would: fetches
 * records in per-channel offset order, runs the user behavior on each delivery, performs sends
 * with protocol stamps, emits null messages on request, and commits everything step-atomically.
 * A step can be aborted mid-flight to model a crash inside an EOS transaction.
 */
final class SimNode {

    final String name;
    private final SimWorld world;
    private final SimBehavior behavior;
    private final BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory;

    NodeConfig config;
    DeliveryProtocol protocol;
    final SimStateStore store = new SimStateStore();
    final SimSendTracker sends;
    boolean up;

    /** Committed consumer positions (next offset to fetch). */
    private final Map<Channel, Long> positions = new HashMap<>();
    /** Appends made by the in-flight step, unwound to ABORTED on a crash. */
    private final List<Object[]> appendedCoords = new ArrayList<>(); // {Channel, Long offset}
    private final Set<Channel> touchedThisStep = new HashSet<>();

    SimNode(String name, SimWorld world, NodeConfig config, SimBehavior behavior,
            BiFunction<NodeConfig, SimNode, DeliveryProtocol> protocolFactory) {
        this.name = name;
        this.world = world;
        this.config = config;
        this.behavior = behavior;
        this.protocolFactory = protocolFactory;
        this.sends = new SimSendTracker(world.broker);
    }

    /** (Re)constructs the protocol from committed state, as a process start does. */
    void start() {
        protocol = protocolFactory.apply(config, this);
        up = true;
        Map<Channel, Long> resume = protocol.resumePositions();
        for (Channel c : config.consumed()) {
            long committed = positions.getOrDefault(c, 0L);
            long position = Math.max(committed, resume.getOrDefault(c, 0L));
            positions.put(c, position);
            world.oracle.subscribed(name, c, position);
        }
    }

    void crashIdle() {
        up = false;
        sends.reset();
        store.discard();
    }

    boolean hasWork(Channel c) {
        return up && world.broker.nextFetchable(c, positions.get(c)) >= 0 && !protocol.pauseWanted(c);
    }

    /**
     * Runs one step: fetch the next record on {@code c}, process to fixpoint, then commit — or
     * abort everything when {@code crash} is set, leaving aborted records at their offsets.
     */
    void step(Channel c, boolean crash) {
        long off = world.broker.nextFetchable(c, positions.get(c));
        if (off < 0) throw new IllegalStateException(name + ": no fetchable record on " + c);
        SimBroker.Entry e = world.broker.entry(c, off);

        world.oracle.snapshot(name);
        touchedThisStep.clear();
        appendedCoords.clear();

        boolean failed = false;
        try {
            InboundRecord inbound = new InboundRecord(
                    c, off, e.clock() == null ? null : e.clock().copy(),
                    e.kind() == SimBroker.Kind.NULL_MESSAGE, e.key(), e.value(), e.timestamp());
            ProcessResult result = protocol.onRecord(inbound);
            List<NullEmission> emissions = new ArrayList<>(result.emissions());
            for (Delivery d : result.deliveries()) {
                Long recordId = world.oracle.recordIdAt(d.channel(), d.offset());
                if (recordId == null) {
                    throw new AssertionError(name + ": delivered a non-business coordinate "
                            + d.channel() + "@" + d.offset());
                }
                world.oracle.onDelivery(name, config.consumed(), recordId);
                int forwards = behavior.process(this, d);
                protocol.afterDelivery(d, forwards).ifPresent(emissions::add);
            }
            for (NullEmission em : emissions) {
                sendNulls(em);
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
            positions.put(c, off + 1);
            world.oracle.commit(name);
        }
    }

    /** Called by behaviors: forward one business record, stamped by the protocol. */
    void sendBusiness(String topic, byte[] key, byte[] value, long timestamp) {
        Channel dest = world.routeByKey(topic, key);
        Clock stamp = protocol.stampForSend(dest);
        long offset = world.broker.endOffset(dest);
        long id = world.oracle.emitted(name, dest, offset);
        long assigned = world.broker.append(dest,
                new SimBroker.Entry(SimBroker.Kind.BUSINESS, id, stamp, key, value, timestamp));
        if (assigned != offset) throw new IllegalStateException("offset race in single-threaded sim");
        sends.sent(dest, offset);
        touchedThisStep.add(dest);
        appendedCoords.add(new Object[] {dest, offset});
    }

    private void sendNulls(NullEmission em) {
        for (java.util.UUID sink : config.sinkTopics()) {
            Channel dest = world.ownPartition(sink, config.taskPartition());
            Clock stamp = protocol.stampForSend(dest);
            long offset = world.broker.append(dest,
                    new SimBroker.Entry(SimBroker.Kind.NULL_MESSAGE, -1, stamp, em.key(), null, em.timestamp()));
            sends.sent(dest, offset);
            touchedThisStep.add(dest);
            appendedCoords.add(new Object[] {dest, offset});
        }
    }

    long position(Channel c) {
        return positions.get(c);
    }
}
