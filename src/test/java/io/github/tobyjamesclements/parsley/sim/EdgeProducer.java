package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * A scripted plain Kafka producer/consumer at the topology's edge. It stamps outbound records
 * the way the edge ops do: a running clock built from observed records ({@code observe}) plus
 * its own acknowledged sends, resolved before each send (a sequential producer waits for its
 * previous acknowledgement, so its own claims are always current).
 */
final class EdgeProducer {

    sealed interface Op {
        /** Produce one business record. */
        record Produce(String topic, Integer partition, byte[] key, byte[] value, long timestamp) implements Op {}

        /** Consume one record from a channel, folding it into the running clock. */
        record PollOne(String topic, int partition) implements Op {}
    }

    final String name;
    private final SimWorld world;
    private final Deque<Op> script = new ArrayDeque<>();
    private final Clock observed = new Clock();
    private final Map<Channel, Long> positions = new HashMap<>();

    EdgeProducer(String name, SimWorld world) {
        this.name = name;
        this.world = world;
    }

    EdgeProducer produce(String topic, Integer partition, String key, String value, long timestamp) {
        script.add(new Op.Produce(topic, partition,
                key == null ? null : key.getBytes(), value == null ? null : value.getBytes(), timestamp));
        return this;
    }

    EdgeProducer pollOne(String topic, int partition) {
        script.add(new Op.PollOne(topic, partition));
        return this;
    }

    boolean hasWork() {
        Op next = script.peek();
        if (next == null) return false;
        if (next instanceof Op.PollOne p) {
            Channel c = world.broker.channel(p.topic(), p.partition());
            return world.broker.nextFetchable(c, positions.getOrDefault(c, 0L)) >= 0;
        }
        return true;
    }

    /** Runs the next scripted op. */
    void step() {
        Op op = script.poll();
        if (op instanceof Op.Produce p) {
            Channel dest = p.partition() != null
                    ? world.broker.channel(p.topic(), p.partition())
                    : world.routeByKey(p.topic(), p.key());
            long offset = world.broker.endOffset(dest);
            long id = world.oracle.emitted(name, dest, offset);
            world.broker.append(dest, new SimBroker.Entry(
                    SimBroker.Kind.BUSINESS, id, observed.copy(), null, -1, p.key(), p.value(), p.timestamp()));
            // A sequential plain producer learns its own offset from the acknowledgement it
            // waits for; fold it so the next send claims this one.
            observed.advanceTo(dest, offset);
        } else if (op instanceof Op.PollOne p) {
            Channel c = world.broker.channel(p.topic(), p.partition());
            long from = positions.getOrDefault(c, 0L);
            long off = world.broker.nextFetchable(c, from);
            if (off < 0) throw new IllegalStateException(name + ": poll on empty " + c);
            SimBroker.Entry e = world.broker.entry(c, off);
            if (e.clock() != null) observed.mergeMax(e.clock());
            observed.advanceTo(c, off);
            if (e.kind() == SimBroker.Kind.BUSINESS) {
                world.oracle.observed(name, e.recordId());
            }
            positions.put(c, off + 1);
        }
    }
}
