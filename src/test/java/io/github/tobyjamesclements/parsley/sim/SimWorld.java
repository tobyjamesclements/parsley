package io.github.tobyjamesclements.parsley.sim;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.ChannelId;

/**
 * The simulated substrate: channels, positions, transactions and their failure modes.
 */
public final class SimWorld {
    public static final class SimChannel {
        final ChannelId id;
        final String name;
        final String topicName;
        final List<Slot> slots = new ArrayList<>();
        long logStart;
        boolean dead;

        SimChannel(ChannelId id, String name, String topicName) {
            this.id = id;
            this.name = name;
            this.topicName = topicName;
        }

        public ChannelId id() {
            return id;
        }
    }

    sealed interface Slot {
    }

    record MessageSlot(Instance instance) implements Slot {
    }

    record PendingSlot(Object txn, Instance instance) implements Slot {
    }

    record DeadSlot() implements Slot {
    }

    private final Map<ChannelId, SimChannel> channels = new LinkedHashMap<>();

    private final Map<String, SimChannel> currentByName = new LinkedHashMap<>();
    private final Random rng;

    public SimWorld(long seed) {
        this.rng = new Random(seed ^ 0x5ee_d5ee_d5eeL);
    }

    public SimChannel createChannel(String name) {
        return createTopic(name, 1).get(0);
    }

    public List<SimChannel> createTopic(String name, int partitions) {
        UUID topicId = new UUID(rng.nextLong(), rng.nextLong() & Long.MAX_VALUE);
        List<SimChannel> created = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            ChannelId id = new ChannelId(topicId, partition);
            SimChannel channel = new SimChannel(id, partitions == 1 ? name : name + "#" + partition, name);
            channels.put(id, channel);
            currentByName.put(channel.name, channel);
            created.add(channel);
        }
        return created;
    }

    public List<SimChannel> recreateTopic(SimChannel channel) {
        int partitions = 0;
        for (SimChannel candidate : channels.values()) {
            if (candidate.id.topicId().equals(channel.id.topicId())) {
                candidate.dead = true;
                partitions++;
            }
        }
        return createTopic(channel.topicName, partitions);
    }

    public SimChannel currentByName(String name) {
        SimChannel current = currentByName.get(name);
        return current == null || current.dead ? null : current;
    }

    public SimChannel channel(ChannelId id) {
        return channels.get(id);
    }

    public Iterable<SimChannel> allChannels() {
        return channels.values();
    }

    long appendPending(SimChannel channel, Object txn, InstanceFactory factory) {
        long position = channel.slots.size();
        Instance instance = factory.at(channel.id, position);
        channel.slots.add(new PendingSlot(txn, instance));
        return position;
    }

    public Instance appendExternal(SimChannel channel, InstanceFactory factory) {
        long position = channel.slots.size();
        Instance instance = factory.at(channel.id, position);
        channel.slots.add(new MessageSlot(instance));
        return instance;
    }

    public void appendDead(SimChannel channel) {
        channel.slots.add(new DeadSlot());
    }

    void commitTxn(Object txn) {
        forEachPending(txn, (channel, index, pending) -> channel.slots.set(index, new MessageSlot(pending.instance())));
    }

    void abortTxn(Object txn) {
        forEachPending(txn, (channel, index, pending) -> channel.slots.set(index, new DeadSlot()));
    }

    private interface PendingVisitor {
        void visit(SimChannel channel, int index, PendingSlot pending);
    }

    private void forEachPending(Object txn, PendingVisitor visitor) {
        for (SimChannel channel : channels.values()) {
            for (int i = 0; i < channel.slots.size(); i++) {
                if (channel.slots.get(i) instanceof PendingSlot pending && pending.txn() == txn) {
                    visitor.visit(channel, i, pending);
                }
            }
        }
    }

    public void truncate(SimChannel channel, long newLogStart) {
        channel.logStart = Math.max(channel.logStart, newLogStart);
    }

    public void killChannel(SimChannel channel) {
        for (SimChannel candidate : channels.values()) {
            if (candidate.id.topicId().equals(channel.id.topicId())) {
                candidate.dead = true;
            }
        }
    }

    public long lso(SimChannel channel) {
        for (int i = (int) channel.logStart; i < channel.slots.size(); i++) {
            if (channel.slots.get(i) instanceof PendingSlot) {
                return i;
            }
        }
        return channel.slots.size();
    }

    public long lastAssigned(SimChannel channel) {
        return channel.slots.size() - 1;
    }

    Slot slot(SimChannel channel, long position) {
        return channel.slots.get((int) position);
    }

    @FunctionalInterface
    public interface InstanceFactory {
        Instance at(ChannelId channel, long position);
    }
}
