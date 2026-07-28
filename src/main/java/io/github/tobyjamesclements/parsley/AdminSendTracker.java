package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Production {@link SendTracker}: end offsets from an admin client for the init-time
 * own-outputs seed; no acknowledgement feed. The node's own outputs are therefore claimed in
 * sequence space for the task's lifetime — sound and non-blocking (the simulator's
 * dropped-acks obligation), at the cost of one sequence entry per sink channel in every
 * stamp and the late-joiner normalisation caveat.
 *
 * <p>Streams does not expose per-task producer acknowledgements: a producer interceptor
 * attributes them per thread, and a co-threaded sibling task's acknowledgement on a shared
 * sink partition would inflate this task's acked count and make self-normalisation
 * under-claim. Exact attribution (a producer-wrapping client supplier correlating each send's
 * sender tag with its callback metadata) would restore offset upgrades.
 */
final class AdminSendTracker implements SendTracker {

    private final TopicIds topicIds;
    private final Admin admin;
    private final Map<String, UUID> sinkIdsByName = new HashMap<>();

    AdminSendTracker(TopicIds topicIds, Admin admin, Set<String> sinkTopics) {
        this.topicIds = topicIds;
        this.admin = admin;
        for (String t : sinkTopics) {
            sinkIdsByName.put(t, topicIds.resolve(t).id());
        }
    }

    @Override
    public List<Ack> drainAcks() {
        return List.of();
    }

    @Override
    public Map<Channel, Long> endOffsets(Set<UUID> sinkTopics) {
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        Map<TopicPartition, Channel> channels = new HashMap<>();
        sinkIdsByName.forEach((name, id) -> {
            if (!sinkTopics.contains(id)) return;
            int partitions = topicIds.resolve(name).partitions();
            for (int p = 0; p < partitions; p++) {
                TopicPartition tp = new TopicPartition(name, p);
                query.put(tp, OffsetSpec.latest());
                channels.put(tp, new Channel(id, p));
            }
        });
        try {
            Map<Channel, Long> out = new HashMap<>();
            var result = admin.listOffsets(query, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED));
            for (var e : result.all().get().entrySet()) {
                out.put(channels.get(e.getKey()), e.getValue().offset());
            }
            return out;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted resolving sink end offsets", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("cannot resolve sink end offsets", e.getCause());
        }
    }

    @Override
    public EarliestOffsets earliestOffsets(Set<Channel> channels) {
        Map<UUID, List<Channel>> byTopic = new HashMap<>();
        for (Channel c : channels) {
            byTopic.computeIfAbsent(c.topicId(), k -> new java.util.ArrayList<>()).add(c);
        }
        Map<UUID, String> names = new HashMap<>();
        Set<Channel> absent = new java.util.HashSet<>();
        var described = admin.describeTopics(
                org.apache.kafka.common.TopicCollection.ofTopicIds(
                        byTopic.keySet().stream()
                                .map(id -> new org.apache.kafka.common.Uuid(
                                        id.getMostSignificantBits(), id.getLeastSignificantBits()))
                                .toList()));
        described.topicIdValues().forEach((kafkaId, future) -> {
            UUID id = new UUID(kafkaId.getMostSignificantBits(), kafkaId.getLeastSignificantBits());
            try {
                names.put(id, future.get().name());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted resolving topic names", e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicIdException) {
                    // Definitive absence: a recreated topic is a different channel, so these
                    // claims are unclaimable forever.
                    absent.addAll(byTopic.get(id));
                } else {
                    throw new IllegalStateException("cannot resolve topic id " + id, e.getCause());
                }
            }
        });

        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        Map<TopicPartition, Channel> tps = new HashMap<>();
        byTopic.forEach((id, chans) -> {
            String name = names.get(id);
            if (name == null) return;
            for (Channel c : chans) {
                TopicPartition tp = new TopicPartition(name, c.partition());
                query.put(tp, OffsetSpec.earliest());
                tps.put(tp, c);
            }
        });
        Map<Channel, Long> logStarts = new HashMap<>();
        try {
            var result = admin.listOffsets(query, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED));
            for (var e : result.all().get().entrySet()) {
                logStarts.put(tps.get(e.getKey()), e.getValue().offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted resolving log starts", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("cannot resolve log starts", e.getCause());
        }
        return new EarliestOffsets(logStarts, absent);
    }
}
