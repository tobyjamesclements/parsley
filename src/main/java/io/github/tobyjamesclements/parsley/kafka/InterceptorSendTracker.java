package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.core.CausalSendException;
import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.SendTracker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Production {@link SendTracker}: acknowledgements come from {@link OwnOutputs.Interceptor} on
 * the task's stream-thread producer; end offsets come from an admin client at init.
 *
 * <p>The crossing wait spins on the producer's pending count with a deadline. Under EOS the
 * producer is flushing continuously (linger is the only slack), so the wait resolves in normal
 * operation within an ack round trip; on an observed send failure or deadline it throws, and
 * the task dies with its transaction (fail closed).
 */
final class InterceptorSendTracker implements SendTracker {

    private final OwnOutputs.Entry entry;
    private final TopicIds topicIds;
    private final Admin admin;
    private final Duration quiescenceDeadline;
    /** Sink topic name to UUID, for translating acknowledgement metadata into channels. */
    private final Map<String, UUID> sinkIdsByName = new HashMap<>();

    InterceptorSendTracker(String producerClientId, TopicIds topicIds, Admin admin,
                           Set<String> sinkTopics, Duration quiescenceDeadline) {
        this.entry = OwnOutputs.entry(producerClientId);
        this.topicIds = topicIds;
        this.admin = admin;
        this.quiescenceDeadline = quiescenceDeadline;
        for (String t : sinkTopics) {
            sinkIdsByName.put(t, topicIds.resolve(t).id());
        }
    }

    @Override
    public List<Ack> drainAcks() {
        List<Ack> out = new java.util.ArrayList<>();
        RecordMetadata m;
        while ((m = entry.acks.poll()) != null) {
            UUID id = sinkIdsByName.get(m.topic());
            if (id != null && m.hasOffset()) {
                out.add(new Ack(new Channel(id, m.partition()), m.offset()));
            }
        }
        return out;
    }

    @Override
    public void awaitQuiescence(Set<Channel> except) {
        long deadline = System.nanoTime() + quiescenceDeadline.toNanos();
        while (entry.pending.get() > 0) {
            Exception failure = entry.failure.get();
            if (failure != null) {
                throw new CausalSendException("send failure observed during crossing wait", failure);
            }
            if (System.nanoTime() > deadline) {
                throw new CausalSendException("crossing wait exceeded " + quiescenceDeadline
                        + " with " + entry.pending.get() + " unacknowledged sends");
            }
            Thread.onSpinWait();
        }
        Exception failure = entry.failure.get();
        if (failure != null) {
            throw new CausalSendException("send failure observed during crossing wait", failure);
        }
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
            var result = admin.listOffsets(query,
                    new org.apache.kafka.clients.admin.ListOffsetsOptions(IsolationLevel.READ_COMMITTED));
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
}
