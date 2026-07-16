package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A Facade (GoF) narrowing the Kafka {@link Admin} operations {@link ParsleyOffsetSeeder} performs before
 * a {@link CausalStreams} instance starts: enumerating topics (to detect surviving Parsley state), reading
 * a source topic's partitions, reading the consumer group's committed offsets, reading a partition's
 * log-start (earliest) offset, and committing a seed offset for a partition that has none. Kept narrow so
 * a test can implement it with a hand-rolled double rather than the full ~40-method {@link Admin} surface
 * (Parsley uses no mock framework).
 *
 * <p>Separate from {@link ParsleyTopicAdmin} — which the processor path uses at {@code init()} for
 * topic-describe validation — because this is a different concern (consumer-group offset seeding at
 * startup) with a different, disjoint set of operations; folding them together would force every
 * describe-only test double to also implement offset seeding.
 */
interface ParsleySeedAdmin extends AutoCloseable {

    /** Every topic name the broker currently knows — used to detect surviving Parsley changelog topics. */
    Set<String> listTopicNames() throws Exception;

    /**
     * The partition count for each of {@code topics}. Throws if any topic does not exist — a causal source
     * that is absent at startup must fail loudly, not be silently skipped.
     */
    Map<String, Integer> partitionCounts(Collection<String> topics) throws Exception;

    /**
     * The committed offset for every partition {@code groupId} has one for; partitions with no committed
     * offset are simply absent from the result (never present with a null value). An empty map means the
     * group has no committed offsets at all (a never-run group, or one whose offsets have expired).
     */
    Map<TopicPartition, Long> committedOffsets(String groupId) throws Exception;

    /** The log-start (earliest) offset for each of {@code partitions}. */
    Map<TopicPartition, Long> earliestOffsets(Collection<TopicPartition> partitions) throws Exception;

    /**
     * Commits {@code offsets} for {@code groupId} as a starting position, one entry per partition. The
     * returned map carries the per-partition outcome — {@code null} for success, the failure otherwise —
     * because {@link Admin#alterConsumerGroupOffsets} is not transactional and may succeed for some
     * partitions while failing for others, and because it fails wholesale (typically {@code
     * UnknownMemberIdException}) once the group is no longer empty (a peer instance has already joined).
     * {@link ParsleyOffsetSeeder} inspects the outcomes per partition rather than trusting an all-or-nothing
     * result.
     */
    Map<TopicPartition, @Nullable Throwable> commitSeedOffsets(String groupId, Map<TopicPartition, Long> offsets)
            throws Exception;

    /**
     * A {@link ParsleySeedAdmin} over a real Kafka {@link Admin} built from {@code configs} (the same
     * Streams {@code props} the runtime holds, so it inherits broker security settings). Closing it closes
     * the underlying {@code Admin}.
     */
    static ParsleySeedAdmin ofConfigs(Map<String, Object> configs) {
        Admin admin = Admin.create(new HashMap<>(configs));
        return new ParsleySeedAdmin() {
            @Override
            public Set<String> listTopicNames() throws Exception {
                return admin.listTopics().names().get();
            }

            @Override
            public Map<String, Integer> partitionCounts(Collection<String> topics) throws Exception {
                Map<String, TopicDescription> descriptions =
                        admin.describeTopics(topics.stream().toList()).allTopicNames().get();
                Map<String, Integer> counts = new HashMap<>();
                descriptions.forEach((topic, desc) -> counts.put(topic, desc.partitions().size()));
                return counts;
            }

            @Override
            public Map<TopicPartition, Long> committedOffsets(String groupId) throws Exception {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
                Map<TopicPartition, Long> offsets = new HashMap<>();
                committed.forEach((tp, meta) -> {
                    // A partition with no committed offset can surface as an absent key or a null value,
                    // depending on the request shape; normalise both to "absent".
                    if (meta != null) {
                        offsets.put(tp, meta.offset());
                    }
                });
                return offsets;
            }

            @Override
            public Map<TopicPartition, Long> earliestOffsets(Collection<TopicPartition> partitions) throws Exception {
                Map<TopicPartition, OffsetSpec> request = new HashMap<>();
                partitions.forEach(tp -> request.put(tp, OffsetSpec.earliest()));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                        admin.listOffsets(request).all().get();
                Map<TopicPartition, Long> earliest = new HashMap<>();
                result.forEach((tp, info) -> earliest.put(tp, info.offset()));
                return earliest;
            }

            @Override
            public Map<TopicPartition, @Nullable Throwable> commitSeedOffsets(
                    String groupId, Map<TopicPartition, Long> offsets) throws Exception {
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
                offsets.forEach((tp, offset) -> toCommit.put(tp, new OffsetAndMetadata(offset)));
                var result = admin.alterConsumerGroupOffsets(groupId, toCommit);
                Map<TopicPartition, @Nullable Throwable> outcomes = new HashMap<>();
                for (TopicPartition tp : offsets.keySet()) {
                    KafkaFuture<Void> future = result.partitionResult(tp);
                    try {
                        future.get();
                        outcomes.put(tp, null);
                    } catch (ExecutionException e) {
                        outcomes.put(tp, e.getCause() != null ? e.getCause() : e);
                    }
                }
                return outcomes;
            }

            @Override
            public void close() {
                admin.close();
            }
        };
    }
}
