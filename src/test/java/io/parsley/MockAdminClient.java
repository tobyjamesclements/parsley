package io.parsley;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link TopicAdmin} that substitutes deterministic name-derived UUIDs (via
 * {@link CausalPosition#nameUuid}) for every topic's Kafka-assigned UUID.
 *
 * <p>This makes {@link CausalDependencies#advance(org.apache.kafka.common.TopicPartition, long)}
 * — which also uses {@code nameUuid} internally — resolve to the same UUID that the consumer's
 * frontier uses, so cross-topic causal dependencies drain naturally in integration tests rather
 * than waiting for the eviction limit.
 *
 * <h2>Constructors</h2>
 * <dl>
 *   <dt>{@link #MockAdminClient(TopicAdmin)} — for ITs with a real broker</dt>
 *   <dd>Delegates {@code createTopic} and partition-count reads to the real admin, but replaces
 *       each topic's UUID with {@code nameUuid}. Partition counts are therefore accurate for
 *       outbox sizing. Close the delegate by passing the result of
 *       {@link TopicAdmin#ofBootstrap(String)}.</dd>
 *   <dt>{@link #MockAdminClient()} — for unit tests without a real broker</dt>
 *   <dd>All operations are no-ops or return minimal stubs. {@code describeTopics} returns one
 *       partition per topic (enough for the outbox sizing calculation) with a {@code nameUuid}.
 *       {@code createTopic} is a no-op.</dd>
 * </dl>
 */
final class MockAdminClient implements TopicAdmin {

    private final TopicAdmin delegate;

    MockAdminClient(TopicAdmin delegate) {
        this.delegate = delegate;
    }

    MockAdminClient() {
        this.delegate = null;
    }

    @Override
    public Map<String, TopicDescription> describeTopics(List<String> topics) throws Exception {
        Map<String, TopicDescription> result = new HashMap<>();
        if (delegate != null) {
            Map<String, TopicDescription> real = delegate.describeTopics(topics);
            for (Map.Entry<String, TopicDescription> entry : real.entrySet()) {
                String name = entry.getKey();
                TopicDescription desc = entry.getValue();
                Uuid nameUuid = CausalPosition.nameUuid(name);
                result.put(name, new TopicDescription(
                        name, desc.isInternal(), desc.partitions(), desc.authorizedOperations(), nameUuid));
            }
        } else {
            for (String topic : topics) {
                Uuid nameUuid = CausalPosition.nameUuid(topic);
                result.put(topic, new TopicDescription(topic, false, List.of(), null, nameUuid));
            }
        }
        return result;
    }

    @Override
    public void createTopic(String name, int partitions) throws Exception {
        if (delegate != null) {
            delegate.createTopic(name, partitions);
        }
    }

    @Override
    public void close() throws Exception {
        if (delegate != null) {
            delegate.close();
        }
    }
}
