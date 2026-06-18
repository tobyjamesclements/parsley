package io.parsley;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ParsleyTopicAdmin} that substitutes deterministic name-derived UUIDs (via
 * {@link CausalPosition#deriveUuid}) for every topic's Kafka-assigned UUID.
 *
 * <p>This ensures that UUIDs passed to {@link CausalDependencies#builder()} via
 * {@link CausalPosition#deriveUuid} match the consumer's frontier, so cross-topic causal
 * dependencies drain naturally in integration tests rather than waiting for the eviction limit.
 *
 * <h2>Constructors</h2>
 * <dl>
 *   <dt>{@link #MockAdminClient(ParsleyTopicAdmin)} — for ITs with a real broker</dt>
 *   <dd>Delegates {@code createTopic} and partition-count reads to the real admin, but replaces
 *       each topic's UUID with {@code deriveUuid}. Partition counts are therefore accurate for
 *       outbox sizing. Close the delegate by passing the result of
 *       {@link ParsleyTopicAdmin#ofBootstrap(String)}.</dd>
 *   <dt>{@link #MockAdminClient()} — for unit tests without a real broker</dt>
 *   <dd>All operations are no-ops or return minimal stubs. {@code describeTopics} returns one
 *       partition per topic (enough for the outbox sizing calculation) with a {@code deriveUuid}.
 *       {@code createTopic} is a no-op.</dd>
 * </dl>
 */
final class MockAdminClient implements ParsleyTopicAdmin {

    private final ParsleyTopicAdmin delegate;

    MockAdminClient(ParsleyTopicAdmin delegate) {
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
                Uuid deriveUuid = CausalPosition.deriveUuid(name);
                result.put(name, new TopicDescription(
                        name, desc.isInternal(), desc.partitions(), desc.authorizedOperations(), deriveUuid));
            }
        } else {
            for (String topic : topics) {
                Uuid deriveUuid = CausalPosition.deriveUuid(topic);
                result.put(topic, new TopicDescription(topic, false, List.of(), null, deriveUuid));
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
