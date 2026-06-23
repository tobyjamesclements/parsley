package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ParsleyTopicAdmin} test double that resolves topic UUIDs from a fixed map, for tests
 * running under {@code TopologyTestDriver} with no broker. Inject it into a builder with the
 * package-private {@code topicAdmin(...)} seam so the causal processor resolves the same stable
 * UUIDs the test asserts on.
 */
final class TestTopicAdmin implements ParsleyTopicAdmin {

    private final Map<String, Uuid> topicIds;

    private TestTopicAdmin(Map<String, Uuid> topicIds) {
        this.topicIds = topicIds;
    }

    static TestTopicAdmin of(Map<String, Uuid> topicIds) {
        return new TestTopicAdmin(Map.copyOf(topicIds));
    }

    @Override
    public Map<String, Uuid> topicIds(List<String> topics) {
        Map<String, Uuid> resolved = new HashMap<>();
        for (String topic : topics) {
            Uuid id = topicIds.get(topic);
            if (id == null) {
                throw new IllegalStateException("TestTopicAdmin has no UUID for topic '" + topic + "'");
            }
            resolved.put(topic, id);
        }
        return resolved;
    }

    @Override
    public Map<String, Integer> partitionCounts(List<String> topics) {
        Map<String, Integer> counts = new HashMap<>();
        topics.forEach(t -> counts.put(t, 1));
        return counts;
    }

    @Override
    public void createTopic(String name, int partitions) {
        // no broker in tests
    }

    @Override
    public void close() {
        // nothing to close
    }
}
