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
    private final Map<String, Integer> partitionCounts;
    private final Map<String, String> cleanupPolicies;
    private final Map<String, Map<Integer, Long>> endOffsets;

    private TestTopicAdmin(Map<String, Uuid> topicIds, Map<String, Integer> partitionCounts,
            Map<String, String> cleanupPolicies, Map<String, Map<Integer, Long>> endOffsets) {
        this.topicIds = topicIds;
        this.partitionCounts = partitionCounts;
        this.cleanupPolicies = cleanupPolicies;
        this.endOffsets = endOffsets;
    }

    static TestTopicAdmin of(Map<String, Uuid> topicIds) {
        return new TestTopicAdmin(Map.copyOf(topicIds), Map.of(), Map.of(), Map.of());
    }

    /**
     * Resolves the given UUIDs and reports the given per-topic partition counts, for exercising the
     * co-partitioning parity check. Topics absent from {@code partitionCounts} report a count of 1.
     */
    static TestTopicAdmin of(Map<String, Uuid> topicIds, Map<String, Integer> partitionCounts) {
        return new TestTopicAdmin(Map.copyOf(topicIds), Map.copyOf(partitionCounts), Map.of(), Map.of());
    }

    /**
     * Resolves the given UUIDs, reports the given per-topic partition counts, and reports the given
     * per-topic {@code cleanup.policy}, for exercising the sink cleanup-policy check. Topics absent
     * from {@code cleanupPolicies} report {@code "delete"}.
     */
    static TestTopicAdmin of(Map<String, Uuid> topicIds, Map<String, Integer> partitionCounts,
            Map<String, String> cleanupPolicies) {
        return new TestTopicAdmin(Map.copyOf(topicIds), Map.copyOf(partitionCounts),
                Map.copyOf(cleanupPolicies), Map.of());
    }

    /**
     * A copy of this double that also reports the given per-topic, per-partition end offsets, for
     * exercising the {@code ownOutputs} init-time seed. Topics absent from {@code endOffsets}
     * report an empty topic (every partition at end offset 0 — nothing appended, nothing seeded).
     */
    TestTopicAdmin withEndOffsets(Map<String, Map<Integer, Long>> endOffsets) {
        return new TestTopicAdmin(topicIds, partitionCounts, cleanupPolicies, Map.copyOf(endOffsets));
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
        topics.forEach(t -> counts.put(t, partitionCounts.getOrDefault(t, 1)));
        return counts;
    }

    @Override
    public Map<String, String> cleanupPolicies(List<String> topics) {
        Map<String, String> policies = new HashMap<>();
        topics.forEach(t -> policies.put(t, cleanupPolicies.getOrDefault(t, "delete")));
        return policies;
    }

    @Override
    public Map<Integer, Long> endOffsets(String topic) {
        return endOffsets.getOrDefault(topic, Map.of());
    }

    @Override
    public void close() {
        // nothing to close
    }
}
