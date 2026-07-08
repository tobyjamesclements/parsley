package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyMarkerPartitioner}: the {@link StreamPartitioner} wrapper {@link CausalTopology}
 * installs on every sink, so a Parsley protocol marker always routes to the forwarding task's own owned
 * partition ({@link ParsleyMarkerPartition}) regardless of its key, while a business forward (no override
 * set) routes exactly as it would have without this wrapper installed at all.
 */
class ParsleyMarkerPartitionerTest {

    /** Guards against a leaked override from a failed assertion mid-test bleeding into a later test. */
    @AfterEach
    void clearAnyLeakedOverride() {
        ParsleyMarkerPartition.clear();
    }

    /**
     * With no override set and no delegate configured (a stage that declared no custom partitioner),
     * the wrapper returns {@link Optional#empty()} — the documented trigger for Kafka's own default
     * key-hash partitioner — so a business forward is unaffected by installing this wrapper.
     */
    @Test
    void withNoOverrideAndNoDelegateFallsBackToKafkasDefaultPartitioner() {
        ParsleyMarkerPartitioner<String, String> partitioner = new ParsleyMarkerPartitioner<>(null);

        assertEquals(Optional.empty(), partitioner.partitions("out", "k", "v", 4),
                "no override and no delegate must fall back to the default partitioner (Optional.empty())");
    }

    /**
     * With no override set and a delegate configured (a stage that called {@code withPartitioner(...)}),
     * the wrapper delegates the call through unchanged — the same topic/key/value/numPartitions the
     * caller passed, and the delegate's own answer returned verbatim — so a business forward behaves
     * exactly as if this wrapper were not installed.
     */
    @Test
    void withNoOverrideDelegatesToTheStagesOwnPartitionerUnchanged() {
        RecordingPartitioner delegate = new RecordingPartitioner(Optional.of(Set.of(2)));
        ParsleyMarkerPartitioner<String, String> partitioner = new ParsleyMarkerPartitioner<>(delegate);

        Optional<Set<Integer>> result = partitioner.partitions("out", "k", "v", 4);

        assertEquals(Optional.of(Set.of(2)), result, "the delegate's own answer must be returned verbatim");
        assertEquals("out", delegate.lastTopic, "the delegate must see the same topic");
        assertEquals("k", delegate.lastKey, "the delegate must see the same key");
        assertEquals("v", delegate.lastValue, "the delegate must see the same value");
        assertEquals(4, delegate.lastNumPartitions, "the delegate must see the same partition count");
    }

    /**
     * With an override set ({@link ParsleyProcessor#forwardToSinks} sets one immediately before every
     * marker forward), the wrapper returns exactly that partition — ignoring the key/value entirely and
     * never even consulting the delegate — regardless of what a real business partitioner would have
     * computed from the same key.
     */
    @Test
    void withAnOverrideSetReturnsItRegardlessOfKeyValueOrDelegate() {
        RecordingPartitioner delegate = new RecordingPartitioner(Optional.of(Set.of(2)));
        ParsleyMarkerPartitioner<String, String> partitioner = new ParsleyMarkerPartitioner<>(delegate);

        ParsleyMarkerPartition.set(3);
        Optional<Set<Integer>> result = partitioner.partitions("out", null, null, 4);

        assertEquals(Optional.of(Set.of(3)), result,
                "the override must win over the delegate's own answer (2) and the null key/value");
        assertFalse(delegate.invoked, "the delegate must never be consulted while an override is set");
    }

    /**
     * Once {@link ParsleyMarkerPartition#clear()} runs (the {@code finally} in {@code forwardToSinks}
     * after a marker forward completes), a subsequent call with no override set falls back to the
     * delegate again — proving the override is scoped to one forward, not sticky across calls.
     */
    @Test
    void afterClearingTheOverrideASubsequentCallFallsBackToTheDelegateAgain() {
        RecordingPartitioner delegate = new RecordingPartitioner(Optional.of(Set.of(2)));
        ParsleyMarkerPartitioner<String, String> partitioner = new ParsleyMarkerPartitioner<>(delegate);

        ParsleyMarkerPartition.set(3);
        partitioner.partitions("out", "marker-key", null, 4);
        ParsleyMarkerPartition.clear();

        Optional<Set<Integer>> result = partitioner.partitions("out", "k", "v", 4);

        assertEquals(Optional.of(Set.of(2)), result, "with the override cleared, the delegate must be consulted");
        assertTrue(delegate.invoked, "the delegate must have been consulted for this second call");
    }

    /** A {@link StreamPartitioner} test double recording the arguments of its last invocation. */
    private static final class RecordingPartitioner implements StreamPartitioner<String, String> {

        private final Optional<Set<Integer>> answer;
        private boolean invoked;
        private String lastTopic;
        private String lastKey;
        private String lastValue;
        private int lastNumPartitions;

        RecordingPartitioner(Optional<Set<Integer>> answer) {
            this.answer = answer;
        }

        @Override
        public Optional<Set<Integer>> partitions(String topic, String key, String value, int numPartitions) {
            invoked = true;
            lastTopic = topic;
            lastKey = key;
            lastValue = value;
            lastNumPartitions = numPartitions;
            return answer;
        }
    }
}
