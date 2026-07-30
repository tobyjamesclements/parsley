package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Worked example — <b>concurrent inputs and what causal order does not promise</b>
 * ({@code docs/guide/expectations.md}, "No total order"). Records with no causal relation are
 * delivered in arrival order, and the interleaving may differ between instances and between
 * runs: the delivered history is one valid extension of the causal partial order, not a total
 * order anyone chose.
 *
 * <p>The practical rule that follows is the one this example makes concrete: logic whose
 * outcome must not depend on the choice is written commutative for concurrent inputs. A
 * commutative fold reaches the same state either way; a non-commutative one does not, and no
 * protocol can rescue it without inventing an order that does not exist.
 */
class ExampleConcurrentInputsTest {

    private static final Topic<String, Long> LEFT = Topic.of("left", Codec.utf8(), Codec.int64());
    private static final Topic<String, Long> RIGHT = Topic.of("right", Codec.utf8(), Codec.int64());
    private static final Topic<String, Long> TOTALS = Topic.of("totals", Codec.utf8(), Codec.int64());

    /** Builds a two-source stage folding both sides with the given step function. */
    private static Stage merging(BiFunction<Long, Long, Long> step) {
        return Stage.named("merge")
                .state(Codec.int64(), () -> 0L)
                .on(LEFT, (Long acc, Message<String, Long> m) -> {
                    long next = step.apply(acc, m.value());
                    return Step.of(next, TOTALS.send(m.key(), next));
                })
                .on(RIGHT, (Long acc, Message<String, Long> m) -> {
                    long next = step.apply(acc, m.value());
                    return Step.of(next, TOTALS.send(m.key(), next));
                })
                .into(TOTALS)
                .build();
    }

    @TempDir
    Path stateDir;

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(appId).toString());
        return props;
    }

    /**
     * Feeds two causally unrelated records — one per source topic, neither claiming the other
     * — in the given order, and returns the final folded total.
     */
    private long finalTotal(BiFunction<Long, Long, Long> step, String appId, boolean leftFirst) {
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(merging(step)).testTopology(), props(appId))) {
            TestInputTopic<byte[], byte[]> left = app.createInputTopic("left",
                    new ByteArraySerializer(), new ByteArraySerializer());
            TestInputTopic<byte[], byte[]> right = app.createInputTopic("right",
                    new ByteArraySerializer(), new ByteArraySerializer());

            if (leftFirst) {
                left.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(10L));
                right.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(3L));
            } else {
                right.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(3L));
                left.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(10L));
            }

            List<Long> totals = app.createOutputTopic("totals",
                            new ByteArrayDeserializer(), new ByteArrayDeserializer())
                    .readValuesToList().stream().map(Codec.int64()::decode).toList();
            return totals.get(totals.size() - 1);
        }
    }

    /**
     * Commutative logic is order-independent, so the arrival order of concurrent records
     * cannot change the outcome. This is the property to aim for whenever two sources can
     * produce unrelated records about one key.
     */
    @Test
    void commutativeLogicReachesTheSameStateWhicheverConcurrentRecordArrivesFirst() {
        BiFunction<Long, Long, Long> sum = Long::sum;
        assertEquals(finalTotal(sum, "concurrent-sum-a", true),
                finalTotal(sum, "concurrent-sum-b", false),
                "a commutative fold must not depend on the interleaving of concurrent inputs");
    }

    /**
     * The honest converse: non-commutative logic over concurrent inputs produces different
     * outcomes for different interleavings, and both are correct causal histories. Causal
     * order fixes cause-before-effect; it does not invent an order between records that have
     * no relation, and treating arrival order as meaningful is the bug this example names.
     */
    @Test
    void nonCommutativeLogicDependsOnAnOrderCausalityDoesNotProvide() {
        BiFunction<Long, Long, Long> subtractFromLatest = (acc, v) -> v - acc;
        assertNotEquals(finalTotal(subtractFromLatest, "concurrent-nc-a", true),
                finalTotal(subtractFromLatest, "concurrent-nc-b", false),
                "this fold's outcome depends on arrival order, which concurrency leaves open");
    }
}
