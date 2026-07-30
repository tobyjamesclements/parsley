package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worked example — <b>time-driven policy as delivered records</b> ({@code docs/guide/ticks.md}).
 * Wall-clock time reaches pure logic only as data on a tick record: the runtime emits one
 * stamped tick per interval to the stage's own tick topic, consumes it back through the causal
 * gate, and folds it. The policy stays a pure function, and the tick is durable — a tick whose
 * processing fails is redelivered by the aborted transaction's retry, never lost.
 *
 * <p>The scope of a tick is its channel: its stamp claims the task's consumed history at the
 * moment of emission, a consistent cut. That is why a channel-scoped policy ("everything seen
 * so far") needs no enumeration, and why a per-entity timer is a topology shape instead
 * ({@code docs/guide/topologies.md#request-and-reply}).
 */
class ExampleTickPolicyTest {

    private static final Topic<String, Long> ORDERS = Topic.of("orders", Codec.utf8(), Codec.int64());
    private static final Topic<String, String> AUDIT =
            Topic.of("audit", Codec.utf8(), Codec.utf8());

    private static final Duration INTERVAL = Duration.ofSeconds(1);

    /**
     * A stateful stage whose per-key state counts each entity's orders, and whose tick logic
     * folds the <em>tick state</em> — one reserved per-partition slot, distinct from every
     * per-key slot — to emit a cut marker every second interval. Tick logic never sees per-key
     * state; it holds tick-to-tick memory instead.
     */
    private static Stage policy() {
        return Stage.named("policy")
                .state(Codec.int64(), () -> 0L)
                .on(ORDERS, (Long seen, Message<String, Long> m) ->
                        Step.of(seen + 1, AUDIT.send(m.key(), "order:" + (seen + 1))))
                .ticks(INTERVAL, (Long quiet, Tick tick) -> quiet + 1 >= 2
                        ? Step.of(0L, AUDIT.send("cut", "cut@" + tick.timestamp()))
                        : Step.of(quiet + 1))
                .into(AUDIT)
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

    private static TestInputTopic<byte[], byte[]> in(TopologyTestDriver d, String topic) {
        return d.createInputTopic(topic, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static TestOutputTopic<byte[], byte[]> out(TopologyTestDriver d, String topic) {
        return d.createOutputTopic(topic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * The policy fires on the tick, not on traffic: two intervals produce one cut marker, and
     * the tick state carries the count between them. Per-key state is untouched by ticks — the
     * tick slot is its own — so the entity's counter keeps counting across the cut.
     */
    @Test
    void aTickDrivenPolicyFiresOnItsOwnCadence() {
        // A fixed initial wall clock makes tick timestamps deterministic.
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(policy()).testTopology(), props("tick-policy"),
                Instant.ofEpochMilli(10_000L))) {
            TestInputTopic<byte[], byte[]> orders = in(app, "orders");
            TestOutputTopic<byte[], byte[]> audit = out(app, "audit");

            orders.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(7L));
            orders.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(9L));
            assertEquals(List.of("order:1", "order:2"),
                    audit.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "per-key state must count this entity's orders");

            app.advanceWallClockTime(INTERVAL);
            assertTrue(audit.isEmpty(), "the first interval only arms the policy");

            app.advanceWallClockTime(INTERVAL);
            assertEquals(List.of("cut@12000"),
                    audit.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the second interval must fire the cut, timestamped by that tick"
                            + " (10000 initial + two 1s intervals)");

            orders.pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(11L));
            assertEquals(List.of("order:3"),
                    audit.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the tick slot is distinct from per-key state: the entity count survives");
        }
    }

    /**
     * A tick is an ordinary record on an ordinary topic: stamped at the same site as every
     * emission, so it claims the task's consumed history — the consistent cut that makes
     * "everything in this channel so far" expressible without enumerating anything.
     */
    @Test
    void aTickIsAStampedRecordOnTheStagesOwnTopic() {
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(policy()).testTopology(), props("tick-stamp"),
                Instant.ofEpochMilli(10_000L))) {
            in(app, "orders").pipeInput(Codec.utf8().encode("k"), Codec.int64().encode(7L));
            app.advanceWallClockTime(INTERVAL);

            List<TestRecord<byte[], byte[]>> ticks =
                    out(app, "vc-policy-ticks").readRecordsToList();
            assertEquals(1, ticks.size(), "one interval must emit exactly one tick per task");

            VectorClock stamp = CausalHeaders.read(ticks.get(0).headers());
            assertNotNull(stamp, "a tick carries a stamp like any other record");
            assertEquals(0L, stamp.get(Stage.testChannel("orders", 0)),
                    "the tick's stamp must claim the consumed history at the moment it fired");
            assertNotNull(CausalHeaders.readSender(ticks.get(0).headers()),
                    "a tick is tagged by its emitting task like any other send");
        }
    }
}
