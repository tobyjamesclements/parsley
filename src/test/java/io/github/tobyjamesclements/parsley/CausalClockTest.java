package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * The plain-client clock: the three verbs a non-Streams producer uses to carry causal custody.
 * Topic identity is resolved through a synthesized resolver; no broker is involved.
 */
class CausalClockTest {

    private static final TopicIds IDS =
            topic -> new TopicIds.Resolved(UUID.nameUUIDFromBytes(topic.getBytes()), 1);

    private static Channel channel(String topic, int partition) {
        return new Channel(UUID.nameUUIDFromBytes(topic.getBytes()), partition);
    }

    private static VectorClock stampOf(CausalClock clock) {
        var headers = new RecordHeaders();
        clock.stamp(headers);
        VectorClock stamp = CausalHeaders.read(headers);
        assertNotNull(stamp, "stamp must write a clock header");
        return stamp;
    }

    /** Observing an unstamped record claims exactly the record's own coordinate. */
    @Test
    void observeClaimsTheRecordsCoordinate() {
        CausalClock clock = new CausalClock(IDS);
        clock.observe(new ConsumerRecord<>("t1", 0, 5L, "k", "v"));

        assertEquals(5L, stampOf(clock).get(channel("t1", 0)),
                "the consumed coordinate must be claimed");
    }

    /** Observing a stamped record folds in its carried clock as well as its coordinate. */
    @Test
    void observeFoldsInTheCarriedClock() {
        var headers = new RecordHeaders();
        CausalHeaders.write(headers, VectorClock.of(channel("upstream", 3), 7));
        var record = new ConsumerRecord<>("t1", 0, 5L, 1000L, TimestampType.CREATE_TIME,
                -1, -1, "k", "v", headers, Optional.empty());

        CausalClock clock = new CausalClock(IDS);
        clock.observe(record);

        VectorClock stamp = stampOf(clock);
        assertEquals(7L, stamp.get(channel("upstream", 3)),
                "the record's carried claims must be adopted");
        assertEquals(5L, stamp.get(channel("t1", 0)),
                "the record's own coordinate must be claimed alongside");
    }

    /** An acknowledged send is claimed; an acknowledgement without an offset claims nothing. */
    @Test
    void recordProducedClaimsOnlyAcknowledgedOffsets() {
        CausalClock clock = new CausalClock(IDS);
        clock.recordProduced(new RecordMetadata(new TopicPartition("out", 0), 9L, 0, 1000L, -1, -1));
        assertEquals(9L, stampOf(clock).get(channel("out", 0)),
                "an acknowledged offset must be claimed");

        clock.recordProduced(new RecordMetadata(new TopicPartition("other", 0), -1L, 0, 1000L, -1, -1));
        assertEquals(VectorClock.NOTHING, stampOf(clock).get(channel("other", 0)),
                "an acknowledgement without an offset must claim nothing");
    }
}
