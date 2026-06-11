package io.parsley.it;

import io.parsley.Partition;
import io.parsley.streams.internal.PartitionAdapter;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionAdapterTest {

    @Test
    void toKafkaPreservesTopicAndPartition() {
        Partition p = new Partition("orders", 3);
        TopicPartition tp = PartitionAdapter.toKafka(p);
        assertEquals("orders", tp.topic());
        assertEquals(3, tp.partition());
    }

    @Test
    void fromKafkaPreservesTopicAndPartition() {
        TopicPartition tp = new TopicPartition("payments", 7);
        Partition p = PartitionAdapter.fromKafka(tp);
        assertEquals("payments", p.topic());
        assertEquals(7, p.partition());
    }

    @Test
    void roundTripFromParsley() {
        Partition original = new Partition("inventory", 2);
        assertEquals(original, PartitionAdapter.fromKafka(PartitionAdapter.toKafka(original)));
    }

    @Test
    void roundTripFromKafka() {
        TopicPartition original = new TopicPartition("shipments", 5);
        assertEquals(original, PartitionAdapter.toKafka(PartitionAdapter.fromKafka(original)));
    }
}
