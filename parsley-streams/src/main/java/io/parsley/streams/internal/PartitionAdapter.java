package io.parsley.streams.internal;

import io.parsley.Partition;

public final class PartitionAdapter {

    private PartitionAdapter() {}

    public static org.apache.kafka.common.TopicPartition toKafka(Partition p) {
        return new org.apache.kafka.common.TopicPartition(p.topic(), p.partition());
    }

    public static Partition fromKafka(org.apache.kafka.common.TopicPartition tp) {
        return new Partition(tp.topic(), tp.partition());
    }
}
