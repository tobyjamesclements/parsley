package io.parsley;

import org.apache.kafka.common.Uuid;

/**
 * A topic-partition: a topic (identified by its Kafka UUID) and a partition index. This is the key a
 * causal clock maps to an offset — a {@link CausalPosition} is a {@code ParsleyPartition} plus that
 * offset. Topic UUIDs are the stable identity across topic deletion and recreation.
 *
 * @param topicId   the Kafka topic UUID
 * @param partition the partition index
 */
record ParsleyPartition(Uuid topicId, int partition) {}
