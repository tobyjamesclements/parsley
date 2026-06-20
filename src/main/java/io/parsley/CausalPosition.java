package io.parsley;

import org.apache.kafka.common.Uuid;

/**
 * A position in the causal log: a topic-partition (a topic, identified by its Kafka UUID, and a
 * partition) plus an offset. The same type serves as a frontier entry ("I have observed this far")
 * and as a dependency entry ("I require at least this position").
 *
 * <p>Topic UUIDs are the stable identity key across topic deletion and recreation. They are
 * typically resolved from the broker via {@code AdminClient} (e.g. {@code
 * CreateTopicsResult.topicId(topic)} or {@code DescribeTopicsResult}). Tests without a live broker
 * may use any stable {@link Uuid}, e.g. {@link Uuid#randomUuid()}.
 *
 * @param topicId   the Kafka topic UUID
 * @param partition the partition index
 * @param offset    the offset (inclusive)
 */
public record CausalPosition(Uuid topicId, int partition, long offset) { }
