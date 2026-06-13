package io.parsley.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.Optional;

/**
 * The internal envelope the engine processes: a record together with its source coordinate and
 * the raw bytes of its dependency clock.
 *
 * @param <K>                 the record key type
 * @param <V>                 the record value type
 * @param key                 the record key; may be {@code null}
 * @param value               the record value; may be {@code null}
 * @param timestamp           the record's broker timestamp (epoch milliseconds)
 * @param headers             the record's headers, in original order
 * @param encodedDependencies the raw bytes of the {@code parsley-vector-clock} header, or
 *                            {@code null} if the record carried none
 * @param sourcePartition     the topic-partition the record was read from
 * @param sourceOffset        the offset the record was read from
 */
record CausalRecord<K, V>(
        K key,
        V value,
        long timestamp,
        List<CausalHeader> headers,
        byte[] encodedDependencies,
        TopicPartition sourcePartition,
        long sourceOffset) {

    /**
     * Canonical constructor; defensively copies {@code headers}.
     */
    CausalRecord {
        headers = List.copyOf(headers);
    }

    /**
     * Returns the raw bytes of the {@code parsley-vector-clock} header, if present.
     *
     * @return the encoded dependency clock, or empty if the record carried none
     */
    Optional<byte[]> dependencies() {
        return Optional.ofNullable(encodedDependencies);
    }

    /**
     * Reconstructs the original Kafka {@link ConsumerRecord} this envelope was built from.
     *
     * @return a {@code ConsumerRecord} at this record's source coordinate
     */
    ConsumerRecord<K, V> toConsumerRecord() {
        return new ConsumerRecord<>(
                sourcePartition.topic(), sourcePartition.partition(), sourceOffset,
                timestamp, TimestampType.CREATE_TIME,
                -1, -1,
                key, value,
                toHeaders(), Optional.empty());
    }

    /**
     * Returns this record's headers as a mutable Kafka {@link Headers} instance.
     *
     * @return the headers
     */
    Headers toHeaders() {
        Headers kafkaHeaders = new RecordHeaders();
        for (CausalHeader header : headers) {
            kafkaHeaders.add(new RecordHeader(header.key(), header.value()));
        }
        return kafkaHeaders;
    }
}
