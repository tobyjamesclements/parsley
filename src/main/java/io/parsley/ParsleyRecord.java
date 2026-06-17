package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The internal envelope the engine processes: a record whose source coordinate and dependency clock
 * are carried as internal headers ({@link ParsleyAttributes#SRC_TOPIC},
 * {@link ParsleyAttributes#SRC_TOPIC_ID}, {@link ParsleyAttributes#SRC_PARTITION},
 * {@link ParsleyAttributes#SRC_OFFSET}, {@link ParsleyAttributes#CAUSAL_DEPENDENCIES}) alongside any user
 * headers.
 *
 * @param <K>       the record key type
 * @param <V>       the record value type
 * @param key       the record key; may be {@code null}
 * @param value     the record value; may be {@code null}
 * @param timestamp the record's broker timestamp (epoch milliseconds)
 * @param headers   all headers — user headers plus internal {@code _parsley_*} and clock headers
 */
record ParsleyRecord<K, V>(K key, V value, long timestamp, List<ParsleyHeader> headers) {

    /**
     * Canonical constructor; defensively copies {@code headers}.
     */
    ParsleyRecord {
        headers = List.copyOf(headers);
    }

    /**
     * Builds an envelope from an inbound Kafka Streams {@link Record} and its source coordinate,
     * using the provided Kafka topic UUID as the stable clock key. Writes
     * {@link ParsleyAttributes#SRC_TOPIC_ID} so downstream steps (engine, serializer) can recover
     * the UUID without an AdminClient call.
     *
     * @param record          the inbound Streams record
     * @param sourcePartition the topic-partition the record was read from
     * @param sourceOffset    the offset the record was read from
     * @param topicId         the Kafka UUID for the source topic
     * @param <K>             the record key type
     * @param <V>             the record value type
     * @return a new {@code ParsleyRecord}
     */
    static <K, V> ParsleyRecord<K, V> of(Record<K, V> record, TopicPartition sourcePartition,
                                          long sourceOffset, Uuid topicId) {
        List<ParsleyHeader> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new ParsleyHeader(header.key(), header.value()));
        }
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC,
                sourcePartition.topic().getBytes(StandardCharsets.UTF_8)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_TOPIC_ID, uuidToBytes(topicId)));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_PARTITION, intToBytes(sourcePartition.partition())));
        headers.add(new ParsleyHeader(ParsleyAttributes.SRC_OFFSET, longToBytes(sourceOffset)));
        return new ParsleyRecord<>(record.key(), record.value(), record.timestamp(), headers);
    }

    /**
     * Convenience overload that derives the topic UUID via {@link CausalPosition#deriveUuid}. Used
     * in paths where no AdminClient UUID is available (tests, {@link org.apache.kafka.streams.TopologyTestDriver}).
     */
    static <K, V> ParsleyRecord<K, V> of(Record<K, V> record, TopicPartition sourcePartition, long sourceOffset) {
        return of(record, sourcePartition, sourceOffset, CausalPosition.deriveUuid(sourcePartition.topic()));
    }

    /** The source topic, read from the {@link ParsleyAttributes#SRC_TOPIC} header. */
    String sourceTopic() {
        ParsleyHeader h = findHeader(ParsleyAttributes.SRC_TOPIC);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    /**
     * The Kafka UUID of the source topic, read from the {@link ParsleyAttributes#SRC_TOPIC_ID}
     * header. Falls back to {@link CausalPosition#deriveUuid} if the header is absent (e.g. records
     * built in tests without an explicit UUID).
     */
    Uuid sourceTopicId() {
        ParsleyHeader h = findHeader(ParsleyAttributes.SRC_TOPIC_ID);
        if (h == null) return CausalPosition.deriveUuid(sourceTopic());
        byte[] b = h.value();
        long msb = ByteBuffer.wrap(b, 0, 8).getLong();
        long lsb = ByteBuffer.wrap(b, 8, 8).getLong();
        return new Uuid(msb, lsb);
    }

    /** The source partition index, read from the {@link ParsleyAttributes#SRC_PARTITION} header. */
    int sourcePartitionIndex() {
        ParsleyHeader h = findHeader(ParsleyAttributes.SRC_PARTITION);
        return h == null ? 0 : ByteBuffer.wrap(h.value()).getInt();
    }

    /** The source offset, read from the {@link ParsleyAttributes#SRC_OFFSET} header. */
    long sourceOffset() {
        ParsleyHeader h = findHeader(ParsleyAttributes.SRC_OFFSET);
        return h == null ? 0L : ByteBuffer.wrap(h.value()).getLong();
    }

    /** Convenience: reconstructs the source {@link TopicPartition} from {@code SRC_TOPIC} and {@code SRC_PARTITION}. */
    TopicPartition sourcePartition() {
        return new TopicPartition(sourceTopic(), sourcePartitionIndex());
    }

    /**
     * Returns the raw bytes of the {@link ParsleyAttributes#CAUSAL_DEPENDENCIES} header, or {@code null}
     * if the record carried none.
     */
    byte[] encodedDependencies() {
        ParsleyHeader h = findHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
        return h == null ? null : h.value();
    }

    /**
     * Reconstructs the original Kafka {@link ConsumerRecord} this envelope was built from.
     *
     * @return a {@code ConsumerRecord} at this record's source coordinate
     */
    ConsumerRecord<K, V> toConsumerRecord() {
        return new ConsumerRecord<>(
                sourceTopic(), sourcePartitionIndex(), sourceOffset(),
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
        for (ParsleyHeader header : headers) {
            kafkaHeaders.add(new RecordHeader(header.key(), header.value()));
        }
        return kafkaHeaders;
    }

    private ParsleyHeader findHeader(String key) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            if (key.equals(headers.get(i).key())) return headers.get(i);
        }
        return null;
    }

    static byte[] intToBytes(int v)    { return ByteBuffer.allocate(4).putInt(v).array(); }
    static byte[] longToBytes(long v)  { return ByteBuffer.allocate(8).putLong(v).array(); }
    static byte[] uuidToBytes(Uuid id) {
        return ByteBuffer.allocate(16)
                .putLong(id.getMostSignificantBits())
                .putLong(id.getLeastSignificantBits())
                .array();
    }
}
