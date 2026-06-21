package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's typed envelope: a record together with the causal metadata Parsley needs, all as
 * typed fields rather than re-parsed headers. {@code headers} holds the user's headers only — the
 * source coordinate ({@code topic}/{@code topicId}/{@code partition}/{@code offset}) and the causal
 * {@code dependencies} are first-class fields, encoded back into the {@code _parsley_*} /
 * {@code parsley-causal-dependencies} wire headers only when a message crosses a Kafka boundary
 * (the buffer store, or the consumer's outbox topic).
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
record ParsleyMessage<K, V>(String topic, Uuid topicId, int partition, long offset, long timestamp,
                            K key, V value, List<ParsleyHeader> headers, ParsleyClock dependencies) {

    private static final Logger log = LoggerFactory.getLogger(ParsleyMessage.class);

    ParsleyMessage {
        headers = List.copyOf(headers);
    }

    /**
     * Builds a message from an inbound Kafka Streams {@link Record} at its source coordinate. The
     * record's {@code parsley-causal-dependencies} header is decoded into {@link #dependencies}
     * (absent or undecodable → empty, vacuously satisfied), and all other non-internal headers are
     * carried as user headers.
     */
    static <K, V> ParsleyMessage<K, V> from(Record<K, V> record, TopicPartition source,
                                            long offset, Uuid topicId) {
        List<ParsleyHeader> userHeaders = new ArrayList<>();
        byte[] encodedDependencies = null;
        for (Header header : record.headers()) {
            if (ParsleyHeader.CAUSAL_DEPENDENCIES.equals(header.key())) {
                encodedDependencies = header.value();
            } else if (!header.key().startsWith(ParsleyHeader.INTERNAL_PREFIX)) {
                userHeaders.add(new ParsleyHeader(header.key(), header.value()));
            }
        }
        ParsleyClock dependencies = decodeDependencies(encodedDependencies, source, offset);
        return new ParsleyMessage<>(source.topic(), topicId, source.partition(), offset,
                record.timestamp(), record.key(), record.value(), userHeaders, dependencies);
    }

    /**
     * Decodes a message routed through the consumer's outbox topic: the source coordinate is read
     * from the {@code _parsley_src_*} headers, the dependencies from
     * {@code parsley-causal-dependencies}, and every other non-internal header is a user header.
     */
    static ParsleyMessage<byte[], byte[]> fromOutboxRecord(ConsumerRecord<byte[], byte[]> record) {
        Headers headers = record.headers();
        String topic = new String(headers.lastHeader(ParsleyHeader.SRC_TOPIC).value(), java.nio.charset.StandardCharsets.UTF_8);
        Uuid topicId = ParsleyHeader.uuidFromBytes(headers.lastHeader(ParsleyHeader.SRC_TOPIC_ID).value());
        int partition = ParsleyHeader.intFromBytes(headers.lastHeader(ParsleyHeader.SRC_PARTITION).value());
        long offset = ParsleyHeader.longFromBytes(headers.lastHeader(ParsleyHeader.SRC_OFFSET).value());

        List<ParsleyHeader> userHeaders = new ArrayList<>();
        byte[] encodedDependencies = null;
        for (Header header : headers) {
            if (ParsleyHeader.CAUSAL_DEPENDENCIES.equals(header.key())) {
                encodedDependencies = header.value();
            } else if (!header.key().startsWith(ParsleyHeader.INTERNAL_PREFIX)) {
                userHeaders.add(new ParsleyHeader(header.key(), header.value()));
            }
        }
        ParsleyClock dependencies = decodeDependencies(encodedDependencies,
                new TopicPartition(topic, partition), offset);
        return new ParsleyMessage<>(topic, topicId, partition, offset, record.timestamp(),
                record.key(), record.value(), userHeaders, dependencies);
    }

    /**
     * The user headers plus the {@code parsley-causal-dependencies} header carrying
     * {@link #dependencies} — the header set a delegate processor or a {@code poll()} caller sees.
     * Carries no internal {@code _parsley_src_*} routing headers.
     */
    Headers headersWithDependencies() {
        Headers out = userHeadersView();
        out.add(ParsleyHeader.CAUSAL_DEPENDENCIES, dependencies.toBytes());
        return out;
    }

    /**
     * The full wire header set for the consumer's outbox topic: user headers, the
     * {@code parsley-causal-dependencies} clock, and the {@code _parsley_src_*} source coordinate so
     * the original coordinate can be reconstructed on the far side.
     */
    Headers toForwardHeaders() {
        Headers out = headersWithDependencies();
        out.add(ParsleyHeader.SRC_TOPIC, ParsleyHeader.srcTopic(topic).value());
        out.add(ParsleyHeader.SRC_TOPIC_ID, ParsleyHeader.uuidToBytes(topicId));
        out.add(ParsleyHeader.SRC_PARTITION, ParsleyHeader.intToBytes(partition));
        out.add(ParsleyHeader.SRC_OFFSET, ParsleyHeader.longToBytes(offset));
        return out;
    }

    private Headers userHeadersView() {
        Headers out = ParsleyHeader.mutableHeaders();
        for (ParsleyHeader header : headers) {
            out.add(header.key(), header.value());
        }
        return out;
    }

    private static ParsleyClock decodeDependencies(byte[] encoded, TopicPartition source, long offset) {
        if (encoded == null) {
            return ParsleyClock.empty();
        }
        try {
            return ParsleyClock.fromBytes(encoded);
        } catch (Exception e) {
            log.warn("Unresolvable causal-dependencies header on {}-{} @{} — treating as trivially satisfied",
                    source.topic(), source.partition(), offset);
            return ParsleyClock.empty();
        }
    }
}
