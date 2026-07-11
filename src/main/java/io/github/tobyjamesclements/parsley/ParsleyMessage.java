package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's typed envelope: a record together with the causal metadata Parsley needs, all as
 * typed fields rather than re-parsed headers. {@code headers} holds the user's headers only — the
 * source coordinate ({@code topic}/{@code topicId}/{@code partition}/{@code offset}) and the causal
 * {@code dependencies} are first-class fields. They are written as typed framing fields (never
 * headers) when a message is persisted to the buffer store ({@link ParsleySerializer}), and the
 * dependencies re-materialise as the {@code parsley-causal-dependencies} header only for the
 * delegate's view ({@link #headersWithDependencies}).
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
record ParsleyMessage<K, V>(String topic, Uuid topicId, int partition, long offset, long timestamp,
                            @Nullable K key, @Nullable V value, List<ParsleyHeader> headers,
                            ParsleyClock dependencies) {

    ParsleyMessage {
        headers = List.copyOf(headers);
    }

    /**
     * Builds a message from an inbound Kafka Streams {@link Record} at its source coordinate. The
     * record's {@code parsley-causal-dependencies} header is decoded into {@link #dependencies}
     * (absent → empty, vacuously satisfied), and all other non-internal headers are carried as user
     * headers.
     *
     * @throws ParsleyClockResolutionException if the dependencies header is present but cannot be
     *     decoded; the caller fails the task fast rather than forward the record on an unknown premise
     */
    static <K, V> ParsleyMessage<K, V> from(Record<K, V> record, TopicPartition source,
                                            long offset, Uuid topicId) {
        ParsleyClock dependencies = decodeDependencies(encodedDependencies(record), source, topicId, offset);
        return from(record, source, offset, topicId, dependencies);
    }

    /**
     * Builds a message with caller-supplied {@code dependencies}, skipping header decoding.
     */
    static <K, V> ParsleyMessage<K, V> from(Record<K, V> record, TopicPartition source,
                                            long offset, Uuid topicId, ParsleyClock dependencies) {
        return new ParsleyMessage<>(source.topic(), topicId, source.partition(), offset,
                record.timestamp(), record.key(), record.value(), userHeaders(record), dependencies);
    }

    private static List<ParsleyHeader> userHeaders(Record<?, ?> record) {
        List<ParsleyHeader> userHeaders = new ArrayList<>();
        for (Header header : record.headers()) {
            if (!ParsleyHeader.CAUSAL_DEPENDENCIES.equals(header.key())
                    && !header.key().startsWith(ParsleyHeader.INTERNAL_PREFIX)) {
                userHeaders.add(new ParsleyHeader(header.key(), header.value()));
            }
        }
        return userHeaders;
    }

    private static byte @Nullable [] encodedDependencies(Record<?, ?> record) {
        Header header = record.headers().lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
        return header == null ? null : header.value();
    }

    /**
     * The user headers plus the {@code parsley-causal-dependencies} header carrying
     * {@link #dependencies} — the header set a delegate processor sees. Carries no
     * {@code _parsley_*}-prefixed internal marker headers.
     */
    Headers headersWithDependencies() {
        Headers out = userHeadersView();
        out.add(ParsleyHeader.CAUSAL_DEPENDENCIES, dependencies.toBytes());
        return out;
    }

    private Headers userHeadersView() {
        Headers out = ParsleyHeader.mutableHeaders();
        for (ParsleyHeader header : headers) {
            out.add(header.key(), header.value());
        }
        return out;
    }

    private static ParsleyClock decodeDependencies(byte @Nullable [] encoded, TopicPartition source,
                                                   Uuid topicId, long offset) {
        if (encoded == null) {
            return ParsleyClock.empty();
        }
        try {
            return ParsleyClock.fromBytes(encoded);
        } catch (Exception e) {
            throw new ParsleyClockResolutionException(source.topic(), topicId, source.partition(), offset,
                    encoded, "encoded causal-dependencies header length " + encoded.length, e);
        }
    }
}
