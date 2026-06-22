package io.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.jspecify.annotations.Nullable;
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
 * (the buffer store).
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
record ParsleyMessage<K, V>(String topic, Uuid topicId, int partition, long offset, long timestamp,
                            @Nullable K key, @Nullable V value, List<ParsleyHeader> headers,
                            ParsleyClock dependencies) {

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
     * The user headers plus the {@code parsley-causal-dependencies} header carrying
     * {@link #dependencies} — the header set a delegate processor sees. Carries no internal
     * {@code _parsley_src_*} routing headers.
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

    private static ParsleyClock decodeDependencies(byte @Nullable [] encoded, TopicPartition source, long offset) {
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
