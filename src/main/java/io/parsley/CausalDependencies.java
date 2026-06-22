package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Objects;
import java.util.Optional;

/**
 * A producer-stamped set of causal requirements: the positions a consumer must have observed before
 * a record stamped with these dependencies may be delivered.
 *
 * <p>The usual flow at a topology edge is to derive the dependencies of the record you are about to
 * produce from the record(s) you consumed with {@link #from(CausalTopics, ConsumerRecord)} — which
 * carries the upstream's dependencies <em>and</em> the consumed record's own position — combine
 * several with {@link #merge(CausalDependencies)} for a fan-in, then attach them to the outbound
 * record with {@link #stamp(ProducerRecord)}. To assert a dependency you did not consume, build one
 * with {@link #builder(CausalTopics)}. Serialise with {@link #toBytes()} / {@link #fromBytes(byte[])}.
 *
 * <p>This is the public face of an internal {@link ParsleyClock}; the two share a wire format.
 *
 * <h2>Serialised size and the {@code message.max.bytes} ceiling</h2>
 * The {@link #toBytes() serialised} form is {@code 5 + 28 × coordinates} bytes. An instance spanning
 * many partitions can breach Kafka's record-size limit ({@code message.max.bytes}, ~1&nbsp;MB by
 * default); the figure to watch is a wide-fan-in record that depends on many topic-partitions.
 */
public final class CausalDependencies {

    private final ParsleyClock clock;

    private CausalDependencies(ParsleyClock clock) {
        this.clock = clock;
    }

    /**
     * Returns an empty instance with no positions recorded.
     *
     * @return an empty {@code CausalDependencies}
     */
    public static CausalDependencies empty() {
        return new CausalDependencies(ParsleyClock.empty());
    }

    /**
     * Returns a new builder that resolves topic names to their stable UUIDs through {@code topics}.
     *
     * @param topics the resolver mapping topic names to their Kafka UUIDs; must not be {@code null}
     * @return a new {@code Builder}
     */
    public static Builder builder(CausalTopics topics) {
        return new Builder(topics);
    }

    /**
     * Builder for {@link CausalDependencies}.
     */
    public static final class Builder {
        private final CausalTopics topics;
        private ParsleyClock clock = ParsleyClock.empty();

        private Builder(CausalTopics topics) {
            this.topics = Objects.requireNonNull(topics, "topics must not be null");
        }

        /**
         * Requires that {@code (topic, partition)} has been observed at offset {@code offset} or
         * later. The topic name is resolved to its stable UUID through the builder's
         * {@link CausalTopics}. If a requirement already exists for that coordinate, the higher offset
         * wins.
         *
         * @param topic     the topic name to require; must not be {@code null}
         * @param partition the partition index
         * @param offset    the offset (inclusive) the consumer must have observed
         * @return this builder
         * @throws IllegalArgumentException if {@code topic} cannot be resolved to a UUID
         */
        public Builder require(String topic, int partition, long offset) {
            clock = clock.observe(topics.topicId(topic), partition, offset);
            return this;
        }

        /**
         * Returns a new {@code CausalDependencies} from the positions accumulated so far.
         *
         * @return a new {@code CausalDependencies}
         */
        public CausalDependencies build() {
            return new CausalDependencies(clock);
        }
    }

    /**
     * Returns the causal union of these dependencies and {@code other} — the per-coordinate maximum
     * offset. Use this to combine the dependencies of several consumed records before stamping a
     * fan-in record.
     *
     * @param other the dependencies to merge in; must not be {@code null}
     * @return a new {@code CausalDependencies} dominating both inputs
     */
    public CausalDependencies merge(CausalDependencies other) {
        Objects.requireNonNull(other, "other must not be null");
        return new CausalDependencies(clock.merge(other.clock));
    }

    /**
     * Returns the dependencies a record produced after consuming {@code record} should carry: the
     * dependencies {@code record} itself carried, unioned with {@code record}'s own position
     * {@code (topic, partition, offset)} so that downstream consumers wait until they have observed
     * {@code record} before delivering anything stamped with the result.
     *
     * @param topics the resolver mapping {@code record}'s topic name to its Kafka UUID; must not be
     *               {@code null}
     * @param record the consumed record; must not be {@code null}
     * @return the consumed record's transitive dependencies plus its own position
     * @throws IllegalArgumentException if {@code record}'s topic cannot be resolved to a UUID
     * @throws IllegalStateException    if {@code record} carries a malformed dependencies header
     */
    public static CausalDependencies from(CausalTopics topics, ConsumerRecord<?, ?> record) {
        Objects.requireNonNull(topics, "topics must not be null");
        Objects.requireNonNull(record, "record must not be null");
        ParsleyClock carried = fromHeaders(record.headers()).map(deps -> deps.clock).orElse(ParsleyClock.empty());
        ParsleyClock withOwnPosition =
                carried.observe(topics.topicId(record.topic()), record.partition(), record.offset());
        return new CausalDependencies(withOwnPosition);
    }

    /**
     * Returns a copy of {@code record} carrying these dependencies in its
     * {@code parsley-causal-dependencies} header, ready to send through a plain Kafka producer. The
     * input record is never mutated: a fresh header set is built (preserving every other header), and
     * stamping is idempotent — any existing dependencies header is replaced, not duplicated.
     *
     * @param record the record to stamp; must not be {@code null}
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @return a new {@code ProducerRecord} with the dependencies header attached
     */
    public <K, V> ProducerRecord<K, V> stamp(ProducerRecord<K, V> record) {
        Objects.requireNonNull(record, "record must not be null");
        Headers stamped = ParsleyHeader.mutableHeaders();
        for (Header header : record.headers()) {
            if (!header.key().equals(ParsleyHeader.CAUSAL_DEPENDENCIES)) {
                stamped.add(header.key(), header.value());
            }
        }
        stamped.add(ParsleyHeader.CAUSAL_DEPENDENCIES, clock.toBytes());
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), record.value(), stamped);
    }

    /**
     * Serialises to a compact binary form.
     *
     * @return the serialised bytes
     */
    public byte[] toBytes() {
        return clock.toBytes();
    }

    /**
     * Reconstructs a {@code CausalDependencies} from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised bytes; must not be {@code null}
     * @return the deserialised {@code CausalDependencies}
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    public static CausalDependencies fromBytes(byte[] bytes) {
        return new CausalDependencies(ParsleyClock.fromBytes(bytes));
    }

    /**
     * Extracts the causal dependencies a Parsley-stamped message carries in its
     * {@code parsley-causal-dependencies} header.
     *
     * @param record the consumed record; must not be {@code null}
     * @return the embedded dependencies, or empty if the record carries no dependencies header
     * @throws IllegalStateException if the header is present but malformed
     */
    public static Optional<CausalDependencies> fromRecord(ConsumerRecord<?, ?> record) {
        return fromHeaders(record.headers());
    }

    /**
     * Extracts the causal dependencies from the {@code parsley-causal-dependencies} header in
     * {@code headers}.
     *
     * @param headers the record headers to read; must not be {@code null}
     * @return the embedded dependencies, or empty if the header is absent
     * @throws IllegalStateException if the header is present but malformed
     */
    public static Optional<CausalDependencies> fromHeaders(Headers headers) {
        Header header = headers.lastHeader(ParsleyHeader.CAUSAL_DEPENDENCIES);
        return header == null ? Optional.empty() : Optional.of(fromBytes(header.value()));
    }

    /** The backing clock; the engine works in {@link ParsleyClock} directly. */
    ParsleyClock clock() {
        return clock;
    }

    /** Wraps a {@link ParsleyClock} as public dependencies. */
    static CausalDependencies of(ParsleyClock clock) {
        return new CausalDependencies(clock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalDependencies other)) return false;
        return clock.equals(other.clock);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(clock);
    }

    @Override
    public String toString() {
        return "CausalDependencies" + clock;
    }
}
