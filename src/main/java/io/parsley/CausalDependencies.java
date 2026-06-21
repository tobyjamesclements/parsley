package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Objects;
import java.util.Optional;

/**
 * A producer-stamped set of causal requirements: the positions a consumer must have observed before
 * a record stamped with these dependencies may be delivered.
 *
 * <p>Build one with {@link #builder()} from the {@link CausalTopic} identities you already register,
 * or read the dependencies a consumed record carries with {@link #fromRecord(ConsumerRecord)} — the
 * latter is usually the right choice when propagating causal context across services, since it
 * carries exactly the positions the upstream producer depended on. Serialise with {@link #toBytes()}
 * / {@link #fromBytes(byte[])}.
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
     * Returns a new builder for constructing a {@code CausalDependencies} instance.
     *
     * @return a new {@code Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link CausalDependencies}.
     */
    public static final class Builder {
        private ParsleyClock clock = ParsleyClock.empty();

        private Builder() {}

        /**
         * Requires that {@code (topic, partition)} has been observed at offset {@code offset} or
         * later. If a requirement already exists for that coordinate, the higher offset wins.
         *
         * @param topic     the topic whose stable causal identity to require; must not be {@code null}
         * @param partition the partition index
         * @param offset    the offset (inclusive) the consumer must have observed
         * @return this builder
         */
        public Builder require(CausalTopic topic, int partition, long offset) {
            clock = clock.observe(topic.topicId(), partition, offset);
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
        Header header = headers.lastHeader(ParsleyAttributes.CAUSAL_DEPENDENCIES);
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
