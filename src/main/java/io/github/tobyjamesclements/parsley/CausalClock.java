package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A vector clock over causal coordinates: a set of {@code (topic, partition) → offset} entries
 * indexed by channel, the topic-partition, rather than by process (Fidge 1988; Mattern 1988). A
 * Kafka partition is the stable, durable axis that a process identity is not.
 *
 * <p>One type plays both classical vector-clock roles, depending on where the instance sits:
 * <ul>
 *   <li><strong>VT(m), attached to a record:</strong> its causal dependencies, the positions a
 *       consumer must have observed before the record may be delivered.</li>
 *   <li><strong>VT(p), accumulated at a topology edge:</strong> a running frontier of everything
 *       this node has observed.</li>
 * </ul>
 *
 * <p>At an edge, hold a running instance as your VT(p): {@link #using(Properties)} binds a resolver,
 * {@link #observe(ConsumerRecord)} folds in each record you consume, and
 * {@link #stamp(ProducerRecord)} attaches the result to each record you produce. Use
 * {@link #builder(Properties)} to assert a dependency you did not consume, and {@link #toBytes()} /
 * {@link #fromBytes(byte[])} to serialise. The serialised form is {@code 5 + 28 × coordinates} bytes
 * and counts against Kafka's {@code message.max.bytes}, so watch a wide fan-in that depends on many
 * topic-partitions.
 */
public final class CausalClock {

    private final ParsleyVectorClock clock;

    /**
     * The resolver bound for {@link #observe(ConsumerRecord)}, or {@code null} if unbound. Transient
     * convenience state only: it is never serialised, and never participates in {@link #equals} /
     * {@link #hashCode} — two instances with the same positions are equal whatever they are bound to.
     */
    private final @Nullable ParsleyTopics topics;

    private CausalClock(ParsleyVectorClock clock) {
        this(clock, null);
    }

    private CausalClock(ParsleyVectorClock clock, @Nullable ParsleyTopics topics) {
        this.clock = clock;
        this.topics = topics;
    }

    /**
     * Returns an empty instance with no positions recorded and no resolver bound. To accumulate
     * a clock from consumed records, prefer {@link #using(Properties)}, which binds a resolver
     * so {@link #observe(ConsumerRecord)} needs no per-call resolver argument.
     *
     * @return an empty {@code CausalClock}
     */
    public static CausalClock empty() {
        return new CausalClock(ParsleyVectorClock.empty());
    }

    /**
     * Returns an empty instance bound to a resolver over {@code props}, the start of a consumer-side
     * frontier: bind the resolver once here, then {@link #observe(ConsumerRecord)} each record you
     * consume. The bound resolver flows through {@code observe} and {@link #merge(CausalClock)} but
     * is never serialised and never affects equality.
     *
     * @param props the Kafka client configuration to resolve topic UUIDs through; must not be
     *              {@code null}
     * @return an empty {@code CausalClock} bound to a resolver over {@code props}
     */
    public static CausalClock using(Properties props) {
        Objects.requireNonNull(props, "props must not be null");
        return using(ParsleyTopics.of(props));
    }

    /**
     * Returns an empty instance bound to a resolver over a fixed name&rarr;UUID map — the broker-free
     * path for tests or callers that already hold the UUIDs. See {@link #using(Properties)}.
     *
     * @param topicIds the topic names mapped to their Kafka UUIDs; must not be {@code null}
     * @return an empty {@code CausalClock} bound to a resolver over {@code topicIds}
     */
    public static CausalClock using(Map<String, Uuid> topicIds) {
        Objects.requireNonNull(topicIds, "topicIds must not be null");
        return using(ParsleyTopics.of(topicIds));
    }

    /** Package-private: the shared implementation the public overloads above delegate to, and the
     * entry point same-package tests use directly with a reusable {@link ParsleyTopics} constant. */
    static CausalClock using(ParsleyTopics topics) {
        Objects.requireNonNull(topics, "topics must not be null");
        return new CausalClock(ParsleyVectorClock.empty(), topics);
    }

    /**
     * Returns a new builder that resolves topic names to their stable UUIDs through {@code props}.
     *
     * @param props the Kafka client configuration to resolve topic UUIDs through; must not be
     *              {@code null}
     * @return a new {@code Builder}
     */
    public static Builder builder(Properties props) {
        Objects.requireNonNull(props, "props must not be null");
        return new Builder(ParsleyTopics.of(props));
    }

    /**
     * Returns a new builder that resolves topic names to their stable UUIDs through a fixed
     * name&rarr;UUID map — the broker-free path for tests or callers that already hold the UUIDs. See
     * {@link #builder(Properties)}.
     *
     * @param topicIds the topic names mapped to their Kafka UUIDs; must not be {@code null}
     * @return a new {@code Builder}
     */
    public static Builder builder(Map<String, Uuid> topicIds) {
        Objects.requireNonNull(topicIds, "topicIds must not be null");
        return new Builder(ParsleyTopics.of(topicIds));
    }

    /** Package-private: the entry point same-package tests use directly with a reusable
     * {@link ParsleyTopics} constant. */
    static Builder builder(ParsleyTopics topics) {
        return new Builder(topics);
    }

    /**
     * Builder for {@link CausalClock}.
     */
    public static final class Builder {
        private final ParsleyTopics topics;
        private ParsleyVectorClock clock = ParsleyVectorClock.empty();

        private Builder(ParsleyTopics topics) {
            this.topics = Objects.requireNonNull(topics, "topics must not be null");
        }

        /**
         * Requires that {@code (topic, partition)} has been observed at offset {@code offset} or
         * later. The topic name is resolved to its stable UUID through the builder's bound resolver.
         * If a requirement already exists for that coordinate, the higher offset wins.
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
         * Returns a new {@code CausalClock} from the positions accumulated so far.
         *
         * @return a new {@code CausalClock}
         */
        public CausalClock build() {
            return new CausalClock(clock, topics);
        }
    }

    /**
     * Returns the causal union of this clock and {@code other} — the per-coordinate maximum
     * offset (the vector-clock join). Use this to combine the clocks of several consumed records
     * before stamping a fan-in record.
     *
     * @param other the clock to merge in; must not be {@code null}
     * @return a new {@code CausalClock} dominating both inputs
     */
    public CausalClock merge(CausalClock other) {
        Objects.requireNonNull(other, "other must not be null");
        return new CausalClock(clock.merge(other.clock), topics != null ? topics : other.topics);
    }

    /**
     * Folds a consumed record into this clock and returns the result: the union of this clock, the
     * clock {@code record} carried, and, for a business record, {@code record}'s own position
     * {@code (topic, partition, offset)}. Offsets take the per-coordinate maximum, so re-observing is
     * safe. Requires a resolver, bound via {@link #using(Properties)} or {@link #builder(Properties)}
     * or carried through a prior {@code observe} / {@code merge}.
     *
     * <p>A null message (see {@link #isNullMessage(ConsumerRecord)}) folds in only the completeness
     * frontier it carries, never its own offset, since it delivers no business payload and gating on
     * that offset would stall downstream on a record that carries nothing.
     *
     * @param record the consumed record to fold in; must not be {@code null}
     * @return a new {@code CausalClock} extended with {@code record}'s past and, for a business
     *         record, its own position
     * @throws IllegalStateException    if no resolver is bound, or if {@code record} carries a
     *                                  malformed clock header
     * @throws IllegalArgumentException if {@code record}'s topic cannot be resolved to a UUID (business
     *                                  records only; a null message's topic is never resolved)
     */
    public CausalClock observe(ConsumerRecord<?, ?> record) {
        if (topics == null) {
            throw new IllegalStateException(
                    "no resolver bound; start the accumulator with CausalClock.using(props) "
                            + "(or CausalClock.builder(props)) before calling observe(record)");
        }
        Objects.requireNonNull(record, "record must not be null");
        ParsleyVectorClock carried = fromHeaders(record.headers()).map(deps -> deps.clock).orElse(ParsleyVectorClock.empty());
        ParsleyVectorClock merged = clock.merge(carried);
        ParsleyVectorClock extended = isNullMessage(record)
                ? merged
                : merged.observe(topics.topicId(record.topic()), record.partition(), record.offset());
        return new CausalClock(extended, topics);
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley null message: a metadata record carrying a
     * node's completeness frontier but no business payload. A plain Kafka client should still
     * {@link #observe(ConsumerRecord) observe} one, so its frontier advances across a service that
     * emitted only null messages, but must not surface it as a business record. The usual loop is to
     * {@code observe} every record and {@code continue} past those this flags.
     *
     * @param record the consumed record to test; must not be {@code null}
     * @return {@code true} if the record carries the Parsley null-message marker header
     */
    public static boolean isNullMessage(ConsumerRecord<?, ?> record) {
        Objects.requireNonNull(record, "record must not be null");
        return record.headers().lastHeader(ParsleyHeader.NULL_MESSAGE) != null;
    }

    /**
     * Returns a copy of {@code record} carrying this clock in its
     * {@code parsley-causal-clock} header, ready to send through a plain Kafka producer. The
     * input record is never mutated: a fresh header set is built (preserving every other header), and
     * stamping is idempotent — any existing clock header is replaced, not duplicated.
     *
     * @param record the record to stamp; must not be {@code null}
     * @param <K>    the record key type
     * @param <V>    the record value type
     * @return a new {@code ProducerRecord} with the clock header attached
     */
    public <K, V> ProducerRecord<K, V> stamp(ProducerRecord<K, V> record) {
        Objects.requireNonNull(record, "record must not be null");
        Headers stamped = ParsleyHeader.replacingClock(record.headers(), clock.toBytes());
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
     * Reconstructs a {@code CausalClock} from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised bytes; must not be {@code null}
     * @return the deserialised {@code CausalClock}
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    public static CausalClock fromBytes(byte[] bytes) {
        return new CausalClock(ParsleyVectorClock.fromBytes(bytes));
    }

    /**
     * Extracts the causal clock a Parsley-stamped message carries in its
     * {@code parsley-causal-clock} header.
     *
     * @param record the consumed record; must not be {@code null}
     * @return the embedded clock, or empty if the record carries no clock header
     * @throws IllegalStateException if the header is present but malformed
     */
    public static Optional<CausalClock> fromRecord(ConsumerRecord<?, ?> record) {
        return fromHeaders(record.headers());
    }

    /**
     * Extracts the causal clock from the {@code parsley-causal-clock} header in {@code headers}.
     *
     * @param headers the record headers to read; must not be {@code null}
     * @return the embedded clock, or empty if the header is absent
     * @throws IllegalStateException if the header is present but malformed
     */
    public static Optional<CausalClock> fromHeaders(Headers headers) {
        Header header = headers.lastHeader(ParsleyHeader.CAUSAL_CLOCK);
        return header == null ? Optional.empty() : Optional.of(fromBytes(header.value()));
    }

    /** The backing clock; the causal-broadcast core works in {@link ParsleyVectorClock} directly. */
    ParsleyVectorClock clock() {
        return clock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalClock other)) return false;
        return clock.equals(other.clock);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(clock);
    }

    @Override
    public String toString() {
        return "CausalClock" + clock;
    }
}
