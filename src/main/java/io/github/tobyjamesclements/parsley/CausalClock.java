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
 * A vector clock (Fidge 1988; Mattern 1988, "Virtual Time and Global States of Distributed
 * Systems") over causal coordinates: a set of {@code (topic, partition) → offset} entries, indexed
 * by <em>channel</em> — the topic-partition — rather than by process, a stated variant of the
 * classical form (Kafka's partition is the stable, durable axis a process identity is not).
 *
 * <p>One type plays both classical vector-clock roles, and which one an instance is playing depends
 * on where it sits:
 * <ul>
 *   <li><strong>Attached to a record, it is the message timestamp VT(m):</strong> the positions a
 *       consumer must have observed before the record may be delivered — its causal
 *       dependencies.</li>
 *   <li><strong>Accumulated at a topology edge, it is the process clock VT(p):</strong> a running
 *       frontier of everything this node has observed, folded forward with every consumed record
 *       and stamped onto every produced one.</li>
 * </ul>
 *
 * <p>The usual flow at a topology edge is to hold a running {@code CausalClock} as your own VT(p):
 * start from {@link #using(Properties)} to bind a resolver, fold in each record you consume with
 * {@link #observe(ConsumerRecord)} — which accumulates the upstream's clock <em>and</em> the
 * consumed record's own position — then attach the result to each outbound record with
 * {@link #stamp(ProducerRecord)}. A one-to-one relay is {@code using(props).observe(record)};
 * a fan-in chains an {@code observe} per input. To assert a dependency you did not consume, build
 * one with {@link #builder(Properties)}. Serialise with {@link #toBytes()} /
 * {@link #fromBytes(byte[])}.
 *
 * The {@link #toBytes() serialised} form is {@code 5 + 28 × coordinates} bytes. An instance spanning
 * many partitions can breach Kafka's record-size limit ({@code message.max.bytes}, ~1&nbsp;MB by
 * default); the figure to watch is a wide-fan-in record that depends on many topic-partitions.
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
     * Returns an empty instance bound to a resolver backed by {@code props}, ready to accumulate
     * consumed records with {@link #observe(ConsumerRecord)}. This is the start of the consumer-side
     * frontier chain: bind the resolver once here, then {@code observe(record)} each record you
     * consume without repeating {@code props}. The bound resolver flows through
     * {@link #observe(ConsumerRecord)} and {@link #merge(CausalClock)}, but is never
     * serialised and never affects equality.
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
     * clock {@code record} itself carried, and {@code record}'s own position
     * {@code (topic, partition, offset)}.
     *
     * <p>This is the consumer-side frontier accumulator — the VT(p) fold. A node consuming with a
     * plain Kafka client has no Parsley causal-broadcast core maintaining a frontier for it, so it
     * maintains one here: bind a resolver
     * once with {@link #using(Properties)}, {@code observe(record)} every record you consume, and
     * {@link #stamp(ProducerRecord) stamp} the result onto every record you produce, so downstream
     * consumers wait until they have observed everything this node did. A one-to-one relay is
     * {@code CausalClock.using(props).observe(record)}; a fan-in (an output caused by several
     * inputs) chains an {@code observe} per input; a stateful node whose output reflects everything it
     * has consumed keeps a single instance and {@code observe}s into it across records. Repeated
     * positions on a coordinate take the maximum offset, so re-observing is safe.
     *
     * <p>A Parsley null message (see {@link #isNullMessage(ConsumerRecord)}) is folded specially:
     * only the completeness frontier it carries is unioned in — its own {@code (topic, partition,
     * offset)} is <em>not</em>, because a null message is metadata occupying an offset with no
     * business payload, and folding that offset would force downstream to wait on a record that
     * delivers nothing. This mirrors how a Parsley causal-broadcast core folds a received null
     * message ({@code ParsleyGossip.receive}), so a plain-client session advances across a service
     * that emitted only null messages on this path while staying consistent with core-side
     * frontiers. The null message itself must not be surfaced to application code as a business
     * record; gate that with {@link #isNullMessage(ConsumerRecord)}.
     *
     * <p>Requires a resolver to be bound — created via {@link #using(Properties)} or
     * {@link #builder(Properties)}, or carried through a prior {@code observe} / {@code merge}.
     *
     * @param record the consumed record to fold in; must not be {@code null}
     * @return a new {@code CausalClock} extended with {@code record}'s past and (for a business
     *         record) its own position
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
     * Returns {@code true} if {@code record} is a Parsley null message (Chandy–Misra–Bryant sense):
     * a metadata record that carries a node's completeness frontier but no business payload (null
     * key, null value). A plain Kafka client should still {@link #observe(ConsumerRecord) observe}
     * a null message — so its running frontier advances across a service that emitted only null
     * messages on this path — but must not surface it to application code as a business record. The
     * usual consumer loop is to {@code observe} every record and {@code continue} past those for
     * which this returns {@code true}.
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
