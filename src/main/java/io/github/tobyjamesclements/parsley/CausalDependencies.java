package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * A producer-stamped set of causal requirements: the positions a consumer must have observed before
 * a record stamped with these dependencies may be delivered.
 *
 * <p>The usual flow at a topology edge is to hold a running {@code CausalDependencies} as your own
 * causal frontier: start from {@link #using(CausalTopics)} to bind a resolver, fold in each record
 * you consume with {@link #observe(ConsumerRecord)} — which accumulates the upstream's dependencies
 * <em>and</em> the consumed record's own position — then attach the result to each outbound record
 * with {@link #stamp(ProducerRecord)}. A one-to-one relay is {@code using(topics).observe(record)};
 * a fan-in chains an {@code observe} per input. To assert a dependency you did not consume, build one
 * with {@link #builder(CausalTopics)}. Serialise with {@link #toBytes()} / {@link #fromBytes(byte[])}.</p>
 *
 * The {@link #toBytes() serialised} form is {@code 5 + 28 × coordinates} bytes. An instance spanning
 * many partitions can breach Kafka's record-size limit ({@code message.max.bytes}, ~1&nbsp;MB by
 * default); the figure to watch is a wide-fan-in record that depends on many topic-partitions.
 */
public final class CausalDependencies {

    private final ParsleyClock clock;

    /**
     * The resolver bound for {@link #observe(ConsumerRecord)}, or {@code null} if unbound. Transient
     * convenience state only: it is never serialised, and never participates in {@link #equals} /
     * {@link #hashCode} — two instances with the same positions are equal whatever they are bound to.
     */
    private final @Nullable CausalTopics topics;

    private CausalDependencies(ParsleyClock clock) {
        this(clock, null);
    }

    private CausalDependencies(ParsleyClock clock, @Nullable CausalTopics topics) {
        this.clock = clock;
        this.topics = topics;
    }

    /**
     * Returns an empty instance with no positions recorded and no resolver bound. To accumulate
     * dependencies from consumed records, prefer {@link #using(CausalTopics)}, which binds a resolver
     * so {@link #observe(ConsumerRecord)} needs no per-call {@code topics} argument.
     *
     * @return an empty {@code CausalDependencies}
     */
    public static CausalDependencies empty() {
        return new CausalDependencies(ParsleyClock.empty());
    }

    /**
     * Returns an empty instance bound to {@code topics}, ready to accumulate consumed records with
     * {@link #observe(ConsumerRecord)}. This is the start of the consumer-side frontier chain: bind
     * the resolver once here, then {@code observe(record)} each record you consume without repeating
     * {@code topics}. The bound resolver flows through {@link #observe(ConsumerRecord)} and
     * {@link #merge(CausalDependencies)}, but is never serialised and never affects equality.
     *
     * @param topics the resolver mapping topic names to their Kafka UUIDs; must not be {@code null}
     * @return an empty {@code CausalDependencies} bound to {@code topics}
     */
    public static CausalDependencies using(CausalTopics topics) {
        Objects.requireNonNull(topics, "topics must not be null");
        return new CausalDependencies(ParsleyClock.empty(), topics);
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
            return new CausalDependencies(clock, topics);
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
        return new CausalDependencies(clock.merge(other.clock), topics != null ? topics : other.topics);
    }

    /**
     * Folds a consumed record into these dependencies and returns the result: the union of these
     * dependencies, the dependencies {@code record} itself carried, and {@code record}'s own position
     * {@code (topic, partition, offset)}.
     *
     * <p>This is the consumer-side frontier accumulator. A node consuming with a plain Kafka client
     * has no Parsley engine maintaining a frontier for it, so it maintains one here: bind a resolver
     * once with {@link #using(CausalTopics)}, {@code observe(record)} every record you consume, and
     * {@link #stamp(ProducerRecord) stamp} the result onto every record you produce, so downstream
     * consumers wait until they have observed everything this node did. A one-to-one relay is
     * {@code CausalDependencies.using(topics).observe(record)}; a fan-in (an output caused by several
     * inputs) chains an {@code observe} per input; a stateful node whose output reflects everything it
     * has consumed keeps a single instance and {@code observe}s into it across records. Repeated
     * positions on a coordinate take the maximum offset, so re-observing is safe.
     *
     * <p>A Parsley protocol watermark (see {@link #isWatermark(ConsumerRecord)}) is folded specially:
     * only the completeness frontier it carries is unioned in — its own {@code (topic, partition,
     * offset)} is <em>not</em>, because a watermark is metadata occupying an offset with no business
     * payload, and folding that offset would force downstream to wait on a record that delivers
     * nothing. This mirrors how a Parsley engine folds a received watermark
     * ({@code ParsleyEngine.onWatermark}), so a plain-client session advances across a service that
     * emitted only watermarks on this path while staying consistent with engine-side frontiers. The
     * watermark itself must not be surfaced to application code as a business record; gate that with
     * {@link #isWatermark(ConsumerRecord)}.
     *
     * <p>Requires a resolver to be bound — created via {@link #using(CausalTopics)} or
     * {@link #builder(CausalTopics)}, or carried through a prior {@code observe} / {@code merge}.
     *
     * @param record the consumed record to fold in; must not be {@code null}
     * @return a new {@code CausalDependencies} extended with {@code record}'s past and (for a business
     *         record) its own position
     * @throws IllegalStateException    if no resolver is bound, or if {@code record} carries a
     *                                  malformed dependencies header
     * @throws IllegalArgumentException if {@code record}'s topic cannot be resolved to a UUID (business
     *                                  records only; a watermark's topic is never resolved)
     */
    public CausalDependencies observe(ConsumerRecord<?, ?> record) {
        if (topics == null) {
            throw new IllegalStateException(
                    "no CausalTopics bound; start the accumulator with CausalDependencies.using(topics) "
                            + "(or CausalDependencies.builder(topics)) before calling observe(record)");
        }
        Objects.requireNonNull(record, "record must not be null");
        ParsleyClock carried = fromHeaders(record.headers()).map(deps -> deps.clock).orElse(ParsleyClock.empty());
        ParsleyClock merged = clock.merge(carried);
        ParsleyClock extended = isWatermark(record)
                ? merged
                : merged.observe(topics.topicId(record.topic()), record.partition(), record.offset());
        return new CausalDependencies(extended, topics);
    }

    /**
     * Returns {@code true} if {@code record} is a Parsley protocol watermark: a metadata record that
     * carries a node's completeness frontier but no business payload (null key, null value). A plain
     * Kafka client should still {@link #observe(ConsumerRecord) observe} a watermark — so its running
     * frontier advances across a service that emitted only watermarks on this path — but must not
     * surface it to application code as a business record. The usual consumer loop is to
     * {@code observe} every record and {@code continue} past those for which this returns {@code true}.
     *
     * @param record the consumed record to test; must not be {@code null}
     * @return {@code true} if the record carries the Parsley watermark marker header
     */
    public static boolean isWatermark(ConsumerRecord<?, ?> record) {
        Objects.requireNonNull(record, "record must not be null");
        return record.headers().lastHeader(ParsleyHeader.WATERMARK) != null;
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
