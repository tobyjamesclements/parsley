package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A producer-stamped set of causal requirements: the positions a consumer must have observed before
 * a record stamped with this clock may be delivered.
 *
 * <p>Clock keys are Kafka topic UUIDs, so topic deletion and recreation produce a different identity
 * even when the name is reused. Use {@link #advance(TopicPartition, long)} for convenience when
 * only a topic name is available; it derives the UUID via {@link CausalPosition#nameUuid}.
 *
 * <p>Instances are immutable. {@link #advance} returns a new clock.
 *
 * <h2>Propagating causal context across services</h2>
 * Use {@link #fromRecord(ConsumerRecord)} to read the upstream producer's clock off a consumed
 * record (this is usually the right choice — it carries exactly the partitions the producer
 * depended on). Use {@link CausalFrontier#asDependencies()} only when the read genuinely depends on
 * <em>everything</em> the consumer has processed (e.g. an aggregator), since the frontier carries
 * every partition ever seen. Serialise with {@link #toBytes()} / {@link #fromBytes(byte[])}.
 *
 * <h2>Clock size and the {@code message.max.bytes} ceiling</h2>
 * The {@link #toBytes() serialised} clock is {@code 5 + 28 × entries} bytes. A clock spanning
 * many partitions can breach Kafka's record-size limit ({@code message.max.bytes}, ~1&nbsp;MB by
 * default). The automatic Streams stamping path is bounded by the number of source topics in the
 * subtopology; the figure to watch is a manual {@link CausalFrontier#asDependencies()} call on a
 * wide-fan-in consumer.
 */
public final class CausalDependencies {

    /** Leading byte of the wire format; shared with {@link CausalFrontier}. */
    private static final byte WIRE_VERSION = 1;

    private record Key(Uuid topicId, int partition) {}

    private final Map<Key, Long> required; // always immutable

    private CausalDependencies(Map<Key, Long> required) {
        this.required = required; // already immutable (Map.copyOf called by callers)
    }

    /**
     * Returns an empty clock with no positions recorded.
     *
     * @return an empty {@code CausalDependencies}
     */
    public static CausalDependencies empty() {
        return new CausalDependencies(Map.of());
    }

    /**
     * Returns a new clock with {@code (topicId, partition)} advanced to
     * {@code max(current, offset)}.
     *
     * @param topicId   the topic UUID
     * @param partition the partition index
     * @param offset    the required offset
     * @return a new {@code CausalDependencies} with the updated position
     */
    public CausalDependencies advance(Uuid topicId, int partition, long offset) {
        Map<Key, Long> next = new HashMap<>(required);
        next.merge(new Key(topicId, partition), offset, Math::max);
        return new CausalDependencies(Map.copyOf(next));
    }

    /**
     * Convenience overload that derives the topic UUID via {@link CausalPosition#nameUuid}.
     *
     * @param tp     the topic-partition to advance
     * @param offset the required offset
     * @return a new {@code CausalDependencies} with the updated position
     */
    public CausalDependencies advance(TopicPartition tp, long offset) {
        return advance(CausalPosition.nameUuid(tp.topic()), tp.partition(), offset);
    }

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything this clock
     * requires — for every position in this clock, the frontier's observed offset is ≥ this
     * clock's required offset.
     *
     * @param frontier the frontier to test against; must not be {@code null}
     * @return {@code true} if the frontier has caught up with this clock
     */
    public boolean satisfiedBy(CausalFrontier frontier) {
        for (Map.Entry<Key, Long> entry : required.entrySet()) {
            if (frontier.observed(entry.getKey().topicId(), entry.getKey().partition()) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the causal gap between this clock (treated as a requirement) and {@code frontier}:
     * for every position where this clock requires a higher offset than the frontier has observed,
     * the result contains a {@link CausalPosition} with the <em>shortfall</em>
     * ({@code required − observed}, counting an absent frontier position as {@code -1} so the gap is
     * {@code required + 1}). The result is empty exactly when {@link #satisfiedBy}.
     *
     * @param frontier the frontier to measure against; must not be {@code null}
     * @return per-position shortfalls; empty if the frontier already satisfies this clock
     */
    public List<CausalPosition> missingAgainst(CausalFrontier frontier) {
        List<CausalPosition> gap = new ArrayList<>();
        for (Map.Entry<Key, Long> entry : required.entrySet()) {
            long req = entry.getValue();
            long obs = frontier.observed(entry.getKey().topicId(), entry.getKey().partition());
            if (obs < req) {
                gap.add(new CausalPosition(entry.getKey().topicId(), entry.getKey().partition(), req - obs));
            }
        }
        return List.copyOf(gap);
    }

    /**
     * Returns the positions in this clock as an unordered list of {@link CausalPosition}s.
     *
     * @return the dependency positions; never {@code null}
     */
    public List<CausalPosition> dependencies() {
        List<CausalPosition> list = new ArrayList<>(required.size());
        required.forEach((k, v) -> list.add(new CausalPosition(k.topicId(), k.partition(), v)));
        return list;
    }

    /**
     * Serialises this clock to a compact binary form compatible with {@link CausalFrontier#toBytes()}.
     *
     * <h2>Wire format</h2>
     * <pre>
     * [byte  version=0x01]
     * [int   count]
     * for each entry:
     *   [long  topicId.mostSignificantBits]
     *   [long  topicId.leastSignificantBits]
     *   [int   partition]
     *   [long  offset]
     * </pre>
     *
     * @return the serialised clock
     */
    public byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(WIRE_VERSION);
            dos.writeInt(required.size());
            for (Map.Entry<Key, Long> entry : required.entrySet()) {
                dos.writeLong(entry.getKey().topicId().getMostSignificantBits());
                dos.writeLong(entry.getKey().topicId().getLeastSignificantBits());
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("CausalDependencies serialisation failed", e);
        }
    }

    /**
     * Reconstructs a clock from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised clock; must not be {@code null}
     * @return the deserialised {@code CausalDependencies}
     * @throws IllegalStateException if {@code bytes} is not valid, including an unrecognised version
     */
    public static CausalDependencies fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte version = dis.readByte();
            if (version != WIRE_VERSION) {
                throw new IllegalStateException(
                        "unsupported CausalDependencies wire version: " + version + " (expected " + WIRE_VERSION + ")");
            }
            int count = dis.readInt();
            Map<Key, Long> map = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                long msb = dis.readLong();
                long lsb = dis.readLong();
                int partition = dis.readInt();
                long offset = dis.readLong();
                map.put(new Key(new Uuid(msb, lsb), partition), offset);
            }
            return new CausalDependencies(Map.copyOf(map));
        } catch (IOException e) {
            throw new IllegalStateException("CausalDependencies deserialisation failed", e);
        }
    }

    /**
     * Extracts the causal clock a Parsley-stamped message carries in its
     * {@code parsley-vector-clock} header.
     *
     * @param record the consumed record; must not be {@code null}
     * @return the embedded clock, or empty if the record carried no clock header
     * @throws IllegalStateException if the header is present but not a valid serialised clock
     */
    public static Optional<CausalDependencies> fromRecord(ConsumerRecord<?, ?> record) {
        return fromHeaders(record.headers());
    }

    /**
     * Extracts the causal clock from the {@code parsley-vector-clock} header in {@code headers}.
     *
     * @param headers the record headers to read; must not be {@code null}
     * @return the embedded clock, or empty if no clock header is present
     * @throws IllegalStateException if the header is present but not a valid serialised clock
     */
    public static Optional<CausalDependencies> fromHeaders(Headers headers) {
        Header header = headers.lastHeader(ParsleyAttributes.VECTOR_CLOCK);
        return header == null ? Optional.empty() : Optional.of(fromBytes(header.value()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CausalDependencies other)) return false;
        return required.equals(other.required);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(required);
    }

    @Override
    public String toString() {
        return "CausalDependencies" + dependencies();
    }
}
