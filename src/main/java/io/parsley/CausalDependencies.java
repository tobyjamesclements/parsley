package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * a record stamped with these dependencies may be delivered.
 *
 * <p>Keys are Kafka topic UUIDs, so topic deletion and recreation produce a different identity
 * even when the name is reused.
 *
 * <p>Instances are immutable. {@link #advance} returns a new instance.
 *
 * <h2>Propagating causal context across services</h2>
 * Use {@link #fromRecord(ConsumerRecord)} to read the upstream producer's dependencies off a
 * consumed record (this is usually the right choice — it carries exactly the partitions the
 * producer depended on). Use {@link CausalFrontier#toDependencies()} only when the read genuinely
 * depends on <em>everything</em> the consumer has processed (e.g. an aggregator), since the
 * frontier carries every partition ever seen. Serialise with {@link #toBytes()} /
 * {@link #fromBytes(byte[])}.
 *
 * <h2>Serialised size and the {@code message.max.bytes} ceiling</h2>
 * The {@link #toBytes() serialised} form is {@code 5 + 28 × entries} bytes. An instance spanning
 * many partitions can breach Kafka's record-size limit ({@code message.max.bytes}, ~1&nbsp;MB by
 * default). The automatic Streams stamping path is bounded by the number of source topics in the
 * subtopology; the figure to watch is a manual {@link CausalFrontier#toDependencies()} call on a
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
     * Returns an empty instance with no positions recorded.
     *
     * @return an empty {@code CausalDependencies}
     */
    public static CausalDependencies empty() {
        return new CausalDependencies(Map.of());
    }

    /**
     * Returns a new instance with {@code (topicId, partition)} advanced to
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
     * Returns {@code true} if {@code frontier} has observed at least everything these dependencies
     * require — for every position in these dependencies, the frontier's observed offset is ≥ the
     * required offset.
     *
     * @param frontier the frontier to test against; must not be {@code null}
     * @return {@code true} if the frontier satisfies these dependencies
     */
    public boolean isSatisfiedBy(CausalFrontier frontier) {
        for (Map.Entry<Key, Long> entry : required.entrySet()) {
            if (frontier.offsetFor(entry.getKey().topicId(), entry.getKey().partition()) < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the causal gap between these dependencies and {@code frontier}: for every position
     * where these dependencies require a higher offset than the frontier has observed, the result
     * contains a {@link CausalPosition} with the <em>shortfall</em> ({@code required − observed},
     * counting an absent frontier position as {@code -1} so the gap is {@code required + 1}).
     * The result is empty exactly when {@link #isSatisfiedBy}.
     *
     * @param frontier the frontier to measure against; must not be {@code null}
     * @return per-position shortfalls; empty if the frontier already satisfies these dependencies
     */
    public List<CausalPosition> findMissing(CausalFrontier frontier) {
        List<CausalPosition> gap = new ArrayList<>();
        for (Map.Entry<Key, Long> entry : required.entrySet()) {
            long req = entry.getValue();
            long obs = frontier.offsetFor(entry.getKey().topicId(), entry.getKey().partition());
            if (obs < req) {
                gap.add(new CausalPosition(entry.getKey().topicId(), entry.getKey().partition(), req - obs));
            }
        }
        return List.copyOf(gap);
    }

    /**
     * Returns a new instance with the entry for {@code (topicId, partition)} removed if its
     * required offset is ≥ {@code selfOffset}; otherwise returns {@code this} unchanged.
     *
     * <p>Used by the engine to strip self-referential entries before evaluation: a record at
     * offset {@code O} on partition {@code (T, P)} that requires {@code (T, P) ≥ O} can never be
     * satisfied by any other record, so the entry is redundant and is removed.
     *
     * @param topicId    the source topic UUID
     * @param partition  the source partition index
     * @param selfOffset the source offset of the record being admitted
     * @return a copy without the circular entry, or {@code this} if none was present
     */
    CausalDependencies withoutSelfReference(Uuid topicId, int partition, long selfOffset) {
        Long req = required.get(new Key(topicId, partition));
        if (req == null || req < selfOffset) return this;
        Map<Key, Long> next = new HashMap<>(required);
        next.remove(new Key(topicId, partition));
        return new CausalDependencies(Map.copyOf(next));
    }

    /**
     * Returns the positions in these dependencies as an unordered list of {@link CausalPosition}s.
     *
     * @return the dependency positions; never {@code null}
     */
    public List<CausalPosition> dependencies() {
        List<CausalPosition> list = new ArrayList<>(required.size());
        required.forEach((k, v) -> list.add(new CausalPosition(k.topicId(), k.partition(), v)));
        return list;
    }

    /**
     * Serialises to a compact binary form compatible with {@link CausalFrontier#toBytes()}.
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
     * @return the serialised bytes
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
     * Reconstructs a {@code CausalDependencies} from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised bytes; must not be {@code null}
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
