package io.parsley;

import io.parsley.internal.Attributes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A snapshot of causal progress: the highest offset observed on each Kafka
 * {@link TopicPartition}.
 *
 * <p>A producer stamps its current clock onto every message; a consumer maintains a
 * <em>frontier</em> clock reflecting everything it has processed. A message may be delivered
 * once the consumer's frontier {@linkplain #satisfiedBy satisfies} the message's clock — i.e.
 * the consumer has already observed everything the message causally depends on.
 *
 * <p>Instances are immutable; {@link #advance} and {@link #merge} return new clocks.
 *
 * <h2>Propagating causal context across services</h2>
 * A clock is also the unit of causal context you can hand to another service — for example, to
 * tell a client "the reply has been processed; don't read until you have caught up to here." Parsley
 * deliberately ships no encryption or transport for this (that is your concern); the building blocks
 * are:
 * <ol>
 *   <li>Obtain the context: {@link #fromRecord(ConsumerRecord)} on the consumed message (the
 *       upstream producer's clock), or {@code consumer.frontier()} for everything you have consumed.
 *   <li>Serialise it with {@link #toBytes()}, then apply <em>your own</em> encryption and a URL-safe
 *       encoding (e.g. Base64) and place it in an HTTP header.
 *   <li>On the receiving side, decode and decrypt, rebuild with {@link #fromBytes(byte[])}, and gate
 *       the downstream read with {@link #satisfiedBy(VectorClock)} against that store's frontier.
 * </ol>
 *
 * @param positions the partition-to-highest-offset map; copied defensively
 */
public record VectorClock(Map<TopicPartition, Long> positions) {

    /**
     * Canonical constructor; defensively copies {@code positions}.
     */
    public VectorClock(Map<TopicPartition, Long> positions) {
        this.positions = Map.copyOf(positions);
    }

    /**
     * Returns an empty clock with no partition positions recorded.
     *
     * @return an empty {@code VectorClock}
     */
    public static VectorClock empty() {
        return new VectorClock(Map.of());
    }

    /**
     * Returns a new clock with {@code tp} advanced to {@code max(current, offset)}.
     *
     * @param tp     the topic-partition to advance
     * @param offset the newly observed offset
     * @return a new {@code VectorClock} with the updated position
     */
    public VectorClock advance(TopicPartition tp, long offset) {
        Map<TopicPartition, Long> advanced = new HashMap<>(positions);
        advanced.merge(tp, offset, Math::max);
        return new VectorClock(advanced);
    }

    /**
     * Returns {@code true} if {@code frontier} has observed at least everything this clock
     * requires — for every partition in this clock, the frontier's offset is ≥ this clock's
     * offset. Partitions absent from {@code frontier} are unsatisfied.
     *
     * @param frontier the frontier clock to test against; must not be {@code null}
     * @return {@code true} if the frontier has caught up with this clock
     */
    public boolean satisfiedBy(VectorClock frontier) {
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            Long observed = frontier.positions.get(entry.getKey());
            if (observed == null || observed < entry.getValue()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the causal gap between this clock (treated as a requirement) and {@code frontier}
     * (what has been observed): for every partition where this clock requires a higher offset than
     * {@code frontier} has observed, the entry maps the partition to the <em>missing</em> amount
     * ({@code required − observed}, counting an absent frontier position as {@code -1} so the gap is
     * {@code required + 1}). The result is empty exactly when {@code this.satisfiedBy(frontier)}.
     *
     * @param frontier the frontier to measure against; must not be {@code null}
     * @return the per-partition shortfall, or an empty map if the frontier already satisfies this
     *         clock
     */
    public Map<TopicPartition, Long> missingAgainst(VectorClock frontier) {
        Map<TopicPartition, Long> gap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            long required = entry.getValue();
            long observed = frontier.positions.getOrDefault(entry.getKey(), -1L);
            if (observed < required) {
                gap.put(entry.getKey(), required - observed);
            }
        }
        return Map.copyOf(gap);
    }

    /**
     * Returns the causal union of this clock and {@code other}: the per-partition maximum of
     * the two position maps.
     *
     * @param other the clock to merge with; must not be {@code null}
     * @return a new {@code VectorClock} dominating both operands
     */
    public VectorClock merge(VectorClock other) {
        Map<TopicPartition, Long> merged = new HashMap<>(positions);
        other.positions.forEach((tp, offset) -> merged.merge(tp, offset, Math::max));
        return new VectorClock(merged);
    }

    /**
     * Serialises this clock to a compact binary form.
     *
     * <h2>Wire format</h2>
     * <pre>
     * [int   count]
     * for each entry:
     *   [short topicLen] [byte[] topic (UTF-8)] [int partition] [long offset]
     * </pre>
     *
     * @return the serialised clock
     */
    public byte[] toBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(positions.size());
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                byte[] topicBytes = entry.getKey().topic().getBytes(StandardCharsets.UTF_8);
                dos.writeShort(topicBytes.length);
                dos.write(topicBytes);
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("VectorClock serialisation failed", e);
        }
    }

    /**
     * Reconstructs a clock from its {@link #toBytes() serialised} form.
     *
     * @param bytes the serialised clock; must not be {@code null}
     * @return the deserialised {@code VectorClock}
     * @throws IllegalStateException if {@code bytes} is not a valid serialised clock
     */
    public static VectorClock fromBytes(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int count = dis.readInt();
            Map<TopicPartition, Long> positions = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                int topicLen = dis.readUnsignedShort();
                byte[] topicBytes = new byte[topicLen];
                dis.readFully(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                int partition = dis.readInt();
                long offset = dis.readLong();
                positions.put(new TopicPartition(topic, partition), offset);
            }
            return new VectorClock(positions);
        } catch (IOException e) {
            throw new IllegalStateException("VectorClock deserialisation failed", e);
        }
    }

    /**
     * Extracts the causal clock a Parsley-stamped message carries in its
     * {@code parsley-vector-clock} header.
     *
     * <p>Use this to read the upstream producer's causal context off a record you consumed — for
     * example, to propagate it to a client (see the class-level
     * <a href="#propagating-causal-context-across-services">propagation</a> notes).
     *
     * @param record the consumed record; must not be {@code null}
     * @return the embedded clock, or empty if the record carried no clock header
     * @throws IllegalStateException if the header is present but not a valid serialised clock
     */
    public static Optional<VectorClock> fromRecord(ConsumerRecord<?, ?> record) {
        return fromHeaders(record.headers());
    }

    /**
     * Extracts the causal clock from the {@code parsley-vector-clock} header in {@code headers}.
     *
     * @param headers the record headers to read; must not be {@code null}
     * @return the embedded clock, or empty if no clock header is present
     * @throws IllegalStateException if the header is present but not a valid serialised clock
     */
    public static Optional<VectorClock> fromHeaders(Headers headers) {
        Header header = headers.lastHeader(Attributes.VECTOR_CLOCK);
        return header == null ? Optional.empty() : Optional.of(fromBytes(header.value()));
    }

    @Override
    public String toString() {
        return "VectorClock" + positions;
    }
}
