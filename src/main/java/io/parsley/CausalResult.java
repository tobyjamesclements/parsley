package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * How a record's delivery related to causal ordering, stamped on every record forwarded by a
 * {@link CausalProcessorSupplier} or returned by a {@link CausalConsumer} under the
 * {@code parsley-causal-result} header.
 *
 * <ul>
 *   <li>{@link #SATISFIED} — the record's causal dependencies were satisfied by the frontier at
 *       delivery time, whether immediately, after a wait, or trivially (no dependencies claimed,
 *       or an undecodable dependencies header — both are treated as an empty, vacuously satisfied
 *       set; the corrupt-header case is logged but not treated as a violation).
 *   <li>{@link #EVICTED} — the record was still waiting on unsatisfied dependencies when the
 *       configured {@link CausalBufferLimit} fired, and was forwarded anyway. Logged with the
 *       causal gap.
 * </ul>
 */
public enum CausalResult {
    SATISFIED,
    EVICTED;

    /**
     * Returns {@code true} for {@link #SATISFIED} — the record's causal dependencies held at
     * delivery time.
     *
     * @return whether this result represents safe (in-order) delivery
     */
    public boolean isSafe() {
        return this == SATISFIED;
    }

    /**
     * Reads the {@code parsley-causal-result} header from {@code record}, if present.
     *
     * @param record the record to read; must not be {@code null}
     * @return the decoded result, or empty if the header is absent
     */
    public static Optional<CausalResult> fromRecord(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(ParsleyAttributes.CAUSAL_RESULT);
        if (header == null) {
            return Optional.empty();
        }
        return Optional.of(CausalResult.valueOf(new String(header.value(), UTF_8)));
    }
}
