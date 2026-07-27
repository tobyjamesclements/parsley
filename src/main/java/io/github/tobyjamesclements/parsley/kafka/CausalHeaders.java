package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.core.Clock;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * The wire vocabulary: one header, {@value #CLOCK}, carrying the record's dependency clock.
 * Absent means the producer claims nothing. There are no other protocol headers and no
 * protocol records.
 */
public final class CausalHeaders {

    public static final String CLOCK = "parsley-clock";

    private CausalHeaders() {}

    /** Reads the clock header; null when absent. Undecodable bytes throw (fail closed). */
    public static Clock read(Headers headers) {
        Header h = headers.lastHeader(CLOCK);
        return h == null || h.value() == null ? null : Clock.deserialize(h.value());
    }

    /** Replaces any existing clock header with {@code clock}. */
    public static void write(Headers headers, Clock clock) {
        headers.remove(CLOCK);
        headers.add(CLOCK, clock.serialize());
    }
}
