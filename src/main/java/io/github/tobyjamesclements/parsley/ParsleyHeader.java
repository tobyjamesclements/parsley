package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * A single record header — a {@code (key, value)} pair — plus Parsley's header-key vocabulary.
 *
 * <p>This type owns the names of every header Parsley reads or writes and the rule that distinguishes
 * Parsley's internal routing headers (the {@code _parsley_*} prefix) from user headers, so that
 * knowledge lives with the header type rather than scattered as loose constants.
 *
 * @param key   the header name; must not be {@code null} or empty
 * @param value the raw header bytes; may be {@code null}
 */
record ParsleyHeader(String key, byte @Nullable [] value) {

    /** Prefix marking a header as Parsley-internal routing metadata, stripped before user delivery. */
    static final String INTERNAL_PREFIX = "_parsley_";

    /**
     * Header carrying a record's serialised causal clock — the vector timestamp VT(m) the producer
     * stamped it with ({@link CausalClock}).
     */
    static final String CAUSAL_CLOCK = "parsley-causal-clock";

    /**
     * Header marking a record as a Parsley null message (Chandy–Misra–Bryant sense: a
     * timestamp-carrying record whose value is literally null). A null message carries no business
     * payload and exists solely to propagate the emitting node's vector time to
     * downstream processors when the user delegate did not forward any business record for the
     * delivered input ({@link ParsleyGossip}). The {@code _parsley_} prefix means it is stripped
     * from user view by {@code ParsleyMessage.userHeaders} and from any public header API.
     */
    static final String NULL_MESSAGE = "_parsley_null_message";

    // Explicit canonical constructor: NullAway does not propagate the type-use @Nullable from an
    // array record component to the implicit constructor parameter, so annotate it here directly.
    ParsleyHeader(String key, byte @Nullable [] value) {
        Objects.requireNonNull(key, "header key must not be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("header key must not be empty");
        }
        this.key = key;
        this.value = value;
    }

    /**
     * Returns a fresh, empty, mutable {@link Headers} to populate via {@code add(String, byte[])}.
     * Kafka exposes no public {@code Headers} factory and its only implementation lives in an
     * {@code internals} package; a throwaway {@link ProducerRecord} hands back an empty mutable
     * instance through the public API, which is what we want without depending on that internals type.
     */
    static Headers mutableHeaders() {
        return new ProducerRecord<byte[], byte[]>("", null, null).headers();
    }

    /**
     * Returns a fresh {@link Headers} containing every header from {@code original} except {@link
     * #CAUSAL_CLOCK}, with a new {@code CAUSAL_CLOCK} header appended carrying {@code clock}. Used
     * to re-stamp a record's causal clock without duplicating the header (any prior clock header is
     * replaced, not accumulated) or disturbing any other header.
     */
    static Headers replacingClock(Headers original, byte[] clock) {
        Headers stamped = mutableHeaders();
        for (Header header : original) {
            if (!header.key().equals(CAUSAL_CLOCK)) {
                stamped.add(header.key(), header.value());
            }
        }
        stamped.add(CAUSAL_CLOCK, clock);
        return stamped;
    }

}
