package io.parsley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ParsleyHeader}'s header-key vocabulary — specifically the rule that distinguishes
 * Parsley-internal routing headers (the {@code _parsley_} prefix) from user headers, which the
 * processor intake relies on to keep internal headers off the user-facing record.
 */
class ParsleyHeaderTest {

    /**
     * A header whose key carries the {@code _parsley_} prefix is recognised as internal.
     *
     * Asserts {@code isInternal()} is {@code true} for a prefixed key.
     */
    @Test
    void aPrefixedKeyIsInternal() {
        assertTrue(new ParsleyHeader("_parsley_src_topic", new byte[0]).isInternal(),
                "a key with the _parsley_ prefix must be internal");
    }

    /**
     * An ordinary user header key is not internal.
     *
     * Asserts {@code isInternal()} is {@code false} for an unprefixed key.
     */
    @Test
    void aUserKeyIsNotInternal() {
        assertFalse(new ParsleyHeader("trace-id", new byte[0]).isInternal(),
                "a user header key must not be internal");
    }

    /**
     * The causal-dependencies header is a user-facing header, not an internal routing one — its key
     * does not carry the {@code _parsley_} prefix.
     *
     * Asserts {@code isInternal()} is {@code false} for the causal-dependencies key.
     */
    @Test
    void theCausalDependenciesHeaderIsNotInternal() {
        assertFalse(new ParsleyHeader(ParsleyHeader.CAUSAL_DEPENDENCIES, new byte[0]).isInternal(),
                "the causal-dependencies header must not be treated as internal");
    }

    /**
     * A header key must not be empty — there is no meaningful internal/user classification for it.
     *
     * Asserts that constructing a header with an empty key throws {@code IllegalArgumentException}.
     */
    @Test
    void anEmptyKeyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ParsleyHeader("", new byte[0]),
                "an empty header key must be rejected");
    }
}
