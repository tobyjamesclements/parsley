package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link ParsleyHeader}'s constructor contract. The internal/user header-key distinction
 * itself (the {@code _parsley_} prefix, {@link ParsleyHeader#INTERNAL_PREFIX}) is exercised
 * end-to-end where it is enforced — the processor intake keeps prefixed headers off the
 * user-facing record, covered by the topology tests.
 */
class ParsleyHeaderTest {

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
