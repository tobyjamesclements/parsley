package io.parsley;

import java.util.List;

/**
 * Evicts when the first of {@code limits} fires; copies {@code limits} defensively.
 */
record ParsleyFirstLimit(List<CausalBufferLimit> limits) implements CausalBufferLimit {
    ParsleyFirstLimit {
        limits = List.copyOf(limits);
        if (limits.isEmpty()) {
            throw new IllegalArgumentException("at least one limit is required");
        }
    }
}
