package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * Evicts when the first of {@code limits} fires; copies {@code limits} defensively.
 */
record ParsleyFirstLimit(List<CausalBoundedBufferLimit> limits) implements CausalBoundedBufferLimit {
    ParsleyFirstLimit {
        limits = List.copyOf(limits);
        if (limits.isEmpty()) {
            throw new IllegalArgumentException("at least one limit is required");
        }
    }
}
