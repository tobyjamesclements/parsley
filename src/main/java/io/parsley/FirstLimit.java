package io.parsley;

import java.util.List;

/**
 * Evicts when the first of {@code limits} fires; copies {@code limits} defensively.
 */
record FirstLimit(List<CausalBufferLimit> limits) implements CausalBufferLimit {
    FirstLimit {
        limits = List.copyOf(limits);
        if (limits.isEmpty()) {
            throw new IllegalArgumentException("at least one limit is required");
        }
    }
}
