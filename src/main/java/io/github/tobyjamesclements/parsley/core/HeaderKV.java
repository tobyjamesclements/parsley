package io.github.tobyjamesclements.parsley.core;

/**
 * One message header.
 *
 * @param key   the header name, never {@code null}
 * @param value the header bytes, which may be {@code null}
 */
public record HeaderKV(String key, byte[] value) {
    /**
     * Validates the header name.
     *
     * @throws IllegalArgumentException if {@code key} is null
     */
    public HeaderKV {
        if (key == null) {
            throw new IllegalArgumentException("header key must be non-null");
        }
    }
}
