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
     * @throws NullPointerException if {@code key} is null
     */
    public HeaderKV {
        if (key == null) {
            throw new NullPointerException("header key");
        }
    }
}
