package io.github.tobyjamesclements.parsley;

/**
 * One message header.
 *
 * @param key   the header name, never {@code null}
 * @param value the header bytes, which may be {@code null}
 */
public record Header(String key, byte[] value) {
    /**
     * Validates the header name.
     *
     * @throws IllegalArgumentException if {@code key} is null
     */
    public Header {
        if (key == null) {
            throw new IllegalArgumentException("header key must be non-null");
        }
    }
}
