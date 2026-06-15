package io.parsley;

/**
 * A single key/value record header, carried internally on a {@link ParsleyRecord}.
 *
 * @param key   the header name; must not be {@code null}
 * @param value the raw header bytes; may be {@code null}
 */
record ParsleyHeader(String key, byte[] value) { }
