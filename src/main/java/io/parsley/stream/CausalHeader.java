package io.parsley.stream;

/**
 * A single key/value record header, carried internally on a {@link CausalRecord}.
 *
 * @param key   the header name; must not be {@code null}
 * @param value the raw header bytes; may be {@code null}
 */
record CausalHeader(String key, byte[] value) { }
