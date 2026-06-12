package io.parsley;

/**
 * A single key/value metadata attribute carried by a {@link CausalRecord}.
 *
 * <p>Broker adapters map their native header/attribute representation onto this type —
 * e.g. Kafka record headers or Kinesis message attributes.
 *
 * @param key   the attribute name; must not be {@code null}
 * @param value the raw attribute bytes; may be {@code null} if the broker permits null values
 */
public record CausalHeader(String key, byte[] value) { }
