package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * A pure byte codec, being two total functions between a value and its bytes.
 *
 * <p>Codecs are the only serialization vocabulary user logic sees. Bridge a Kafka serde in
 * through {@link #fromSerde}.
 *
 * <p>A codec is never invoked with {@code null}. Null keys and null values pass through the
 * runtime untouched, so {@link #encode} and {@link #decode} may assume their argument is
 * present.
 *
 * @param <T> the type this codec converts to and from bytes
 */
public interface Codec<T> {

    /**
     * Encodes a value to bytes.
     *
     * @param value the value, never null
     * @return the encoded bytes
     */
    byte[] encode(T value);

    /**
     * Decodes bytes to a value.
     *
     * @param bytes the bytes, never null
     * @return the decoded value
     */
    T decode(byte[] bytes);

    /**
     * Returns a codec for UTF-8 strings.
     *
     * @return the codec
     */
    static Codec<String> utf8() {
        return of(s -> s.getBytes(StandardCharsets.UTF_8), b -> new String(b, StandardCharsets.UTF_8));
    }

    /**
     * Returns a codec for big-endian eight-byte longs.
     *
     * @return the codec
     */
    static Codec<Long> int64() {
        return of(v -> ByteBuffer.allocate(Long.BYTES).putLong(v).array(),
                b -> ByteBuffer.wrap(b).getLong());
    }

    /**
     * Returns a codec for raw bytes, passed through unchanged.
     *
     * @return the codec
     */
    static Codec<byte[]> bytes() {
        return of(b -> b, b -> b);
    }

    /**
     * Returns a codec from an encoder and a decoder.
     *
     * @param <T> the type the codec converts to and from bytes
     * @param encoder encodes a value to bytes
     * @param decoder decodes bytes to a value
     * @return the codec
     */
    static <T> Codec<T> of(Function<T, byte[]> encoder, Function<byte[], T> decoder) {
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                return encoder.apply(value);
            }

            @Override
            public T decode(byte[] bytes) {
                return decoder.apply(bytes);
            }
        };
    }

    /**
     * Bridges a Kafka serde into a codec.
     *
     * <p>The topic name is fixed at bridge time, because serdes take it per call and
     * schema-registry serdes derive their subjects from it. Use one bridged codec per topic.
     *
     * @param <T> the type the serde converts to and from bytes
     * @param serde the serde to bridge
     * @param topic the topic name to pass on every serde call
     * @return the codec
     */
    static <T> Codec<T> fromSerde(Serde<T> serde, String topic) {
        return of(v -> serde.serializer().serialize(topic, v),
                b -> serde.deserializer().deserialize(topic, b));
    }
}
