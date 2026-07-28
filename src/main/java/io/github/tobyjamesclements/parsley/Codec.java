package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * A pure byte codec: two total functions between a value and its bytes. Codecs are the only
 * serialization vocabulary in the functional core; Kafka serdes stay at the imperative edge
 * and bridge in through {@link #fromSerde}.
 *
 * <p>A codec is never invoked with {@code null}: null keys and null values pass through the
 * runtime untouched, so {@code encode} and {@code decode} may assume their argument is
 * present.
 */
public interface Codec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);

    /** UTF-8 strings. */
    static Codec<String> utf8() {
        return of(s -> s.getBytes(StandardCharsets.UTF_8), b -> new String(b, StandardCharsets.UTF_8));
    }

    /** Big-endian eight-byte longs. */
    static Codec<Long> int64() {
        return of(v -> ByteBuffer.allocate(Long.BYTES).putLong(v).array(),
                b -> ByteBuffer.wrap(b).getLong());
    }

    /** Raw bytes, passed through unchanged. */
    static Codec<byte[]> bytes() {
        return of(b -> b, b -> b);
    }

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
     * Bridges a Kafka serde into the core. The topic name is fixed at bridge time because
     * serdes take it per call (schema-registry serdes derive subjects from it); use one
     * bridged codec per topic.
     */
    static <T> Codec<T> fromSerde(Serde<T> serde, String topic) {
        return of(v -> serde.serializer().serialize(topic, v),
                b -> serde.deserializer().deserialize(topic, b));
    }
}
