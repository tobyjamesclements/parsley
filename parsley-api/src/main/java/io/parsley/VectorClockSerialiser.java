package io.parsley;

/**
 * SPI for binary serialisation and deserialisation of {@link VectorClock} instances.
 *
 * <p>Implementations are discovered at runtime via {@link java.util.ServiceLoader}.
 * Register a custom implementation via {@code META-INF/services/io.parsley.VectorClockSerialiser}
 * (classpath mode) or a {@code provides} declaration in {@code module-info.java} (module mode).
 * When more than one implementation is registered, select one by declaring
 * {@code io.parsley.VectorClockSerialiser=<implementation class>} in a
 * {@code parsley.properties} file at the classpath root.
 *
 * <p>Each broker module ships a serialiser for its own clock type — e.g.
 * {@code io.parsley.kafka.KafkaVectorClockSerialiser} (compact binary format, bundled with
 * {@code parsley-kafka}). Replace it when interoperability with an existing serialisation
 * scheme is required.
 *
 * @param <T> the concrete {@link VectorClock} type this serialiser handles
 */
public interface VectorClockSerialiser<T extends VectorClock<T>> {

    /**
     * Serialises a {@link VectorClock} to a byte array.
     *
     * @param clock the clock to serialise; must not be {@code null}
     * @return a non-null byte array representing {@code clock}
     */
    byte[] serialise(T clock);

    /**
     * Deserialises a byte array produced by {@link #serialise} back into a {@link VectorClock}.
     *
     * @param bytes the bytes to deserialise; must not be {@code null}
     * @return the reconstructed clock
     * @throws IllegalStateException if the byte array is malformed
     */
    T deserialise(byte[] bytes);
}
