package io.parsley;

/**
 * SPI for binary serialisation and deserialisation of {@link VectorClock} instances.
 *
 * <p>Implementations are discovered at runtime via {@link java.util.ServiceLoader}.
 * Register a custom implementation via {@code META-INF/services/io.parsley.VectorClockSerialiser}
 * (classpath mode) or a {@code provides} declaration in {@code module-info.java} (module mode).
 *
 * <p>The default implementation is
 * {@code io.parsley.serialisation.DefaultVectorClockSerialiser} (compact binary format).
 * Replace it when interoperability with an existing serialisation scheme is required.
 */
public interface VectorClockSerialiser {

    /**
     * Serialises a {@link VectorClock} to a byte array.
     *
     * @param clock the clock to serialise; must not be {@code null}
     * @return a non-null byte array representing {@code clock}
     */
    byte[] serialise(VectorClock clock);

    /**
     * Deserialises a byte array produced by {@link #serialise} back into a {@link VectorClock}.
     *
     * @param bytes the bytes to deserialise; must not be {@code null}
     * @return the reconstructed {@code VectorClock}
     * @throws IllegalStateException if the byte array is malformed
     */
    VectorClock deserialise(byte[] bytes);
}
