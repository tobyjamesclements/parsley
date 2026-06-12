package io.parsley.core;

import io.parsley.FenceToken;
import io.parsley.FenceTokenEncryption;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.core.internal.DefaultFenceToken;

/**
 * Creates and decodes {@link FenceToken}s.
 *
 * <p>The no-service overloads resolve the {@link FenceTokenEncryption} and
 * {@link VectorClockSerialiser} via {@link ParsleyServices#global()}, so both factories
 * share one encryption instance (and therefore one key) per JVM. The explicit-service
 * overloads exist for tests and for hosts that wire services themselves.
 *
 * <p><strong>Single clock type per process:</strong> {@link #decode(String)} trusts the
 * configured serialiser to produce the caller's clock type. Mixing different
 * {@link VectorClock} implementations in one process is not supported.
 */
public final class FenceTokens {

    private FenceTokens() {}

    /**
     * Wraps a {@link VectorClock} in a new {@link FenceToken} using the globally resolved
     * services.
     *
     * @param <T>   the concrete clock type
     * @param clock the causal clock to encode
     * @return a token whose {@link FenceToken#vectorClock()} equals {@code clock}
     */
    public static <T extends VectorClock<T>> FenceToken<T> of(T clock) {
        return of(clock, encryption(), FenceTokens.<T>serialiser());
    }

    /**
     * Wraps a {@link VectorClock} in a new {@link FenceToken} using the given services.
     *
     * @param <T>        the concrete clock type
     * @param clock      the causal clock to encode
     * @param encryption the encryption used by {@link FenceToken#encode()}
     * @param serialiser the serialiser used by {@link FenceToken#encode()}
     * @return a token whose {@link FenceToken#vectorClock()} equals {@code clock}
     */
    public static <T extends VectorClock<T>> FenceToken<T> of(
            T clock, FenceTokenEncryption encryption, VectorClockSerialiser<T> serialiser) {
        return new DefaultFenceToken<>(clock, encryption, serialiser);
    }

    /**
     * Decodes a previously {@link FenceToken#encode() encode}-d token string using the
     * globally resolved services.
     *
     * @param <T>     the concrete clock type produced by the configured serialiser
     * @param encoded the URL-safe token string
     * @return the decoded token
     * @throws IllegalStateException if decryption or deserialisation fails (e.g. tampered
     *                               input)
     */
    public static <T extends VectorClock<T>> FenceToken<T> decode(String encoded) {
        return decode(encoded, encryption(), FenceTokens.<T>serialiser());
    }

    /**
     * Decodes a previously {@link FenceToken#encode() encode}-d token string using the
     * given services.
     *
     * @param <T>        the concrete clock type
     * @param encoded    the URL-safe token string
     * @param encryption the encryption that produced the token
     * @param serialiser the serialiser that produced the token's clock payload
     * @return the decoded token
     * @throws IllegalStateException if decryption or deserialisation fails (e.g. tampered
     *                               input)
     */
    public static <T extends VectorClock<T>> FenceToken<T> decode(
            String encoded, FenceTokenEncryption encryption, VectorClockSerialiser<T> serialiser) {
        T clock = serialiser.deserialise(encryption.decrypt(encoded));
        return new DefaultFenceToken<>(clock, encryption, serialiser);
    }

    private static FenceTokenEncryption encryption() {
        return ParsleyServices.global().resolve(FenceTokenEncryption.class);
    }

    @SuppressWarnings("unchecked")
    private static <T extends VectorClock<T>> VectorClockSerialiser<T> serialiser() {
        return (VectorClockSerialiser<T>) ParsleyServices.global().resolve(VectorClockSerialiser.class);
    }
}
