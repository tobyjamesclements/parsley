package io.parsley;

import io.parsley.internal.DefaultFenceToken;

import java.util.ServiceLoader;

/**
 * An opaque, URL-safe causal fence token that encodes a {@link VectorClock}.
 *
 * <p>A fence token is a compact, encrypted string that can be passed across service boundaries
 * (e.g. as an HTTP header or message attribute) to assert <em>"the receiver must have caught up
 * to this causal state before proceeding."</em> The receiving service decodes the token back
 * into a {@link VectorClock} and waits until its own frontier dominates that clock.
 *
 * <h2>Usage</h2>
 * <p>On the producer side — after a {@code io.parsley.streams.CausalConsumer} or {@code io.parsley.streams.CausalProducer}
 * has processed messages — obtain a token from the current frontier:
 * <pre>{@code
 * FenceToken token = consumer.fenceToken();
 * String encoded = token.encode();           // safe to embed in HTTP headers, queue messages, etc.
 * }</pre>
 *
 * <p>On the consumer side — wait until the local frontier dominates the received token:
 * <pre>{@code
 * FenceToken token = FenceToken.decode(encoded);
 * // poll until consumer.frontier().dominates(token.vectorClock())
 * }</pre>
 *
 * <h2>Dependencies</h2>
 * <p>Requires {@link FenceTokenEncryption} and {@link VectorClockSerialiser} on the classpath,
 * registered via {@link ServiceLoader}. The defaults are {@code parsley-crypto-jdk} and
 * {@code parsley-serialisation} respectively.
 */
public interface FenceToken {

    /**
     * Encodes this token as a URL-safe Base64 string.
     *
     * <p>The encoding is encrypted via the {@link FenceTokenEncryption} service, so the
     * embedded {@link VectorClock} is not readable by third parties without the same key.
     *
     * @return a non-blank, URL-safe string representation of this token
     */
    String encode();

    /**
     * Returns the {@link VectorClock} embedded in this token.
     *
     * @return the causal clock this token represents
     */
    VectorClock vectorClock();

    /**
     * Decodes a previously {@link #encode()}-d token string.
     *
     * @param encoded the URL-safe token string produced by {@link #encode()}
     * @return the decoded {@code FenceToken}
     * @throws IllegalStateException if decryption or deserialisation fails (e.g. tampered input)
     */
    static FenceToken decode(String encoded) {
        VectorClock clock = Services.SERIALISER.deserialise(Services.ENCRYPTION.decrypt(encoded));
        return new DefaultFenceToken(clock, Services.ENCRYPTION, Services.SERIALISER);
    }

    /**
     * Wraps a {@link VectorClock} in a new {@code FenceToken}.
     *
     * @param clock the causal clock to encode
     * @return a {@code FenceToken} whose {@link #vectorClock()} equals {@code clock}
     */
    static FenceToken of(VectorClock clock) {
        return new DefaultFenceToken(clock, Services.ENCRYPTION, Services.SERIALISER);
    }

    /**
     * Eagerly loads the {@link FenceTokenEncryption} and {@link VectorClockSerialiser} services.
     * Both are loaded once per JVM so that {@link #of} and {@link #decode} share the same
     * encryption key throughout the process lifetime.
     */
    final class Services {
        static final FenceTokenEncryption ENCRYPTION = ServiceLoader.load(FenceTokenEncryption.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No FenceTokenEncryption on classpath — add parsley-crypto-jdk"));
        static final VectorClockSerialiser SERIALISER = ServiceLoader.load(VectorClockSerialiser.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No VectorClockSerialiser on classpath — add parsley-serialisation"));

        private Services() {}
    }
}
