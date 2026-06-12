package io.parsley;

/**
 * An opaque, URL-safe causal fence token that encodes a {@link VectorClock}.
 *
 * <p>A fence token is a compact, encrypted string that can be passed across service boundaries
 * (e.g. as an HTTP header or message attribute) to assert <em>"the receiver must have caught up
 * to this causal state before proceeding."</em> The receiving service decodes the token back
 * into a {@link VectorClock} and waits until the token's clock is satisfied by the frontier.
 *
 * <h2>Example Usage</h2>
 * <p>On the producer side — after a causal consumer or producer has processed messages —
 * obtain a token from the current frontier:
 * <pre>{@code
 * FenceToken<MyClock> token = consumer.fenceToken();
 * String encoded = token.encode();           // safe to embed in HTTP headers, queue messages, etc.
 * }</pre>
 *
 * <p>On the consumer side — wait until the token's clock is satisfied by the local frontier:
 * <pre>{@code
 * FenceToken<MyClock> token = FenceTokens.decode(encoded);
 * // poll until token.vectorClock().satisfiedBy(consumer.frontier())
 * }</pre>
 *
 * <p>Tokens are created and decoded via the {@code io.parsley.core.FenceTokens} factory in
 * {@code parsley-core}, which resolves the {@link FenceTokenEncryption} and
 * {@link VectorClockSerialiser} services.
 *
 * @param <T> the concrete {@link VectorClock} type embedded in this token
 */
public interface FenceToken<T extends VectorClock<T>> {

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
    T vectorClock();
}
