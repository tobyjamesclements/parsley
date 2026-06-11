/**
 * Core abstractions for causal consistency.
 *
 * <p>This module is dependency-free and defines:
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; an opaque causal progress snapshot</li>
 *   <li>{@link io.parsley.FenceToken} &mdash; an opaque, cross-service causal ordering assertion</li>
 *   <li>{@link io.parsley.BufferLimit} and {@link io.parsley.BufferingPolicy} &mdash; when and how to
 *       handle records whose causal dependencies are not yet met</li>
 *   <li>{@link io.parsley.FenceTokenEncryption} and {@link io.parsley.VectorClockSerialiser} &mdash;
 *       SPI interfaces for pluggable encryption and serialisation</li>
 *   <li>{@link io.parsley.ParsleyMetrics} &mdash; observability hook for buffer and frontier events</li>
 * </ul>
 *
 * <p>Default SPI implementations are provided by the {@code io.parsley.crypto.jdk.aes} and
 * {@code io.parsley.kafka} modules and are discovered automatically via
 * {@link java.util.ServiceLoader}.
 */
module io.parsley {
    exports io.parsley;
    exports io.parsley.internal to io.parsley.kafka;
    uses io.parsley.FenceTokenEncryption;
    uses io.parsley.VectorClockSerialiser;
}
