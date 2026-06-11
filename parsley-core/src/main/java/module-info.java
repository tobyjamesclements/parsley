/**
 * Core abstractions for causal consistency in Kafka Streams topologies.
 *
 * <p>This module is dependency-free and defines:
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; a causal progress snapshot across Kafka partitions</li>
 *   <li>{@link io.parsley.FenceToken} &mdash; an opaque, cross-service causal ordering assertion</li>
 *   <li>{@link io.parsley.BufferLimit} and {@link io.parsley.BufferingPolicy} &mdash; when and how to
 *       handle records whose causal dependencies are not yet met</li>
 *   <li>{@link io.parsley.FenceTokenEncryption} and {@link io.parsley.VectorClockSerialiser} &mdash;
 *       SPI interfaces for pluggable encryption and serialisation</li>
 *   <li>{@link io.parsley.ParsleyMetrics} &mdash; observability hook for buffer and frontier events</li>
 * </ul>
 *
 * <p>Default SPI implementations are provided by the {@code io.parsley.crypto.jdk} and
 * {@code io.parsley.serialisation} modules and are discovered automatically via
 * {@link java.util.ServiceLoader}.
 */
module io.parsley {
    exports io.parsley;
    exports io.parsley.internal to io.parsley.serialisation,
                                   io.parsley.buffer,
                                   io.parsley.streams;
    uses io.parsley.FenceTokenEncryption;
    uses io.parsley.VectorClockSerialiser;
}
