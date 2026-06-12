/**
 * The Parsley API for causal consistency atop totally ordered messaging technologies.
 *
 * <p>This module is dependency-free and defines:
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; an opaque, self-typed causal progress snapshot</li>
 *   <li>{@link io.parsley.FenceToken} &mdash; an opaque, cross-service causal ordering assertion</li>
 *   <li>{@link io.parsley.CausalRecord} &mdash; a broker-neutral message envelope</li>
 *   <li>{@link io.parsley.BufferLimit} and {@link io.parsley.BufferingPolicy} &mdash; when and how to
 *       handle records whose causal dependencies are not yet met</li>
 *   <li>{@link io.parsley.CausalBuffer}, {@link io.parsley.FenceTokenEncryption} and
 *       {@link io.parsley.VectorClockSerialiser} &mdash; SPI interfaces for pluggable buffering,
 *       encryption and serialisation</li>
 *   <li>{@link io.parsley.ParsleyMetrics} &mdash; observability hook for buffer and frontier events</li>
 * </ul>
 *
 * <p>SPI implementors depend only on this module. Service discovery and resolution is
 * performed by the {@code io.parsley.core} module.
 */
module io.parsley {
    exports io.parsley;
}
