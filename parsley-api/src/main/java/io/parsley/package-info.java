/**
 * The Parsley API: every abstraction, value type, and SPI for causal consistency atop
 * totally ordered messaging technologies.
 *
 * <p>This package is dependency-free and broker-neutral. The buffering engine lives in
 * {@code parsley-core}; broker integrations (e.g. {@code parsley-kafka}) bind these
 * abstractions to a concrete messaging technology. SPI implementors depend only on this
 * module.
 *
 * <h2>Key types</h2>
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; opaque, self-typed causal progress snapshot;
 *       check readiness with {@link io.parsley.VectorClock#satisfiedBy satisfiedBy} and
 *       combine clocks with {@link io.parsley.VectorClock#merge merge};
 *       {@link io.parsley.VectorClocks} supplies the standard map-based algebra</li>
 *   <li>{@link io.parsley.FenceToken} &mdash; opaque cross-service ordering assertion</li>
 *   <li>{@link io.parsley.CausalRecord} &mdash; broker-neutral message envelope processed by
 *       the buffering engine</li>
 *   <li>{@link io.parsley.BufferingPolicy} &mdash; what to do when a buffer limit fires:
 *       {@link io.parsley.BufferingPolicy.Ignore Ignore},
 *       {@link io.parsley.BufferingPolicy.Drop Drop},
 *       {@link io.parsley.BufferingPolicy.DeadLetter DeadLetter}</li>
 *   <li>{@link io.parsley.BufferLimit} &mdash; when to stop waiting:
 *       {@link io.parsley.BufferLimit.DurationLimit DurationLimit},
 *       {@link io.parsley.BufferLimit.SizeLimit SizeLimit},
 *       {@link io.parsley.BufferLimit.FirstLimit FirstLimit}</li>
 *   <li>{@link io.parsley.CausalBuffer} /
 *       {@link io.parsley.FenceTokenEncryption} /
 *       {@link io.parsley.VectorClockSerialiser} &mdash; SPI interfaces</li>
 *   <li>{@link io.parsley.CausalViolationHandler} &mdash; violation callback</li>
 *   <li>{@link io.parsley.ParsleyMetrics} &mdash; observability hook</li>
 * </ul>
 */
package io.parsley;
