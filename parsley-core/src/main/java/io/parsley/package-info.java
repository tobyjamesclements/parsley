/**
 * Core abstractions for causal consistency: vector clocks, fence tokens, buffer limits
 * and policies, SPI interfaces, and the observability hook.
 *
 * <p>This package is dependency-free. All Kafka-specific integration lives in the
 * {@code io.parsley.buffer} and {@code io.parsley.streams} packages.
 *
 * <h2>Key types</h2>
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; causal progress snapshot; compare clocks with
 *       {@link io.parsley.VectorClock#dominates dominates} /
 *       {@link io.parsley.VectorClock#dominatedBy dominatedBy}</li>
 *   <li>{@link io.parsley.FenceToken} &mdash; opaque cross-service ordering assertion</li>
 *   <li>{@link io.parsley.BufferingPolicy} &mdash; what to do when a buffer limit fires:
 *       {@link io.parsley.BufferingPolicy.Ignore Ignore},
 *       {@link io.parsley.BufferingPolicy.Drop Drop},
 *       {@link io.parsley.BufferingPolicy.DeadLetter DeadLetter}</li>
 *   <li>{@link io.parsley.BufferLimit} &mdash; when to stop waiting:
 *       {@link io.parsley.BufferLimit.DurationLimit DurationLimit},
 *       {@link io.parsley.BufferLimit.SizeLimit SizeLimit},
 *       {@link io.parsley.BufferLimit.FirstLimit FirstLimit}</li>
 *   <li>{@link io.parsley.FenceTokenEncryption} /
 *       {@link io.parsley.VectorClockSerialiser} &mdash; SPI interfaces</li>
 *   <li>{@link io.parsley.ParsleyMetrics} &mdash; observability hook</li>
 * </ul>
 */
package io.parsley;
