/**
 * The shared Parsley vocabulary: the value and configuration types used across the producer,
 * consumer, and stream packages.
 *
 * <h2>Key types</h2>
 * <ul>
 *   <li>{@link io.parsley.VectorClock} &mdash; a snapshot of causal progress (highest offset
 *       per {@link org.apache.kafka.common.TopicPartition}); test readiness with
 *       {@link io.parsley.VectorClock#satisfiedBy satisfiedBy} and combine clocks with
 *       {@link io.parsley.VectorClock#merge merge}</li>
 *   <li>{@link io.parsley.BufferingPolicy} &mdash; what to do when a buffer limit fires:
 *       {@link io.parsley.BufferingPolicy.ForwardUnsafe ForwardUnsafe},
 *       {@link io.parsley.BufferingPolicy.Drop Drop},
 *       {@link io.parsley.BufferingPolicy.DeadLetter DeadLetter}</li>
 *   <li>{@link io.parsley.BufferLimit} &mdash; when to stop waiting:
 *       {@link io.parsley.BufferLimit.DurationLimit DurationLimit},
 *       {@link io.parsley.BufferLimit.SizeLimit SizeLimit},
 *       {@link io.parsley.BufferLimit.FirstLimit FirstLimit}</li>
 *   <li>{@link io.parsley.CausalViolationHandler} &mdash; violation callback</li>
 *   <li>{@link io.parsley.Metrics} &mdash; observability hook</li>
 * </ul>
 */
package io.parsley;
