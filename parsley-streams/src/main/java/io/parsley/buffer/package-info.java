/**
 * Causal buffer that holds Kafka consumer records until their vector-clock dependencies
 * have been satisfied by the consumer's current frontier.
 *
 * <p>The central abstraction is {@link io.parsley.buffer.CausalBuffer}: records are
 * {@link io.parsley.buffer.CausalBuffer#add added} as they arrive, then
 * {@link io.parsley.buffer.CausalBuffer#drain drained} as the frontier advances.
 * Records that cannot be released within the configured {@link io.parsley.BufferLimit}
 * are {@link io.parsley.buffer.CausalBuffer#evict evicted} according to the
 * {@link io.parsley.BufferingPolicy}.
 *
 * <p>Use {@link io.parsley.buffer.CausalBuffers} to obtain a buffer instance:
 * <pre>{@code
 * CausalBuffer<String, Order> buffer = CausalBuffers.create(
 *     BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *     new DefaultVectorClockSerialiser()
 * );
 * }</pre>
 *
 * <h2>Violation handling</h2>
 * <p>When a record is evicted, a {@link io.parsley.buffer.CausalViolationHandler} is
 * notified with the record and the {@link io.parsley.CausalViolationReason}.
 * Use {@link io.parsley.buffer.CausalViolationHandler#throwing throwing()} to surface
 * violations as exceptions, or {@link io.parsley.buffer.CausalViolationHandler#noop noop()}
 * to ignore them silently.
 */
package io.parsley.buffer;
