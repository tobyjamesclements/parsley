/**
 * Parsley: causal consistency for Kafka. A single package whose public surface is interfaces and
 * records; all implementations are package-private and obtained through factory methods.
 *
 * <h2>Entry points (interfaces with static factories)</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalProcessorSupplier} &mdash; {@code CausalProcessorSupplier.create(...)} wraps your own
 *       Kafka Streams {@code Processor} so its state access and {@code forward}s run behind the causal
 *       guarantee; drop it into {@code stream(...).process(...)}</li>
 *   <li>{@link io.parsley.CausalConsumer} &mdash; {@code CausalConsumer.create(...)} delivers records
 *       in causal order over a {@code poll()} API</li>
 *   <li>{@link io.parsley.CausalProducer} &mdash; {@code CausalProducer.create(...)} stamps the causal
 *       clock onto produced records</li>
 * </ul>
 *
 * <h2>Key value types</h2>
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
 *   <li>{@link io.parsley.ViolationHandler} &mdash; violation callback, handed a
 *       {@link io.parsley.Violation} carrying the causal gap</li>
 *   <li>{@link io.parsley.FrontierListener} &mdash; frontier-advance callback, the public way to
 *       observe causal progress out of a {@link io.parsley.CausalProcessorSupplier}</li>
 * </ul>
 */
package io.parsley;
