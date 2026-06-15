/**
 * Parsley: causal consistency for Kafka. A single package whose public surface is interfaces and
 * records; all implementations are package-private and obtained through factory methods.
 *
 * <h2>Entry points (interfaces with static factories)</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalProcessorSupplier} &mdash; {@code CausalProcessors.builder(...).build()} wraps your own
 *       Kafka Streams {@code Processor} so its state access and {@code forward}s run behind the causal
 *       guarantee; drop it into {@code stream(...).process(...)}</li>
 *   <li>{@link io.parsley.CausalConsumer} &mdash; {@code CausalConsumers.builder(...).build()} delivers
 *       records in causal order over a {@code poll()} API</li>
 *   <li>{@link io.parsley.CausalProducer} &mdash; {@code CausalProducers.builder(...).build()} stamps the
 *       causal clock onto produced records</li>
 * </ul>
 *
 * <h2>Key value types</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalDependencies} &mdash; a snapshot of causal progress (highest offset
 *       per {@link org.apache.kafka.common.TopicPartition}); test readiness with
 *       {@link io.parsley.CausalDependencies#satisfiedBy satisfiedBy} and combine clocks with
 *       {@link io.parsley.CausalDependencies#merge merge}</li>
 *   <li>{@link io.parsley.CausalBufferingPolicy} &mdash; what to do when a buffer limit fires:
 *       {@link io.parsley.CausalBufferingPolicy.ForwardUnsafe ForwardUnsafe},
 *       {@link io.parsley.CausalBufferingPolicy.Drop Drop},
 *       {@link io.parsley.CausalBufferingPolicy.DeadLetter DeadLetter}</li>
 *   <li>{@link io.parsley.CausalBufferLimit} &mdash; when to stop waiting:
 *       {@link io.parsley.CausalBufferLimit.DurationLimit DurationLimit},
 *       {@link io.parsley.CausalBufferLimit.SizeLimit SizeLimit},
 *       {@link io.parsley.CausalBufferLimit.FirstLimit FirstLimit}</li>
 *   <li>{@link io.parsley.CausalViolationHandler} &mdash; violation callback, handed a
 *       {@link io.parsley.CausalViolation} carrying the causal gap</li>
 *   <li>{@link io.parsley.CausalFrontierListener} &mdash; frontier-advance callback, the public way to
 *       observe causal progress out of a {@link io.parsley.CausalProcessorSupplier}</li>
 * </ul>
 */
package io.parsley;
