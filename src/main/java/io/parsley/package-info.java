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
 *       {@link io.parsley.CausalDependencies#merge merge}. Its serialised size grows with the number
 *       of relevant topic-partitions and counts against Kafka's {@code message.max.bytes}, so prefer
 *       {@link io.parsley.CausalDependencies#fromRecord fromRecord} over a wide {@code frontier()}
 *       when propagating across services</li>
 *   <li>{@link io.parsley.CausalBufferPolicy} &mdash; what to do when a buffer limit fires:
 *       {@code forwardUnsafe}, {@code drop}, or {@code deadLetter}</li>
 *   <li>{@link io.parsley.CausalBufferLimit} &mdash; when to stop waiting:
 *       {@code ofDuration}, {@code ofSize}, or {@code first}</li>
 *   <li>{@link io.parsley.CausalViolationHandler} &mdash; violation callback, handed a
 *       {@link io.parsley.CausalViolation} carrying the causal gap</li>
 *   <li>{@link io.parsley.CausalFrontierListener} &mdash; frontier-advance callback, the public way to
 *       observe causal progress out of a {@link io.parsley.CausalProcessorSupplier}</li>
 * </ul>
 */
package io.parsley;
