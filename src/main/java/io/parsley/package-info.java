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
 *       causal dependencies onto produced records</li>
 * </ul>
 *
 * <h2>Key value types</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalFrontier} &mdash; the highest offset observed per topic UUID;
 *       combine two frontiers with {@link io.parsley.CausalFrontier#merge merge} and convert to
 *       dependencies with {@link io.parsley.CausalFrontier#toDependencies toDependencies}</li>
 *   <li>{@link io.parsley.CausalDependencies} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered); test readiness
 *       with {@link io.parsley.CausalDependencies#isSatisfiedBy isSatisfiedBy}. Its serialised size
 *       grows with the number of relevant topic-partitions and counts against Kafka's
 *       {@code message.max.bytes}, so prefer {@link io.parsley.CausalDependencies#fromRecord
 *       fromRecord} over a wide {@code frontier().toDependencies()} when propagating across
 *       services</li>
 *   <li>{@link io.parsley.CausalBufferLimit} &mdash; how long to wait for a record's dependencies
 *       before forwarding it anyway: {@code ofDuration}, {@code ofSize}, or {@code first}</li>
 *   <li>{@link io.parsley.CausalResult} &mdash; stamped on every record under the
 *       {@code parsley-causal-result} header: {@code SATISFIED} (delivered with its causal
 *       dependencies intact) or {@code EVICTED} (the buffer limit fired first, logged with the
 *       causal gap); read it with {@link io.parsley.CausalResult#fromRecord} to react in your own
 *       {@code process()}/{@code poll()}</li>
 *   <li>{@link io.parsley.CausalFrontierListener} &mdash; frontier-advance callback, the public way to
 *       observe causal progress out of a {@link io.parsley.CausalProcessorSupplier}</li>
 * </ul>
 */
package io.parsley;
