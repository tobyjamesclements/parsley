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
 *   <li>{@link io.parsley.CausalBuffer} &mdash; registers one causal source on a builder: a topic
 *       name paired with the serdes the buffer round-trips held records with (the topic's stable UUID
 *       is resolved from the broker automatically)</li>
 *   <li>{@link io.parsley.CausalDependencies} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered); build one from the
 *       {@link io.parsley.CausalTopic} identities you register, or read a consumed record's
 *       dependencies with {@link io.parsley.CausalDependencies#fromRecord fromRecord} when
 *       propagating across services. Its serialised size grows with the number of relevant
 *       topic-partitions and counts against Kafka's {@code message.max.bytes}</li>
 *   <li>{@link io.parsley.CausalTopic} &mdash; a topic's stable causal identity (name + Kafka UUID),
 *       used when declaring {@link io.parsley.CausalDependencies}</li>
 *   <li>{@link io.parsley.CausalBufferLimit} &mdash; how long to wait for a record's dependencies
 *       before forwarding it anyway: {@code ofDuration}, {@code ofSize}, or {@code first}</li>
 * </ul>
 */
package io.parsley;
