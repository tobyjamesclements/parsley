/**
 * Parsley: causal consistency for Kafka. A single package whose public surface is interfaces and
 * records; all implementations are package-private and obtained through factory methods.
 *
 * <h2>Entry point (interface with a static factory)</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalProcessorSupplier} &mdash; {@code CausalProcessors.builder(...).build()} wraps your own
 *       Kafka Streams {@code Processor} so its state access and {@code forward}s run behind the causal
 *       guarantee; drop it into {@code stream(...).process(...)}</li>
 * </ul>
 *
 * <h2>Edge operations</h2>
 * To talk to a Parsley topology from plain Kafka clients, stamp and propagate causal dependencies
 * directly with {@link io.parsley.CausalDependencies}:
 * <ul>
 *   <li>{@link io.parsley.CausalDependencies#stamp(org.apache.kafka.clients.producer.ProducerRecord)
 *       stamp} &mdash; attach dependencies to an outbound {@code ProducerRecord}</li>
 *   <li>{@link io.parsley.CausalDependencies#from(io.parsley.CausalTopics, org.apache.kafka.clients.consumer.ConsumerRecord)
 *       from} &mdash; derive the dependencies of a record produced after consuming another (its
 *       carried dependencies plus its own position)</li>
 *   <li>{@link io.parsley.CausalDependencies#merge(io.parsley.CausalDependencies) merge} &mdash;
 *       combine dependency sets for a fan-in</li>
 * </ul>
 *
 * <h2>Key value types</h2>
 * <ul>
 *   <li>{@link io.parsley.CausalBuffer} &mdash; registers one causal source on the processor builder: a
 *       topic name paired with the serdes the buffer round-trips held records with (the topic's stable
 *       UUID is resolved from the broker automatically)</li>
 *   <li>{@link io.parsley.CausalDependencies} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered). Its serialised size
 *       grows with the number of relevant topic-partitions and counts against Kafka's
 *       {@code message.max.bytes}</li>
 *   <li>{@link io.parsley.CausalTopics} &mdash; resolves topic names to their stable Kafka UUIDs
 *       (through a caller-owned {@code Admin}), so {@link io.parsley.CausalDependencies} can be built
 *       from topic names</li>
 *   <li>{@link io.parsley.CausalBufferLimit} &mdash; how long to wait for a record's dependencies
 *       before forwarding it anyway: {@code ofDuration}, {@code ofSize}, or {@code first}</li>
 * </ul>
 */
@NullMarked
package io.parsley;

import org.jspecify.annotations.NullMarked;
