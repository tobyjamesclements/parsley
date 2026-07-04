/**
 * Parsley: causal delivery order for Kafka Streams processors. A single package whose public surface is interfaces and
 * records; all implementations are package-private and obtained through factory methods.
 *
 * <h2>Entry point (interface with a static factory)</h2>
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalProcessorSupplier} &mdash; {@code CausalProcessors.builder(...).build()} wraps your own
 *       Kafka Streams {@code Processor} so its state access and {@code forward}s run behind the causal
 *       guarantee; drop it into {@code stream(...).process(...)}</li>
 * </ul>
 *
 * <h2>Edge operations</h2>
 * To talk to a Parsley topology from plain Kafka clients, stamp and propagate causal dependencies
 * directly with {@link io.github.tobyjamesclements.parsley.CausalDependencies}:
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#stamp(org.apache.kafka.clients.producer.ProducerRecord)
 *       stamp} &mdash; attach dependencies to an outbound {@code ProducerRecord}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#from(io.github.tobyjamesclements.parsley.CausalTopics, org.apache.kafka.clients.consumer.ConsumerRecord)
 *       from} &mdash; derive the dependencies of a record produced after consuming another (its
 *       carried dependencies plus its own position)</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#merge(io.github.tobyjamesclements.parsley.CausalDependencies) merge} &mdash;
 *       combine dependency sets for a fan-in</li>
 * </ul>
 *
 * <h2>Key value types</h2>
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalBuffer} &mdash; registers one causal source on the processor builder: a
 *       topic name paired with the serdes the buffer round-trips held records with (the topic's stable
 *       UUID is resolved from the broker automatically)</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered). Its serialised size
 *       grows with the number of relevant topic-partitions and counts against Kafka's
 *       {@code message.max.bytes}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalTopics} &mdash; resolves topic names to their stable Kafka UUIDs
 *       (through a caller-owned {@code Admin}), so {@link io.github.tobyjamesclements.parsley.CausalDependencies} can be built
 *       from topic names</li>
 * </ul>
 */
@NullMarked
package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.NullMarked;
