/**
 * Parsley: causal delivery order for Kafka Streams processors. A single package whose public surface is
 * a concise, topology-level API modelled on Kafka Streams; the low-level processor decorator that powers
 * it is package-private internal machinery.
 *
 * <h2>Entry point</h2>
 * Three roles mirroring Kafka Streams — {@code StreamsBuilder} / {@code Topology} / {@code KafkaStreams}:
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalStreamsBuilder} &mdash; declare one or more causal
 *       stages: {@code stream(...)} one or more source topics, {@code .process(supplier)} to bind them to a
 *       causal-decorated processor, {@code .to(...)} to declare its sink(s); {@code .build()} produces a
 *       {@link io.github.tobyjamesclements.parsley.CausalTopology}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalTopology} &mdash; the built causal topology, ready
 *       for {@code new CausalStreams(topology, props)}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalStreams} &mdash; the runtime: wraps the underlying
 *       {@code KafkaStreams} instance, and owns graceful causal drain on {@code close()} and (when
 *       {@code parsley.coordination.epoch-events-topic} is configured) topology-epoch coordination</li>
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
