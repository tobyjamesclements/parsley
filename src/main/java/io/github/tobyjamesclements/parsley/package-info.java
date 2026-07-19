/**
 * Parsley: causal delivery order for Kafka Streams processors. A single package whose public surface is
 * a concise, topology-level API modelled on Kafka Streams; the low-level processor decorator that powers
 * it is package-private internal machinery.
 *
 * <h2>Three pillars</h2>
 * <ul>
 *   <li><strong>Causal broadcast.</strong> Every record carries the producer's causal dependencies in a
 *       header; a consumer holds a record in a changelog-backed buffer until its own frontier dominates
 *       those dependencies, then delivers it — so a topology of Kafka Streams processors sees causally
 *       related events across topics in the order they actually happened, not merely in per-partition
 *       order. The classic broadcast/receive/deliver vocabulary and where each lives in this package is
 *       spelled out in {@link io.github.tobyjamesclements.parsley.ParsleyCausalBroadcast}'s Javadoc. See {@link
 *       io.github.tobyjamesclements.parsley.CausalDependencies} for the wire contract and the edge
 *       operations below for talking to a Parsley topology from plain Kafka clients.</li>
 *   <li><strong>Topology epochs.</strong> A running, coordinated topology can evolve — a stage added,
 *       replaced, or reconfigured — without dragging pre-change history into causal time: {@link
 *       io.github.tobyjamesclements.parsley.CausalStreams#requestEpochTransition()} evolves every
 *       participating member through a leaderless, in-band epoch boundary. Optional and off by default;
 *       {@code parsley.coordination.epoch-events-topic} turns it on. Absent that key, a topology runs in
 *       epoch 0 indefinitely — no epoch-events log, no coordination thread.</li>
 *   <li><strong>Schema handling.</strong> A held record's key and value are (de)serialised with the
 *       {@code Serde} registered for its own <em>source</em> topic, never the buffer's internal changelog
 *       topic, so topic-scoped serdes — Avro plus Schema Registry {@code TopicNameStrategy} in particular
 *       — resolve the correct subject even for a record held across a schema change. A record that
 *       becomes undecodable while buffered (e.g. an incompatible registry change) fails the task fast
 *       rather than being silently dropped or forwarded on an unproven causal premise.</li>
 * </ul>
 *
 * <h2>Entry point</h2>
 * Three roles mirroring Kafka Streams — {@code StreamsBuilder} / {@code Topology} / {@code KafkaStreams}:
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalStreamsBuilder} &mdash; declare the one causal
 *       stage: {@code stream(...)} one or more source topics, {@code .process(supplier)} to bind them to a
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
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#using(java.util.Properties) using}
 *       &mdash; bind a resolver (topic name &rarr; stable Kafka UUID, resolved internally through the
 *       given Kafka client configuration) and start accumulating a consumer-side frontier</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#observe(org.apache.kafka.clients.consumer.ConsumerRecord)
 *       observe} &mdash; fold a consumed record's dependencies and own position into the accumulator</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#stamp(org.apache.kafka.clients.producer.ProducerRecord)
 *       stamp} &mdash; attach the accumulated dependencies to an outbound {@code ProducerRecord}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies#merge(io.github.tobyjamesclements.parsley.CausalDependencies) merge} &mdash;
 *       combine dependency sets for a fan-in</li>
 * </ul>
 *
 * <h2>Internal protocol modules</h2>
 * Internally the implementation is organised as layered protocols, each presented in the module style
 * of Cachin–Guerraoui–Rodrigues (<em>Introduction to Reliable and Secure Distributed Programming</em>):
 * requests in, indications out, properties guaranteed. The channels module
 * ({@link io.github.tobyjamesclements.parsley.ParsleyChannels}) adapts Kafka topic-partitions into the
 * reliable FIFO channels classical causal broadcast assumes. Two Parsley-wide deviations from the
 * textbook presentation apply to every module, stated once here:
 * <ul>
 *   <li><strong>Indications are pulled, not pushed.</strong> Deliveries come back as ordered return
 *       values rather than through an upcall, because Kafka Streams' threading is synchronous. Same
 *       semantics, pull style.</li>
 *   <li><strong>The sender's clock increment is performed by the broker.</strong> In
 *       Birman–Schiper–Stephenson the sender increments its own vector entry at send; here the
 *       increment is the broker's offset assignment, learned asynchronously from producer
 *       acknowledgements. This is why an outbound stamp cannot include the record's own
 *       coordinate.</li>
 * </ul>
 *
 * <h2>Key value types</h2>
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalDependencies} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered). Its serialised size
 *       grows with the number of relevant topic-partitions and counts against Kafka's
 *       {@code message.max.bytes}. Topic-UUID resolution is an internal detail of {@code using}/
 *       {@code builder} &mdash; there is no separate public resolver type to construct or manage</li>
 * </ul>
 */
@NullMarked
package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.NullMarked;
