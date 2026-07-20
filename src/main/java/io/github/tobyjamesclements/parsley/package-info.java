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
 *       io.github.tobyjamesclements.parsley.CausalClock} for the wire contract and the edge
 *       operations below for talking to a Parsley topology from plain Kafka clients.</li>
 *   <li><strong>Coordination-free joins.</strong> There is no membership, no epoch, and no join
 *       barrier: a new application simply starts consuming, its replay self-gates into causal
 *       delivery order through the ordinary hold-back queue, and its truthful stamps make its
 *       outputs correctly gated everywhere from the first emission. The removed {@code
 *       parsley.coordination.*} keys of earlier versions fail startup loudly if present.</li>
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
 *       {@code KafkaStreams} instance, and owns graceful causal drain on {@code close()}</li>
 * </ul>
 *
 * <h2>Edge operations</h2>
 * To talk to a Parsley topology from plain Kafka clients, stamp and propagate causal dependencies
 * directly with {@link io.github.tobyjamesclements.parsley.CausalClock}:
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalClock#using(java.util.Properties) using}
 *       &mdash; bind a resolver (topic name &rarr; stable Kafka UUID, resolved internally through the
 *       given Kafka client configuration) and start accumulating a consumer-side frontier</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalClock#observe(org.apache.kafka.clients.consumer.ConsumerRecord)
 *       observe} &mdash; fold a consumed record's dependencies and own position into the accumulator</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalClock#stamp(org.apache.kafka.clients.producer.ProducerRecord)
 *       stamp} &mdash; attach the accumulated dependencies to an outbound {@code ProducerRecord}</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalClock#merge(io.github.tobyjamesclements.parsley.CausalClock) merge} &mdash;
 *       combine dependency sets for a fan-in</li>
 * </ul>
 *
 * <h2>Internal protocol modules</h2>
 * Internally the implementation is organised as layered protocols, each presented in the module style
 * of Cachin–Guerraoui–Rodrigues (<em>Introduction to Reliable and Secure Distributed Programming</em>):
 * requests in, indications out, properties guaranteed. The channels module
 * ({@link io.github.tobyjamesclements.parsley.ParsleyChannels}) adapts Kafka topic-partitions into the
 * reliable FIFO channels classical causal broadcast assumes; the causal-broadcast module
 * ({@link io.github.tobyjamesclements.parsley.ParsleyCausalBroadcast}) is Birman–Schiper–Stephenson
 * causal delivery over those channels; the gossip module
 * ({@link io.github.tobyjamesclements.parsley.ParsleyGossip}) disseminates clock progress as null
 * messages so completeness keeps flowing through non-emitting paths. Two Parsley-wide deviations
 * from the textbook presentation apply to every module, stated once here:
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
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalClock} &mdash; the causal requirements stamped on a record
 *       (what the consumer must have observed before the record may be delivered). Its serialised size
 *       grows with the number of relevant topic-partitions and counts against Kafka's
 *       {@code message.max.bytes}. Topic-UUID resolution is an internal detail of {@code using}/
 *       {@code builder} &mdash; there is no separate public resolver type to construct or manage</li>
 * </ul>
 */
@NullMarked
package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.NullMarked;
