/**
 * Parsley provides causal delivery order for Kafka Streams processors. Every record carries its
 * producer's causal dependencies in a header, and a consumer holds a record in a changelog-backed
 * buffer until its own frontier dominates those dependencies, so a topology sees causally related
 * events across topics in the order they happened rather than merely in per-partition order. The
 * public surface is a single topology-level API modelled on Kafka Streams; everything below it is
 * package-private machinery. Parsley has no configuration keys; startup fails if any
 * {@code parsley.*} key is present.
 *
 * <h2>Entry point</h2>
 * Three roles mirroring Kafka Streams' {@code StreamsBuilder} / {@code Topology} /
 * {@code KafkaStreams}:
 * <ul>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalStreamsBuilder} declares the one causal
 *       stage: {@code stream(...)} source topics, {@code .process(supplier)}, {@code .to(...)}
 *       sink(s), and {@code .build()} to a
 *       {@link io.github.tobyjamesclements.parsley.CausalTopology}.</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalTopology} is the built topology, ready for
 *       {@code new CausalStreams(topology, props)}.</li>
 *   <li>{@link io.github.tobyjamesclements.parsley.CausalStreams} is the runtime: it wraps the
 *       underlying {@code KafkaStreams} instance and owns graceful causal drain on {@code close()}.</li>
 * </ul>
 *
 * <h2>Edge operations</h2>
 * To talk to a Parsley topology from a plain Kafka client, stamp and read causal dependencies with
 * {@link io.github.tobyjamesclements.parsley.CausalClock}:
 * {@link io.github.tobyjamesclements.parsley.CausalClock#using(java.util.Properties) using} binds a
 * resolver and starts a consumer-side frontier,
 * {@link io.github.tobyjamesclements.parsley.CausalClock#observe(org.apache.kafka.clients.consumer.ConsumerRecord)
 * observe} folds in a consumed record,
 * {@link io.github.tobyjamesclements.parsley.CausalClock#merge(io.github.tobyjamesclements.parsley.CausalClock)
 * merge} combines dependency sets for a fan-in, and
 * {@link io.github.tobyjamesclements.parsley.CausalClock#stamp(org.apache.kafka.clients.producer.ProducerRecord)
 * stamp} attaches the result to an outbound record.
 *
 * <h2>Naming convention</h2>
 * Every public type is named {@code Causal*}. The one exception is
 * {@link io.github.tobyjamesclements.parsley.ParsleyOwnOutputInterceptor}, which is public only
 * because Kafka instantiates producer interceptors reflectively and is not public API.
 * Package-private machinery is named {@code Parsley*}, except implementations of a {@code Parsley*}
 * seam, which are named for their backing ({@code KafkaTopics} implements {@code ParsleyTopics};
 * the {@code StoreBacked*} stores implement their {@code Parsley*} interfaces over a Kafka Streams
 * {@code KeyValueStore}).
 */
@NullMarked
package io.github.tobyjamesclements.parsley;

import org.jspecify.annotations.NullMarked;
