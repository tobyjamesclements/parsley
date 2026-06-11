/**
 * Kafka integration for causal ordering: a high-level consumer, a causal producer,
 * a Kafka Streams processor, and {@link io.parsley.kafka.KafkaVectorClock} — the Kafka-specific
 * implementation of {@link io.parsley.VectorClock}.
 *
 * <h2>High-level consumer</h2>
 * <p>{@link io.parsley.kafka.CausalConsumer} wraps a Kafka Streams topology and exposes a
 * familiar {@code poll(Duration)} interface. Only records whose causal dependencies have been
 * satisfied by the current frontier are returned:
 * <pre>{@code
 * CausalConsumer<String, Order> consumer = CausalConsumer.create(
 *     List.of("orders"),
 *     BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *     consumerConfig, streamsConfig
 * );
 * List<ConsumerRecord<String, Order>> ready = consumer.poll(Duration.ofMillis(100));
 * }</pre>
 *
 * <h2>Causal producer</h2>
 * <p>{@link io.parsley.kafka.CausalProducer} stamps every outgoing record with a
 * {@link io.parsley.FenceToken}, so downstream consumers can enforce causal ordering
 * end-to-end across service boundaries.
 *
 * <h2>Streams topology integration</h2>
 * <p>{@link io.parsley.kafka.CausalProcessorSupplier} plugs into an existing
 * {@code Topology} via {@link io.parsley.kafka.CausalStreams} helper methods, adding
 * causal buffering at any point in the processing graph.
 */
package io.parsley.kafka;
