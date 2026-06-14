/**
 * The high-level consuming side: {@link io.parsley.consumer.CausalConsumer} delivers records in
 * causal order over a {@code poll()} API. Its implementation is a package-private decorator that
 * builds an internal Kafka Streams pipeline with {@link io.parsley.stream.Parsley#causal}.
 */
package io.parsley.consumer;
