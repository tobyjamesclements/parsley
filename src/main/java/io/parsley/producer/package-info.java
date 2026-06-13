/**
 * The producing side: {@link io.parsley.producer.CausalProducer} stamps a causal vector clock
 * onto every record it sends. Its implementation is a package-private decorator over a plain
 * Kafka {@link org.apache.kafka.clients.producer.Producer}.
 */
package io.parsley.producer;
