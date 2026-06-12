/**
 * The Kafka Streams integration: build topologies whose processors enforce causal ordering.
 *
 * <p>The public surface is {@link io.parsley.stream.CausalProcessorSupplier} (with its static
 * {@code create} factories) and the {@link io.parsley.stream.CausalStreams} helper. The
 * buffering engine, processor, and record envelope behind them are package-private
 * implementation details.
 */
package io.parsley.stream;
