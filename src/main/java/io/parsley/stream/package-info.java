/**
 * The Kafka Streams integration: build topologies whose processors enforce causal ordering.
 *
 * <p>The public surface is {@link io.parsley.stream.Parsley} (with its static {@code causal}
 * factories): wrap your own {@code Processor} so its state access and {@code forward}s run behind the
 * causal guarantee. The buffering engine, decorating processor, stamping context, and record
 * envelope behind it are package-private implementation details.
 */
package io.parsley.stream;
