/**
 * The protocol, independent of any host.
 *
 * <p>This package names no host type. It opens no connection, reads no clock and touches
 * nothing outside its {@link io.github.tobyjamesclements.parsley.core.OrderingStore}, so the
 * same code runs under a deterministic simulator and under Kafka Streams. A test enforces
 * this by scanning the sources.
 *
 * <p>The safety rule is {@link io.github.tobyjamesclements.parsley.core.Deliverability#decide},
 * a pure function of the causes a message expressed, the channels a process receives, and how
 * far each has settled. {@link io.github.tobyjamesclements.parsley.core.ProcessEngine} holds
 * the state that function reads: a causal frontier
 * ({@link io.github.tobyjamesclements.parsley.core.Causes}), a per-channel hold-back buffer,
 * and the delivered causal past.
 *
 * <p>Where the guarantee cannot be upheld, operations throw
 * {@link io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException} and delivery
 * stops.
 */
package io.github.tobyjamesclements.parsley.core;
