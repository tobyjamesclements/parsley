/**
 * Causal delivery order for Kafka stream processing.
 *
 * <p>One package, two tiers. The public types are the entire supported surface:
 * {@link io.github.tobyjamesclements.parsley.CausalStage} /
 * {@link io.github.tobyjamesclements.parsley.CausalStreams} (the Kafka Streams runtime),
 * {@link io.github.tobyjamesclements.parsley.Clock} (plain-client stamping),
 * {@link io.github.tobyjamesclements.parsley.CausalHeaders} (header names and observability
 * readers), and {@link io.github.tobyjamesclements.parsley.CausalStage#testTopology()} for
 * broker-less {@code TopologyTestDriver} tests — no seam is injectable from outside, and no
 * protocol vocabulary (the vector clock, channels, the node) escapes.
 *
 * <p>Everything else is package-private on purpose. The protocol core is only sound under a
 * host contract no API can enforce — per-channel offset order in, atomic commit of store,
 * offsets, and sends, position advances from the real consumer, partitioning before stamping —
 * and the one host that upholds it ships in this package. The deterministic simulator, the
 * protocol's primary verifier, reaches the internals from the test tree of this same package.
 */
package io.github.tobyjamesclements.parsley;
