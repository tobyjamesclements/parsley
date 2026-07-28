/**
 * Causal delivery order for Kafka stream processing.
 *
 * <p>One package, two tiers. The public types are the entire supported surface:
 * {@link io.github.tobyjamesclements.parsley.CausalStage} /
 * {@link io.github.tobyjamesclements.parsley.CausalStreams} (the Kafka Streams runtime),
 * {@link io.github.tobyjamesclements.parsley.EdgeClock} and
 * {@link io.github.tobyjamesclements.parsley.CausalHeaders} (plain-client stamping and
 * inspection, with {@link io.github.tobyjamesclements.parsley.Clock} and
 * {@link io.github.tobyjamesclements.parsley.Channel} as their vocabulary), and
 * {@link io.github.tobyjamesclements.parsley.TopicIds} with
 * {@link io.github.tobyjamesclements.parsley.SendTracker} as the seams a
 * {@code TopologyTestDriver} test injects.
 *
 * <p>Everything else is package-private on purpose. The protocol core is only sound under a
 * host contract no API can enforce — per-channel offset order in, atomic commit of store,
 * offsets, and sends, position advances from the real consumer, partitioning before stamping —
 * and the one host that upholds it ships in this package. The deterministic simulator, the
 * protocol's primary verifier, reaches the internals from the test tree of this same package.
 */
package io.github.tobyjamesclements.parsley;
