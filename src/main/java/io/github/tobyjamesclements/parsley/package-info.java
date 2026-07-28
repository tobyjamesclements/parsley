/**
 * Causal delivery order for Kafka stream processing.
 *
 * <p>One package, two tiers, shaped as a functional core with imperative edges.
 *
 * <p>The functional core is the user's logic vocabulary, free of Kafka types:
 * {@link io.github.tobyjamesclements.parsley.Topic} /
 * {@link io.github.tobyjamesclements.parsley.Codec} /
 * {@link io.github.tobyjamesclements.parsley.Message} /
 * {@link io.github.tobyjamesclements.parsley.Emission} /
 * {@link io.github.tobyjamesclements.parsley.Handler} /
 * {@link io.github.tobyjamesclements.parsley.Fold} /
 * {@link io.github.tobyjamesclements.parsley.Step}. Logic is a pure function from a delivered
 * message to emission values (and, for folds, the next per-key state), testable with plain
 * equality and no runtime.
 *
 * <p>The imperative edges run it: {@link io.github.tobyjamesclements.parsley.Stage} wires
 * logic to topics, {@link io.github.tobyjamesclements.parsley.Parsley} composes stages and
 * is the front door — {@code testTopology()} for broker-less {@code TopologyTestDriver}
 * tests, {@code streams(props)} for the
 * {@link io.github.tobyjamesclements.parsley.CausalStreams} runtime (a curated allowlist
 * over Kafka Streams with no escape hatch). Plain producers and consumers at the
 * application's borders stamp and observe through
 * {@link io.github.tobyjamesclements.parsley.CausalClock} and
 * {@link io.github.tobyjamesclements.parsley.CausalHeaders}.
 *
 * <p>Everything else is package-private on purpose. The protocol core is only sound under a
 * host contract no API can enforce — per-channel offset order in, atomic commit of store,
 * offsets, and sends, position advances from the real consumer, partitioning before stamping —
 * and the one host that upholds it ships in this package. The deterministic simulator, the
 * protocol's primary verifier, reaches the internals from the test tree of this same package.
 */
package io.github.tobyjamesclements.parsley;
