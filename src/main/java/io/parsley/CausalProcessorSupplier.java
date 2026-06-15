package io.parsley;

import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * A {@link ProcessorSupplier} that wraps an ordinary user {@code ProcessorSupplier} so it runs
 * behind Parsley's causal guarantee. Obtain one from {@link CausalProcessors} and drop
 * it into a topology with {@code stream(...).process(...)}.
 *
 * <p>Write a normal {@link org.apache.kafka.streams.processor.api.Processor} — with arbitrary
 * transformation, connected state-store access, and {@code forward} — declare its state stores in
 * your own supplier as usual, then drop the decorated supplier into a topology:
 *
 * <pre>{@code
 * ProcessorSupplier<String, Order, String, Enriched> user = new ProcessorSupplier<>() {
 *     public Processor<String, Order, String, Enriched> get() { return new EnrichOrder(); }
 *     public Set<StoreBuilder<?>> stores() { return Set.of(pricesStateBuilder); }
 * };
 *
 * builder.stream(List.of("prices", "orders"), Consumed.with(Serdes.String(), orderSerde))
 *        .process(CausalProcessors.builder(user, CausalBufferPolicy.deadLetter(limit, "parsley-dlq"))
 *                                .serdes(Serdes.String(), orderSerde).onViolation(onViolation)
 *                                .deadLetterSink(deadLetterSink).build())
 *        .to("output-topic");
 * }</pre>
 *
 * <h2>The guarantee</h2>
 * Within the user's {@code process()}, every state read reflects all causally-prior writes, every
 * state write and every {@code forward} is a causally-ordered, clock-stamped event —
 * <strong>provided</strong> three preconditions hold:
 *
 * <ol>
 *   <li><strong>Closed effects.</strong> The processor's only side effects are reads/writes to its
 *       connected, changelogged state stores and {@code forward}. No global stores, no external I/O
 *       (no HTTP, no self-constructed producers). This is the boundary of soundness: Parsley owns
 *       every egress only if {@code forward} is the sole egress. It cannot be enforced in code — it
 *       is the contract boundary.
 *   <li><strong>Co-partitioning.</strong> All causally-related input and output topics are
 *       co-partitioned so each instance owns the complete partition set for the causally-related
 *       events. Parsley does not detect or enforce this (consistent with existing library
 *       behaviour) — a misconfigured topology silently evaluates against an incomplete frontier.
 *   <li><strong>Accepted buffering policy.</strong> The user accepts the chosen
 *       {@link CausalBufferPolicy}'s behaviour under sustained lag. <strong>Strict</strong> policies
 *       ({@code deadLetter}, {@code drop}) divert un-satisfiable records away from {@code process()}
 *       — the guarantee holds unconditionally for every record that reaches {@code process()}, at
 *       the cost of delivery. The <strong>lenient</strong> policy ({@code forwardUnsafe}) preserves
 *       delivery by admitting un-satisfied records, which suspends the guarantee for exactly those
 *       records; they are flagged via the {@link CausalViolationHandler}.
 * </ol>
 *
 * <p>Outgoing messages are stamped with the current frontier transparently as they are forwarded —
 * no {@code CausalProducer} is needed on egress, because Streams sinks propagate record headers to
 * the produced messages. Held records are persisted to a changelog-backed buffer store (serialised
 * with the serdes you supply, resolved per source topic) so they survive a restart.
 */
public interface CausalProcessorSupplier<KIn, VIn, KOut, VOut> extends ProcessorSupplier<KIn, VIn, KOut, VOut> {
}
