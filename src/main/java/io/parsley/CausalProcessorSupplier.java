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
 *        .process(CausalProcessors.builder(user, CausalBufferLimit.ofDuration(limit))
 *                                .addBuffers(List.of("prices", "orders"), Serdes.String(), orderSerde)
 *                                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <h2>The guarantee</h2>
 * Every record reaches the user's {@code process()} exactly once — Parsley never drops or
 * diverts a record. Within {@code process()}, every state read reflects all causally-prior
 * writes, and every state write and {@code forward} is a causally-ordered, dependency-stamped
 * event, in the common case: the record's dependencies were observed before delivery, whether
 * immediately or after a wait, including trivially for records claiming none. The exception is
 * eviction — when the configured {@link CausalBufferLimit} fires before a held record's
 * dependencies are satisfied, the record is delivered anyway (out of causal order), suspending the
 * guarantee for that one record. Evictions are not signalled per-record; they are logged with the
 * causal gap and counted by the buffer's violation metric.
 *
 * <p>The guarantee further depends on two preconditions that hold across the whole processor,
 * not per-record:
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
 * </ol>
 *
 * <p>Outgoing messages are stamped with the current frontier transparently as they are forwarded —
 * no {@code CausalProducer} is needed on egress, because Streams sinks propagate record headers to
 * the produced messages. Held records are persisted to a changelog-backed buffer store (serialised
 * with the serdes you supply, resolved per source topic) so they survive a restart.
 */
public interface CausalProcessorSupplier<KIn, VIn, KOut, VOut> extends ProcessorSupplier<KIn, VIn, KOut, VOut> {
}
