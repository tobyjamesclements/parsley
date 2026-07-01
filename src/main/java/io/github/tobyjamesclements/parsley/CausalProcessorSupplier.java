package io.github.tobyjamesclements.parsley;

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
 *        .process(CausalProcessors.builder(user)
 *                                .addBufferStore("parsley", CausalBufferLimit.ofDuration(limit))
 *                                .addBuffers(List.of("prices", "orders"), Serdes.String(), orderSerde)
 *                                .build())
 *        .to("output-topic");
 * }</pre>
 *
 * <h2>The guarantee</h2>
 * Parsley guarantees causal delivery order subject to the conditions below, which hold across the
 * whole processor, not per-record. Parsley cannot verify most of them, so they are a contract on how
 * the topology is built; see the Streams integration preconditions in the documentation for the full
 * list, including the branching and topic-compaction constraints.
 *
 * <ol>
 *   <li><strong>Your key is your shard.</strong> Every causally-related topic is partitioned by the
 *       record key with a matching partition count, so each task owns the complete partition set for a
 *       causally-related group. The key is the unit of causal locality. An advanced user may partition
 *       by a coarser function of the key with a custom {@code StreamPartitioner} that reads the key,
 *       not the value. Parsley checks only that the input topics share a partition count, at startup,
 *       governed by {@code parsley.topology.validation}; the rest is not detected, and a misconfigured
 *       topology silently evaluates against an incomplete frontier.
 *   <li><strong>The key is preserved across the processor.</strong> Changing the key moves a record to
 *       a different shard and breaks co-partitioning downstream, so key-changing operations (a
 *       {@code groupBy}, or a join on a derived key) belong outside the causally-related segment.
 *   <li><strong>Closed effects.</strong> The processor's only side effects are reads/writes to its
 *       connected, changelogged state stores and {@code forward}. No global stores, no external I/O
 *       (no HTTP, no self-constructed producers). This is the boundary of soundness: Parsley owns
 *       every egress only if {@code forward} is the sole egress. It cannot be enforced in code — it
 *       is the contract boundary.
 * </ol>
 *
 * <p>When those conditions hold: every record B whose causal dependencies include record A is not
 * forwarded to {@code process()} until A has been processed. Every record reaches {@code process()}
 * exactly once — Parsley never drops or diverts a record. Records claiming no dependencies, or
 * carrying an undecodable header, are treated as vacuously satisfied and delivered immediately.
 * The exception is eviction — when the configured {@link CausalBufferLimit} fires before a held
 * record's dependencies are satisfied, the default ({@code parsley.buffer.eviction.failure.policy = fail})
 * fails the task fast rather than delivering the record out of causal order; setting the policy to
 * {@code continue} delivers it anyway, trading ordering for availability for that record. Either
 * outcome is signalled to any registered {@link CausalAudit} (via
 * {@link CausalAudit#recordEvictionLimitExceeded} or {@link CausalAudit#recordViolation}
 * respectively), logged, and counted by the buffer's violation metric.
 *
 * <p>Outgoing messages are stamped with the current frontier transparently as they are forwarded —
 * nothing extra is needed on egress, because Streams sinks propagate record headers to the produced
 * messages. When the delegate forwards nothing for a delivered input, Parsley emits a protocol
 * watermark carrying the same frontier, keyed with that input record's key so it routes to the
 * record's partition. Held records are persisted to a changelog-backed buffer store (serialised with
 * the serdes you supply, resolved per source topic) so they survive a restart.
 */
public interface CausalProcessorSupplier<KIn, VIn, KOut, VOut> extends ProcessorSupplier<KIn, VIn, KOut, VOut> {
}
