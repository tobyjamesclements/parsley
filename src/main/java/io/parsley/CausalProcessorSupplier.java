package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link ProcessorSupplier} that wraps an ordinary user {@code ProcessorSupplier} so it runs
 * behind Parsley's causal guarantee. Obtain one from the static {@link #create} factories and drop
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
 *        .process(CausalProcessorSupplier.create(user, CausalBufferingPolicy.deadLetter(limit, "parsley-dlq"), onViolation,
 *                                deadLetterSink, Serdes.String(), orderSerde))
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
 *       {@link CausalBufferingPolicy}'s behaviour under sustained lag. <strong>Strict</strong> policies
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

    /**
     * Decorates {@code userSupplier} for a {@link CausalBufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link CausalBufferingPolicy.Drop Drop} policy, using one serde pair for all input topics.
     *
     * @param userSupplier the user's processor supplier
     * @param policy       the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation  the rich violation callback
     * @param keySerde     the serde for buffered keys
     * @param valueSerde   the serde for buffered values
     * @param <KIn>        the input key type
     * @param <VIn>        the input value type
     * @param <KOut>       the forwarded key type
     * @param <VOut>       the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Serde<KIn> keySerde,
            Serde<VIn> valueSerde) {
        return create(userSupplier, policy, onViolation, topic -> keySerde, topic -> valueSerde);
    }

    /**
     * Decorates {@code userSupplier} for a {@link CausalBufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link CausalBufferingPolicy.Drop Drop} policy, resolving the buffer serde by source topic so input
     * topics with distinct (e.g. Avro {@code SpecificRecord} / Schema Registry) serdes round-trip
     * correctly.
     *
     * @param userSupplier      the user's processor supplier
     * @param policy            the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation       the rich violation callback
     * @param keySerdeByTopic   resolves the key serde for a given source topic
     * @param valueSerdeByTopic resolves the value serde for a given source topic
     * @param <KIn>             the input key type
     * @param <VIn>             the input value type
     * @param <KOut>            the forwarded key type
     * @param <VOut>            the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic) {
        return create(userSupplier, policy, onViolation, keySerdeByTopic, valueSerdeByTopic, "parsley");
    }

    /**
     * As {@link #create(ProcessorSupplier, CausalBufferingPolicy, CausalViolationHandler, Function, Function)}
     * but with an explicit {@code storeName} namespace, so several causal processors can share one
     * topology. The processor's frontier store is {@code storeName + "-frontier"} and its buffer
     * store is {@code storeName + "-buffer"}; Kafka Streams requires these to be unique within a
     * topology, and the names are persistent and changelog-backed, so keep {@code storeName} stable
     * across restarts.
     *
     * @param userSupplier      the user's processor supplier
     * @param policy            the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation       the rich violation callback
     * @param keySerdeByTopic   resolves the key serde for a given source topic
     * @param valueSerdeByTopic resolves the value serde for a given source topic
     * @param storeName         the state-store namespace for this processor's frontier and buffer
     * @param <KIn>             the input key type
     * @param <VIn>             the input value type
     * @param <KOut>            the forwarded key type
     * @param <VOut>            the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic,
            String storeName) {
        return create(userSupplier, policy, onViolation, keySerdeByTopic, valueSerdeByTopic, storeName,
                frontier -> {});
    }

    /**
     * As {@link #create(ProcessorSupplier, CausalBufferingPolicy, CausalViolationHandler, Function, Function, String)}
     * but with a {@link CausalFrontierListener} that observes every advance of this processor's causal
     * frontier (and the restored frontier at startup). Use it to track causal progress without
     * reaching into the processor's internal state stores — this is how {@link CausalConsumer}
     * surfaces {@link CausalConsumer#frontier()}.
     *
     * @param userSupplier      the user's processor supplier
     * @param policy            the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation       the rich violation callback
     * @param keySerdeByTopic   resolves the key serde for a given source topic
     * @param valueSerdeByTopic resolves the value serde for a given source topic
     * @param storeName         the state-store namespace for this processor's frontier and buffer
     * @param frontierListener  invoked with the new frontier after every advance, and once with the
     *                          restored frontier at startup; must be thread-safe (see
     *                          {@link CausalFrontierListener})
     * @param <KIn>             the input key type
     * @param <VIn>             the input value type
     * @param <KOut>            the forwarded key type
     * @param <VOut>            the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic,
            String storeName,
            CausalFrontierListener frontierListener) {
        if (policy instanceof CausalBufferingPolicy.DeadLetter) {
            throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use a create() overload with a sink");
        }
        return new ParsleyProcessorSupplier<>(
                userSupplier, policy, onViolation, null, keySerdeByTopic, valueSerdeByTopic,
                storeName + "-frontier", storeName + "-buffer", frontierListener);
    }

    /**
     * Decorates {@code userSupplier} for a {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy,
     * routing evicted records to {@code deadLetterSink}, using one serde pair for all input topics.
     *
     * @param userSupplier   the user's processor supplier
     * @param policy         the dead-letter buffering policy
     * @param onViolation    the rich violation callback
     * @param deadLetterSink the consumer that receives evicted records for dead-lettering
     * @param keySerde       the serde for buffered keys
     * @param valueSerde     the serde for buffered values
     * @param <KIn>          the input key type
     * @param <VIn>          the input value type
     * @param <KOut>         the forwarded key type
     * @param <VOut>         the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy.DeadLetter policy,
            CausalViolationHandler onViolation,
            Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
            Serde<KIn> keySerde,
            Serde<VIn> valueSerde) {
        return create(userSupplier, policy, onViolation, deadLetterSink,
                topic -> keySerde, topic -> valueSerde);
    }

    /**
     * Decorates {@code userSupplier} for a {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy,
     * routing evicted records to {@code deadLetterSink}, resolving the buffer serde by source topic.
     *
     * @param userSupplier      the user's processor supplier
     * @param policy            the dead-letter buffering policy
     * @param onViolation       the rich violation callback
     * @param deadLetterSink    the consumer that receives evicted records for dead-lettering
     * @param keySerdeByTopic   resolves the key serde for a given source topic
     * @param valueSerdeByTopic resolves the value serde for a given source topic
     * @param <KIn>             the input key type
     * @param <VIn>             the input value type
     * @param <KOut>            the forwarded key type
     * @param <VOut>            the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy.DeadLetter policy,
            CausalViolationHandler onViolation,
            Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic) {
        return create(userSupplier, policy, onViolation, deadLetterSink,
                keySerdeByTopic, valueSerdeByTopic, "parsley");
    }

    /**
     * As
     * {@link #create(ProcessorSupplier, CausalBufferingPolicy.DeadLetter, CausalViolationHandler, Consumer, Function, Function)}
     * but with an explicit {@code storeName} namespace (see the non-dead-letter named overload for
     * the naming rules), so several causal processors can share one topology.
     *
     * @param userSupplier      the user's processor supplier
     * @param policy            the dead-letter buffering policy
     * @param onViolation       the rich violation callback
     * @param deadLetterSink    the consumer that receives evicted records for dead-lettering
     * @param keySerdeByTopic   resolves the key serde for a given source topic
     * @param valueSerdeByTopic resolves the value serde for a given source topic
     * @param storeName         the state-store namespace for this processor's frontier and buffer
     * @param <KIn>             the input key type
     * @param <VIn>             the input value type
     * @param <KOut>            the forwarded key type
     * @param <VOut>            the forwarded value type
     * @return a decorated supplier ready for {@code stream(...).process(...)}
     */
    static <KIn, VIn, KOut, VOut> CausalProcessorSupplier<KIn, VIn, KOut, VOut> create(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            CausalBufferingPolicy.DeadLetter policy,
            CausalViolationHandler onViolation,
            Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic,
            String storeName) {
        return new ParsleyProcessorSupplier<>(
                userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic,
                storeName + "-frontier", storeName + "-buffer");
    }
}
