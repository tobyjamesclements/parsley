package io.parsley.stream;

import io.parsley.BufferingPolicy;
import io.parsley.ViolationHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Entry point for wrapping an ordinary Kafka Streams {@link ProcessorSupplier} so it runs behind
 * Parsley's causal guarantee.
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
 *        .process(Parsley.causal(user, BufferingPolicy.deadLetter(limit, "parsley-dlq"), onViolation,
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
 *       {@link BufferingPolicy}'s behaviour under sustained lag. <strong>Strict</strong> policies
 *       ({@code deadLetter}, {@code drop}) divert un-satisfiable records away from {@code process()}
 *       — the guarantee holds unconditionally for every record that reaches {@code process()}, at
 *       the cost of delivery. The <strong>lenient</strong> policy ({@code forwardUnsafe}) preserves
 *       delivery by admitting un-satisfied records, which suspends the guarantee for exactly those
 *       records; they are flagged via the {@link ViolationHandler}.
 * </ol>
 *
 * <p>Outgoing messages are stamped with the current frontier transparently as they are forwarded —
 * no {@code CausalProducer} is needed on egress, because Streams sinks propagate record headers to
 * the produced messages. Held records are persisted to a changelog-backed buffer store (serialised
 * with the serdes you supply, resolved per source topic) so they survive a restart.
 */
public final class Parsley {

    private Parsley() {}

    /**
     * Decorates {@code userSupplier} for a {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link BufferingPolicy.Drop Drop} policy, using one serde pair for all input topics.
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
    public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> causal(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            BufferingPolicy policy,
            ViolationHandler onViolation,
            Serde<KIn> keySerde,
            Serde<VIn> valueSerde) {
        return causal(userSupplier, policy, onViolation, topic -> keySerde, topic -> valueSerde);
    }

    /**
     * Decorates {@code userSupplier} for a {@link BufferingPolicy.ForwardUnsafe ForwardUnsafe} or
     * {@link BufferingPolicy.Drop Drop} policy, resolving the buffer serde by source topic so input
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
    public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> causal(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            BufferingPolicy policy,
            ViolationHandler onViolation,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic) {
        if (policy instanceof BufferingPolicy.DeadLetter) {
            throw new IllegalArgumentException(
                    "DeadLetter policy requires a dead-letter sink — use a causal() overload with a sink");
        }
        return new DecoratingCausalProcessorSupplier<>(
                userSupplier, policy, onViolation, null, keySerdeByTopic, valueSerdeByTopic);
    }

    /**
     * Decorates {@code userSupplier} for a {@link BufferingPolicy.DeadLetter DeadLetter} policy,
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
    public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> causal(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            BufferingPolicy.DeadLetter policy,
            ViolationHandler onViolation,
            Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
            Serde<KIn> keySerde,
            Serde<VIn> valueSerde) {
        return causal(userSupplier, policy, onViolation, deadLetterSink,
                topic -> keySerde, topic -> valueSerde);
    }

    /**
     * Decorates {@code userSupplier} for a {@link BufferingPolicy.DeadLetter DeadLetter} policy,
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
    public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> causal(
            ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
            BufferingPolicy.DeadLetter policy,
            ViolationHandler onViolation,
            Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
            Function<String, Serde<KIn>> keySerdeByTopic,
            Function<String, Serde<VIn>> valueSerdeByTopic) {
        return new DecoratingCausalProcessorSupplier<>(
                userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic);
    }
}
