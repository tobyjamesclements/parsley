package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * A high-level Kafka consumer that delivers records in causal order.
 *
 * <p>A {@code CausalConsumer} wraps a Kafka Streams topology to provide causal ordering across
 * one or more topics. Records whose {@link CausalDependencies} dependencies are not yet satisfied by
 * the current frontier are held in an internal buffer; once the frontier catches up, buffered
 * records are released and returned in subsequent {@link #poll} calls.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (CausalConsumer<String, Order> consumer = CausalConsumer.create(
 *         List.of("prices", "orders"),
 *         CausalBufferingPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         Map.of(),
 *         streamsConfig)) {
 *
 *     ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(200));
 *     CausalDependencies frontier = consumer.frontier(); // pass to a CausalProducer when producing
 * }
 * }</pre>
 *
 * <h2>Thread safety</h2>
 * Like Kafka's own {@code Consumer}, {@link #poll} is intended to be driven by a <strong>single
 * thread</strong>. Polling concurrently from several threads does not corrupt the consumer, but it
 * splits the causally-ordered record stream across those threads — so each thread sees only a
 * subset, defeating the ordering guarantee this consumer exists to provide. Unlike Kafka's consumer,
 * this is not detected or rejected; it is simply the caller's responsibility.
 *
 * <p>{@link #frontier()} is safe to call from any thread, concurrently with {@code poll} and with the
 * internal stream threads. {@link #close()} is safe to call from a thread other than the poller, but
 * it does <em>not</em> interrupt a {@code poll} already blocked waiting for records — there is no
 * {@code wakeup()}; the in-flight {@code poll} simply returns empty once its timeout elapses.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalConsumer<K, V> extends Closeable {

    /**
     * Polls for records whose causal dependencies are satisfied.
     *
     * <p>Records with unsatisfied dependencies remain buffered until the frontier catches up
     * or the configured {@link io.parsley.CausalBufferLimit} fires.
     *
     * @param timeout the maximum time to block waiting for records; must not be {@code null}
     * @return records ready for processing; never {@code null}, may be empty
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * Returns the current causal frontier — the highest offset seen on each partition, i.e.
     * everything this consumer has processed so far.
     *
     * <p>Pass this clock to {@link CausalProducer#send} to propagate the
     * consumer's causal position onto records it produces, or serialise it to hand downstream as a
     * causal token (see {@link CausalDependencies} for the cross-service propagation pattern). To instead
     * read the causal context of <em>one specific</em> consumed message — the upstream producer's
     * clock — use {@link CausalDependencies#fromRecord(org.apache.kafka.clients.consumer.ConsumerRecord)}.
     *
     * @return the current {@link CausalDependencies} frontier; never {@code null}
     */
    CausalDependencies frontier();

    /**
     * Stops the consumer and releases all resources.
     */
    @Override
    void close();

    /**
     * Creates a new, running {@code CausalConsumer}, defaulting the violation handler to
     * {@link CausalViolationHandler#noop()} and the state-store namespace to {@code "parsley"}.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy; must not be a {@code DeadLetter} policy
     * @param consumerConfig additional consumer configuration (overrides defaults derived from
     *                       {@code streamsConfig})
     * @param streamsConfig  Kafka Streams configuration; must include at minimum
     *                       {@code application.id} and {@code bootstrap.servers}
     * @return a new, running {@code CausalConsumer}
     * @throws IllegalArgumentException if {@code policy} is a
     *                                  {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy
     */
    static <K, V> CausalConsumer<K, V> create(
            Collection<String> topics,
            CausalBufferingPolicy policy,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return create(topics, policy, CausalViolationHandler.noop(), consumerConfig, streamsConfig);
    }

    /**
     * Creates a new, running {@code CausalConsumer} with an explicit violation handler, defaulting
     * the state-store namespace to {@code "parsley"}.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation    the callback invoked when a record cannot be delivered in causal order
     * @param consumerConfig additional consumer configuration
     * @param streamsConfig  Kafka Streams configuration ({@code application.id} + {@code bootstrap.servers})
     * @return a new, running {@code CausalConsumer}
     * @throws IllegalArgumentException if {@code policy} is a {@code DeadLetter} policy
     */
    static <K, V> CausalConsumer<K, V> create(
            Collection<String> topics,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return create(topics, policy, onViolation, consumerConfig, streamsConfig, "parsley");
    }

    /**
     * Creates a new, running {@code CausalConsumer} with full control over the violation handler and
     * the state-store namespace.
     *
     * <p>{@link CausalBufferingPolicy.DeadLetter DeadLetter} policies are not supported by this facade —
     * there is no parameter for a dead-letter sink — and are rejected. To dead-letter evicted
     * records, build a custom topology with {@link CausalProcessorSupplier#create}'s dead-letter overload
     * instead.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy; must not be a {@code DeadLetter} policy
     * @param onViolation    the callback invoked when a record cannot be delivered in causal order
     * @param consumerConfig additional consumer configuration
     * @param streamsConfig  Kafka Streams configuration ({@code application.id} + {@code bootstrap.servers})
     * @param storeName      the state-store namespace; the frontier store is
     *                       {@code storeName + "-frontier"} and the buffer store
     *                       {@code storeName + "-buffer"} (these name the backing changelog topics,
     *                       so keep {@code storeName} stable across restarts)
     * @return a new, running {@code CausalConsumer}
     * @throws IllegalArgumentException if {@code policy} is a
     *                                  {@link CausalBufferingPolicy.DeadLetter DeadLetter} policy
     */
    static <K, V> CausalConsumer<K, V> create(
            Collection<String> topics,
            CausalBufferingPolicy policy,
            CausalViolationHandler onViolation,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig,
            String storeName) {
        return new ParsleyConsumer<>(topics, policy, onViolation, consumerConfig, streamsConfig, storeName);
    }
}
