package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.Closeable;
import java.time.Duration;

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
 * try (CausalConsumer<String, Order> consumer = CausalConsumers.<String, Order>builder(
 *         List.of("prices", "orders"),
 *         CausalBufferPolicy.forwardUnsafe(CausalBufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         Map.of(),
 *         streamsConfig).build()) {
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
}
