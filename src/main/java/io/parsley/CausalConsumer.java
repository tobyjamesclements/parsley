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
 *         CausalBufferLimit.ofDuration(Duration.ofSeconds(30)),
 *         Map.of(),
 *         streamsConfig)
 *         .addBuffer(CausalBuffer.of("prices", Serdes.String(), orderSerde))
 *         .addBuffer(CausalBuffer.of("orders", Serdes.String(), orderSerde))
 *         .build()) {
 *
 *     ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(200));
 * }
 * }</pre>
 * To propagate causal context when producing downstream, read the upstream record's dependencies
 * with {@link CausalDependencies#fromRecord(org.apache.kafka.clients.consumer.ConsumerRecord)} and
 * pass them to {@link CausalProducer#send}.
 *
 * <h2>Thread safety</h2>
 * Like Kafka's own {@code Consumer}, {@link #poll} is intended to be driven by a <strong>single
 * thread</strong>. Polling concurrently from several threads does not corrupt the consumer, but it
 * splits the causally-ordered record stream across those threads — so each thread sees only a
 * subset, defeating the ordering guarantee this consumer exists to provide. Unlike Kafka's consumer,
 * this is not detected or rejected; it is simply the caller's responsibility.
 *
 * <p>{@link #close()} is safe to call from a thread other than the poller, but
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
     * <h2>Delivery guarantee</h2>
     * Records are delivered <strong>at-least-once</strong>: admitted records are written to an
     * internal Kafka outbox topic before being returned here, so they survive a crash and will be
     * re-delivered on restart. For <strong>exactly-once</strong> delivery, set
     * {@code processing.guarantee=exactly_once_v2} in the {@code streamsConfig} passed to
     * {@link CausalConsumers}; the internal outbox consumer already reads with
     * {@code isolation.level=read_committed}, so no additional configuration is needed.
     *
     * @param timeout the maximum time to block waiting for records; must not be {@code null}
     * @return records ready for processing; never {@code null}, may be empty
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * Stops the consumer and releases all resources.
     */
    @Override
    void close();
}
