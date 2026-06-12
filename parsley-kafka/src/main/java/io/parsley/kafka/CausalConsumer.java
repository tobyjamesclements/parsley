package io.parsley.kafka;

import io.parsley.BufferingPolicy;
import io.parsley.FenceToken;
import io.parsley.VectorClock;
import io.parsley.kafka.internal.DefaultCausalConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * A high-level Kafka consumer that delivers records in causal order.
 *
 * <p>A {@code CausalConsumer} wraps a Kafka Streams topology to provide causal ordering
 * guarantees over one or more topics. Records whose {@link VectorClock} dependencies are
 * not yet satisfied by the current frontier are held in an internal buffer. Once the frontier
 * catches up, buffered records are released and returned in subsequent {@link #poll} calls.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * try (CausalConsumer<String, Order> consumer = CausalConsumer.create(
 *         List.of("orders"),
 *         BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
 *         Map.of(),
 *         streamsConfig)) {
 *
 *     ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(200));
 *     FenceToken<KafkaVectorClock> token = consumer.fenceToken(); // encode causal progress
 * }
 * }</pre>
 *
 * <h2>Fence tokens</h2>
 * <p>After processing records, call {@link #fenceToken()} to obtain a {@link FenceToken}
 * encoding the current frontier. Pass this token to downstream services so they can wait
 * until they have caught up before processing causally dependent work.
 *
 * @param <K> the record key type
 * @param <V> the record value type
 */
public interface CausalConsumer<K, V> extends Closeable {

    /**
     * Polls for records whose causal dependencies are satisfied.
     *
     * <p>Records with unsatisfied dependencies remain buffered until the frontier catches up
     * or the configured {@link io.parsley.BufferLimit} fires.
     *
     * @param timeout the maximum time to block waiting for records; must not be {@code null}
     * @return records ready for processing; never {@code null}, may be empty
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * Returns the current causal frontier — the highest offset seen on each partition.
     *
     * @return the current {@link KafkaVectorClock} frontier; never {@code null}
     */
    KafkaVectorClock frontier();

    /**
     * Returns a {@link FenceToken} encoding the current causal frontier.
     *
     * <p>The returned token can be passed to downstream services to assert
     * "you must have caught up to this state before proceeding."
     *
     * @return a fence token representing the current frontier
     */
    FenceToken<KafkaVectorClock> fenceToken();

    /**
     * Stops the consumer and releases all resources.
     */
    @Override
    void close();

    /**
     * Creates a new {@code CausalConsumer} over the given topics.
     *
     * <p>{@link BufferingPolicy.DeadLetter DeadLetter} policies are not supported by this
     * facade — there is no parameter for a dead-letter sink — and are rejected with
     * {@link IllegalArgumentException}. To dead-letter evicted records, build a custom
     * topology with {@link CausalProcessorSupplier}'s 4-argument constructor instead.
     *
     * @param <K>            the record key type
     * @param <V>            the record value type
     * @param topics         the Kafka topics to subscribe to; must not be empty
     * @param policy         the buffering policy governing how unordered records are handled;
     *                       must not be a {@code DeadLetter} policy
     * @param consumerConfig additional consumer configuration (overrides any defaults derived
     *                       from {@code streamsConfig})
     * @param streamsConfig  Kafka Streams configuration ({@code StreamsConfig} properties);
     *                       must include at minimum {@code application.id} and
     *                       {@code bootstrap.servers}
     * @return a new, running {@code CausalConsumer}
     * @throws IllegalArgumentException if {@code policy} is a
     *                                  {@link BufferingPolicy.DeadLetter DeadLetter} policy
     */
    static <K, V> CausalConsumer<K, V> create(
            Collection<String> topics,
            BufferingPolicy policy,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {
        return new DefaultCausalConsumer<>(topics, policy, consumerConfig, streamsConfig);
    }
}
