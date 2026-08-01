package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * Captures the main consumer's post-poll view per stream thread, holding for each assigned
 * partition the consumer position and the highest record offset any poll has returned.
 *
 * <p>The position advances past transaction markers and aborted batches even when a poll
 * returns no records. That advance is the protocol's liveness signal, taken by
 * {@link DeliveryProtocol#positionAdvance}, and {@code position()} read on the polling thread
 * is its only sound source.
 *
 * <p>A post-poll position also runs ahead of returned records the task has buffered but not
 * yet processed, where {@code positionAdvance} asserts the opposite, that everything below a
 * reported position is fetched-and-fed or consumer-skipped. The captured last-returned offset
 * is what lets the adapter honour that contract. A position is reported only once every
 * returned record at or below it has been fed through the protocol. Reported early, the
 * frontier would jump the buffered records and they would be dropped as replays.
 *
 * <p>The processor adapter reads the map from the same stream thread that polls, so a plain
 * thread-local suffices.
 */
final class Positions {

    /**
     * One partition's post-poll view.
     *
     * @param position the consumer position, below which everything is fetched or skipped
     * @param lastReturned the highest record offset polls have returned, or {@code -1} until a
     *         poll returns one
     */
    record PollView(long position, long lastReturned) {}

    private static final ThreadLocal<Map<TopicPartition, PollView>> POSITIONS =
            ThreadLocal.withInitial(HashMap::new);

    private Positions() {}

    /**
     * Returns this stream thread's captured views. Call from the thread that polls.
     *
     * @return the live map, keyed by assigned partition, updated after each poll
     */
    static Map<TopicPartition, PollView> forCurrentThread() {
        return POSITIONS.get();
    }

    /**
     * Wraps the default client supplier so main consumers record their view after each poll.
     *
     * <p>Only the main consumer is wrapped. The restore and global consumers are passed
     * through, since neither feeds the protocol.
     *
     * @return the wrapping supplier, to hand to Kafka Streams at construction
     */
    static KafkaClientSupplier capturingClientSupplier() {
        DefaultKafkaClientSupplier delegate = new DefaultKafkaClientSupplier();
        return new KafkaClientSupplier() {
            @Override
            public org.apache.kafka.clients.admin.Admin getAdmin(Map<String, Object> config) {
                return delegate.getAdmin(config);
            }

            @Override
            public org.apache.kafka.clients.producer.Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
                return delegate.getProducer(config);
            }

            @Override
            public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
                return capturing(delegate.getConsumer(config));
            }

            @Override
            public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
                return delegate.getRestoreConsumer(config);
            }

            @Override
            public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
                return delegate.getGlobalConsumer(config);
            }
        };
    }

    /**
     * Wraps one consumer in a proxy that records each assigned partition's view after a poll.
     *
     * <p>Every other call passes straight through, with the target's own exception rethrown
     * rather than the reflective wrapper's.
     *
     * @param inner the consumer to wrap
     * @return the wrapping proxy, capturing on the thread that calls it
     */
    @SuppressWarnings("unchecked")
    static Consumer<byte[], byte[]> capturing(Consumer<byte[], byte[]> inner) {
        InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
            Object result;
            try {
                result = method.invoke(inner, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
            if (method.getName().equals("poll") && result instanceof ConsumerRecords<?, ?> records) {
                Map<TopicPartition, PollView> map = POSITIONS.get();
                for (TopicPartition tp : inner.assignment()) {
                    PollView prior = map.get(tp);
                    long last = prior == null ? -1 : prior.lastReturned();
                    var batch = records.records(tp);
                    if (!batch.isEmpty()) {
                        // Per-partition batches are in offset order: the last one is the max.
                        last = Math.max(last, batch.get(batch.size() - 1).offset());
                    }
                    map.put(tp, new PollView(inner.position(tp), last));
                }
            }
            return result;
        };
        return (Consumer<byte[], byte[]>) Proxy.newProxyInstance(
                Positions.class.getClassLoader(), new Class<?>[] {Consumer.class}, handler);
    }
}
