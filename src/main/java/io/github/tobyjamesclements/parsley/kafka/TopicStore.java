package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import io.github.tobyjamesclements.parsley.core.OrderingStore;

/**
 * A byte-level key-value store materialised in memory from one partition of a compacted
 * topic, and written back through the task's transactional producer (D114).
 *
 * <p>Reads are served from memory. A write lands in memory at once and is staged; the
 * staged writes — the latest value per key, so a key written many times in one commit
 * interval costs one record — are sent inside the open transaction when the runtime
 * commits, ahead of the offsets, so the state a step persists commits atomically with the
 * positions it consumed and the messages it sent. On abort the task is discarded and
 * rebuilt from the topic, so memory never outlives what was committed.
 *
 * <p>Serves both as the engine's {@link OrderingStore} and as an application store behind
 * the {@link DeliverySeam}.
 */
final class TopicStore implements OrderingStore, DeliverySeam.ByteStore {
    private final String topic;
    private final int partition;
    private final Producer<byte[], byte[]> producer;
    private final Runnable onWrite;
    private final TreeMap<byte[], byte[]> latest = new TreeMap<>(Arrays::compareUnsigned);
    private final TreeMap<byte[], byte[]> staged = new TreeMap<>(Arrays::compareUnsigned);

    /**
     * @param topic     the compacted topic this store's partition lives in
     * @param partition the partition
     * @param restored  the latest committed value per key, tombstones as {@code null}
     * @param producer  the transactional producer the staged writes go through
     * @param onWrite   invoked before the first write of a step, so the runtime can open
     *                  the transaction the write will commit in
     */
    TopicStore(String topic, int partition, Map<byte[], byte[]> restored, Producer<byte[], byte[]> producer,
               Runnable onWrite) {
        this.topic = topic;
        this.partition = partition;
        this.producer = producer;
        this.onWrite = onWrite;
        restored.forEach((key, value) -> {
            if (value != null) {
                latest.put(key, value);
            }
        });
    }

    @Override
    public byte[] get(byte[] key) {
        return latest.get(key);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        onWrite.run();
        latest.put(key, value);
        staged.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
        onWrite.run();
        latest.remove(key);
        staged.put(key, null);
    }

    @Override
    public void scanPrefix(byte[] prefix, EntryConsumer consumer) {
        for (var entry : latest.tailMap(prefix, true).entrySet()) {
            byte[] key = entry.getKey();
            if (key.length < prefix.length
                    || Arrays.compareUnsigned(key, 0, prefix.length, prefix, 0, prefix.length) != 0) {
                return;
            }
            consumer.accept(key, entry.getValue());
        }
    }

    /** Sends every staged write into the open transaction. */
    void flush() {
        for (var entry : staged.entrySet()) {
            producer.send(new ProducerRecord<>(topic, partition, entry.getKey(), entry.getValue()));
        }
        staged.clear();
    }

    /** @return whether any write is staged for the open transaction */
    boolean dirty() {
        return !staged.isEmpty();
    }

    /** @return the latest value per key, for tests */
    Map<byte[], byte[]> snapshot() {
        return new TreeMap<>(latest);
    }
}
