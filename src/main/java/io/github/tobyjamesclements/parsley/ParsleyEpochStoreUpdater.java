package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * The state-update processor for the epoch global store: mirrors every record from the compacted
 * epoch topic into the store, keyed by the raw {@code (topicId, partition)} bytes. A null value is a
 * compaction tombstone (a coordinate's bound retracted), applied as a delete. Kafka Streams runs
 * this on its own global thread, one per instance, decoupled from the task threads that gate records.
 *
 * <p>Forwards nothing: a global store's update processor is a sink for the topic, not a stage in the
 * business topology.
 */
final class ParsleyEpochStoreUpdater implements Processor<byte[], byte[], Void, Void> {

    private final String storeName;
    private KeyValueStore<byte[], byte[]> store;

    ParsleyEpochStoreUpdater(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(Record<byte[], byte[]> record) {
        if (record.value() == null) {
            store.delete(record.key());
        } else {
            store.put(record.key(), record.value());
        }
    }
}
