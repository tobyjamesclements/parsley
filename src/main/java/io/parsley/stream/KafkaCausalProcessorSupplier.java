package io.parsley.stream;

import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Set;
import java.util.function.Consumer;

final class KafkaCausalProcessorSupplier<K, V> implements CausalProcessorSupplier<K, V> {

    private final BufferingPolicy policy;
    private final CausalViolationHandler violationHandler;
    private final Consumer<ConsumerRecord<K, V>> deadLetterSink;
    private final String frontierStoreName;

    KafkaCausalProcessorSupplier(BufferingPolicy policy, CausalViolationHandler violationHandler,
                                 Consumer<ConsumerRecord<K, V>> deadLetterSink, String frontierStoreName) {
        this.policy = policy;
        this.violationHandler = violationHandler;
        this.deadLetterSink = deadLetterSink;
        this.frontierStoreName = frontierStoreName;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new CausalProcessor<>(policy, violationHandler, deadLetterSink, frontierStoreName);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<String, byte[]>> frontierBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(frontierStoreName),
                        Serdes.String(),
                        Serdes.ByteArray());
        return Set.of(frontierBuilder);
    }
}
