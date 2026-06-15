package io.parsley;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The {@link CausalProcessorSupplier} returned by {@code CausalProcessorSupplier.create(...)}: it wraps the user's
 * supplier in a {@link ParsleyProcessor} and {@linkplain #stores() unions} the user's
 * declared state stores with Parsley's internal frontier and buffer stores, so the DSL wires all of
 * them to the same processor node. The user never names Parsley's internal stores.
 */
final class ParsleyProcessorSupplier<KIn, VIn, KOut, VOut>
        implements CausalProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    private final CausalBufferingPolicy policy;
    private final CausalViolationHandler onViolation;
    private final Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink;
    private final Function<String, Serde<KIn>> keySerdeByTopic;
    private final Function<String, Serde<VIn>> valueSerdeByTopic;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final CausalFrontierListener frontierListener;

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      CausalBufferingPolicy policy,
                                      CausalViolationHandler onViolation,
                                      Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic) {
        this(userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic,
                ParsleyAttributes.FRONTIER_STORE, ParsleyAttributes.BUFFER_STORE);
    }

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      CausalBufferingPolicy policy,
                                      CausalViolationHandler onViolation,
                                      Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName) {
        this(userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic,
                frontierStoreName, bufferStoreName, CausalFrontierListener.noop());
    }

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      CausalBufferingPolicy policy,
                                      CausalViolationHandler onViolation,
                                      Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName,
                                      CausalFrontierListener frontierListener) {
        this.userSupplier = userSupplier;
        this.policy = policy;
        this.onViolation = onViolation;
        this.deadLetterSink = deadLetterSink;
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.frontierListener = frontierListener;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new ParsleyProcessor<>(
                userSupplier.get(), policy, onViolation, deadLetterSink,
                new ParsleySerializer<>(new ParsleyResolver<>(keySerdeByTopic, valueSerdeByTopic)),
                frontierStoreName, bufferStoreName, frontierListener);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        Set<StoreBuilder<?>> stores = new HashSet<>();
        Set<StoreBuilder<?>> userStores = userSupplier.stores();
        if (userStores != null) {
            stores.addAll(userStores);
        }
        stores.add(byteStore(frontierStoreName));
        stores.add(bufferStore(bufferStoreName));
        return stores;
    }

    // The frontier store is keyed by a single well-known String key; the buffer store is keyed by the
    // monotonic insertion sequence (a long), so its entries iterate in causal arrival order.
    private static StoreBuilder<KeyValueStore<String, byte[]>> byteStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.String(),
                Serdes.ByteArray());
    }

    private static StoreBuilder<KeyValueStore<Long, byte[]>> bufferStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.Long(),
                Serdes.ByteArray());
    }
}
