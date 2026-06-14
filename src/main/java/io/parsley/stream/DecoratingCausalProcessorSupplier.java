package io.parsley.stream;

import io.parsley.BufferingPolicy;
import io.parsley.ViolationHandler;
import io.parsley.internal.Attributes;
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
 * The {@link ProcessorSupplier} returned by {@code Parsley.causal(...)}: it wraps the user's
 * supplier in a {@link DecoratingCausalProcessor} and {@linkplain #stores() unions} the user's
 * declared state stores with Parsley's internal frontier and buffer stores, so the DSL wires all of
 * them to the same processor node. The user never names Parsley's internal stores.
 */
final class DecoratingCausalProcessorSupplier<KIn, VIn, KOut, VOut>
        implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    private final BufferingPolicy policy;
    private final ViolationHandler onViolation;
    private final Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink;
    private final Function<String, Serde<KIn>> keySerdeByTopic;
    private final Function<String, Serde<VIn>> valueSerdeByTopic;
    private final String frontierStoreName;
    private final String bufferStoreName;

    DecoratingCausalProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      BufferingPolicy policy,
                                      ViolationHandler onViolation,
                                      Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic) {
        this(userSupplier, policy, onViolation, deadLetterSink, keySerdeByTopic, valueSerdeByTopic,
                Attributes.FRONTIER_STORE, Attributes.BUFFER_STORE);
    }

    DecoratingCausalProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      BufferingPolicy policy,
                                      ViolationHandler onViolation,
                                      Consumer<ConsumerRecord<KIn, VIn>> deadLetterSink,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName) {
        this.userSupplier = userSupplier;
        this.policy = policy;
        this.onViolation = onViolation;
        this.deadLetterSink = deadLetterSink;
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new DecoratingCausalProcessor<>(
                userSupplier.get(), policy, onViolation, deadLetterSink,
                new BufferedRecordCodec<>(keySerdeByTopic, valueSerdeByTopic),
                frontierStoreName, bufferStoreName);
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        Set<StoreBuilder<?>> stores = new HashSet<>();
        Set<StoreBuilder<?>> userStores = userSupplier.stores();
        if (userStores != null) {
            stores.addAll(userStores);
        }
        stores.add(byteStore(frontierStoreName));
        stores.add(byteStore(bufferStoreName));
        return stores;
    }

    private static StoreBuilder<KeyValueStore<String, byte[]>> byteStore(String name) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(name),
                Serdes.String(),
                Serdes.ByteArray());
    }
}
