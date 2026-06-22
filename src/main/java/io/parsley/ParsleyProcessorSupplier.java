package io.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link CausalProcessorSupplier} returned by {@link CausalProcessors}: it wraps the user's
 * supplier in a {@link ParsleyProcessor} and {@linkplain #stores() unions} the user's
 * declared state stores with Parsley's internal frontier and buffer stores, so the DSL wires all of
 * them to the same processor node. The user never names Parsley's internal stores.
 */
final class ParsleyProcessorSupplier<KIn, VIn, KOut, VOut>
        implements CausalProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    private final CausalBufferLimit limit;
    private final Function<String, Serde<KIn>> keySerdeByTopic;
    private final Function<String, Serde<VIn>> valueSerdeByTopic;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String positionIndexStoreName;
    private final Set<String> topics;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      CausalBufferLimit limit,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName,
                                      String positionIndexStoreName,
                                      Set<String> topics,
                                      Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                                      ParsleyConfig config) {
        this.userSupplier = userSupplier;
        this.limit = limit;
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.positionIndexStoreName = positionIndexStoreName;
        this.topics = topics;
        this.adminFactory = adminFactory;
        this.config = config;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new ParsleyProcessor<>(
                userSupplier.get(), limit,
                new ParsleySerializer<>(new ParsleyResolver<>(keySerdeByTopic, valueSerdeByTopic)),
                frontierStoreName, bufferStoreName, positionIndexStoreName, topics, adminFactory, config);
    }

    /** The buffer eviction limit this supplier was built with. Package-private for tests. */
    CausalBufferLimit limit() {
        return limit;
    }

    /** The effective Parsley configuration this supplier was built with. Package-private for tests. */
    ParsleyConfig config() {
        return config;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        Set<StoreBuilder<?>> stores = new HashSet<>();
        Set<StoreBuilder<?>> userStores = userSupplier.stores();
        if (userStores != null) {
            stores.addAll(userStores);
        }
        stores.add(ParsleyStores.frontierStore(frontierStoreName));
        stores.add(ParsleyStores.bufferStore(bufferStoreName));
        stores.add(ParsleyStores.positionIndexStore(positionIndexStoreName));
        return stores;
    }
}
