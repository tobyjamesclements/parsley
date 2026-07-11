package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@link ProcessorSupplier} returned by {@link ParsleyProcessors}: it wraps the user's
 * supplier in a {@link ParsleyProcessor} and {@linkplain #stores() unions} the user's
 * declared state stores with Parsley's internal frontier and buffer stores, so the DSL wires all of
 * them to the same processor node. The user never names Parsley's internal stores.
 */
final class ParsleyProcessorSupplier<KIn, VIn, KOut, VOut>
        implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier;
    private final Function<String, Serde<KIn>> keySerdeByTopic;
    private final Function<String, Serde<VIn>> valueSerdeByTopic;
    private final String frontierStoreName;
    private final String bufferStoreName;
    private final String candidateIndexStoreName;
    private final String forwardedIndexStoreName;
    private final Set<String> topics;
    private final Set<String> passthroughTopics;
    private final Set<String> sinkTopics;
    private final List<String> sinkNodeNames;
    private final Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory;
    private final ParsleyConfig config;
    private final @Nullable ParsleyQuiesce quiesce;
    private final @Nullable ParsleyCoordination coordination;

    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName,
                                      String candidateIndexStoreName,
                                      String forwardedIndexStoreName,
                                      Set<String> topics,
                                      Set<String> sinkTopics,
                                      List<String> sinkNodeNames,
                                      Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                                      ParsleyConfig config,
                                      @Nullable ParsleyQuiesce quiesce,
                                      @Nullable ParsleyCoordination coordination) {
        this(userSupplier, keySerdeByTopic, valueSerdeByTopic, frontierStoreName, bufferStoreName,
                candidateIndexStoreName, forwardedIndexStoreName, topics, Set.of(),
                sinkTopics, sinkNodeNames, adminFactory, config, quiesce, coordination);
    }

    /**
     * @param passthroughTopics a subset of {@code topics} that {@link CausalTopology} wires as extra,
     *                          raw byte[]/byte[] sources into the same processor node — a domain topic
     *                          this stage does not otherwise consume or produce, whose sole purpose is
     *                          contributing its causal progress to this task's frontier (see {@link
     *                          ParsleyProcessor}'s passthrough-record handling). Empty for the low-level
     *                          {@link ParsleyProcessors} API's ordinary use.
     */
    ParsleyProcessorSupplier(ProcessorSupplier<KIn, VIn, KOut, VOut> userSupplier,
                                      Function<String, Serde<KIn>> keySerdeByTopic,
                                      Function<String, Serde<VIn>> valueSerdeByTopic,
                                      String frontierStoreName,
                                      String bufferStoreName,
                                      String candidateIndexStoreName,
                                      String forwardedIndexStoreName,
                                      Set<String> topics,
                                      Set<String> passthroughTopics,
                                      Set<String> sinkTopics,
                                      List<String> sinkNodeNames,
                                      Function<Map<String, Object>, ParsleyTopicAdmin> adminFactory,
                                      ParsleyConfig config,
                                      @Nullable ParsleyQuiesce quiesce,
                                      @Nullable ParsleyCoordination coordination) {
        this.userSupplier = userSupplier;
        this.keySerdeByTopic = keySerdeByTopic;
        this.valueSerdeByTopic = valueSerdeByTopic;
        this.frontierStoreName = frontierStoreName;
        this.bufferStoreName = bufferStoreName;
        this.candidateIndexStoreName = candidateIndexStoreName;
        this.forwardedIndexStoreName = forwardedIndexStoreName;
        this.topics = topics;
        this.passthroughTopics = passthroughTopics;
        this.sinkTopics = sinkTopics;
        this.sinkNodeNames = sinkNodeNames;
        this.adminFactory = adminFactory;
        this.config = config;
        this.quiesce = quiesce;
        this.coordination = coordination;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new ParsleyProcessor<>(
                userSupplier.get(),
                new ParsleySerializer<>(new ParsleyResolver<>(keySerdeByTopic, valueSerdeByTopic)),
                frontierStoreName, bufferStoreName, candidateIndexStoreName, forwardedIndexStoreName,
                topics, passthroughTopics, sinkTopics, sinkNodeNames,
                adminFactory, config, quiesce, ParsleyEpochSnapshotPublisher.NOOP, coordination);
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
        stores.add(ParsleyStores.candidateIndexStore(candidateIndexStoreName));
        stores.add(ParsleyStores.forwardedIndexStore(forwardedIndexStoreName));
        return stores;
    }
}
