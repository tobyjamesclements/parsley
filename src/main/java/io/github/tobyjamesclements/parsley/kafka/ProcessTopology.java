package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;

import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.StoreDef;

/**
 * Builds the byte-level topology for one declared process: sources for its received topics, one processor node,
 * sinks for its send topics, the reserved ordering store, and the declared application stores. Sources carry no
 * per-source reset policy: with the main consumer's {@code auto.offset.reset=none} (set by the runtime), a position
 * outside the retained range kills the task rather than silently skipping discarded messages (SPEC Safety 8).
 * Package-private on purpose — the topology never leaves parsley's control (SPEC Structural 9, Substrate 3).
 */
final class ProcessTopology {

    static final String ORDERING_STORE = "__parsley.ordering";
    private static final String PROCESSOR = "process";

    private ProcessTopology() {
    }

    static String sourceName(String topic) {
        return "source-" + topic;
    }

    static String sinkName(String topic) {
        return "sink-" + topic;
    }

    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          FactsSource factsSource, Duration factsInterval) {
        return build(definition, topics, factsSource, factsInterval, Runnable::run,
                io.github.tobyjamesclements.parsley.core.ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES);
    }

    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          FactsSource factsSource, Duration factsInterval,
                          java.util.concurrent.Executor factsExecutor, int metadataBudgetBytes) {
        Topology topology = new Topology();
        String[] sources = definition.receivedTopics().stream().map(ProcessTopology::sourceName).toArray(String[]::new);
        for (String topic : definition.receivedTopics()) {
            topology.addSource(sourceName(topic), new ByteArrayDeserializer(), new ByteArrayDeserializer(), topic);
        }
        topology.addProcessor(PROCESSOR,
                () -> new ParsleyProcessor(definition, topics, factsSource, factsInterval,
                        factsExecutor, metadataBudgetBytes), sources);
        for (String topic : definition.sendTopics()) {
            topology.addSink(sinkName(topic), topic, new ByteArraySerializer(), new ByteArraySerializer(), PROCESSOR);
        }
        // The guarantees stand on this store being changelogged with compaction (restart restores it — Safety 2,
        // Liveness 5 — and held entries must never age out of the changelog). Both were Streams *defaults* this
        // build relied on implicitly; they are requested explicitly so a defaults change cannot silently void
        // them (D57).
        topology.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ORDERING_STORE), Serdes.Bytes(), Serdes.ByteArray())
                .withLoggingEnabled(Map.of("cleanup.policy", "compact")), PROCESSOR);
        for (StoreDef<?, ?> store : definition.stores()) {
            topology.addStateStore(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(store.name()), Serdes.Bytes(), Serdes.ByteArray()), PROCESSOR);
        }
        return topology;
    }
}
