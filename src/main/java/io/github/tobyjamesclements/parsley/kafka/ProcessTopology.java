package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;

import io.github.tobyjamesclements.parsley.api.KafkaNames;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.Store;

/**
 * Builds the Kafka Streams topology for one process.
 *
 * <p>Sources and sinks carry raw bytes. Application serdes are applied inside the processor,
 * after the delivery decision, so a payload that fails to decode cannot bypass the gate.
 *
 * <p>The ordering store is log-compacted, which lets a restarted process rebuild its state
 * from the changelog.
 */
final class ProcessTopology {

    /** Name of the store holding ordering state. */
    static final String ORDERING_STORE = Store.RESERVED_PREFIX + "ordering";
    private static final String PROCESSOR = "process";

    private ProcessTopology() {
    }

    /**
     * The one composition of a store's changelog topic name. Every site that needs the
     * name — validation and description at start, the held-message scan, and the serde
     * topic the processor hands to serializers — composes it here, so the spelling cannot
     * drift: a diverging serde topic would silently change schema-registry subjects.
     *
     * <p>Each component is validated at declaration, but only here are they composed; a
     * composite beyond Kafka's limit would otherwise fail deep inside Streams
     * internal-topic creation.
     *
     * @param applicationId the process's Kafka application id
     * @param storeName     a declared store name, or {@link #ORDERING_STORE}
     * @return the changelog topic name
     * @throws IllegalArgumentException if the composite exceeds Kafka's topic-name limit
     */
    static String changelogName(String applicationId, String storeName) {
        String changelog = applicationId + "-" + storeName + "-changelog";
        if (changelog.length() > KafkaNames.MAX_TOPIC_NAME_LENGTH) {
            throw new IllegalArgumentException("changelog topic name '" + changelog + "' exceeds"
                    + " Kafka's " + KafkaNames.MAX_TOPIC_NAME_LENGTH + "-character limit; shorten"
                    + " the applicationIdPrefix, process name or store name");
        }
        return changelog;
    }

    /**
     * The ordering store's builder: persistent, changelogged with compaction (D57), and
     * cached (D110). Every received record merges its whole frontier — one write per
     * channel whose position advanced — and every delivery merges the same channels into
     * the delivered past, so without the cache a record cost about two writes per
     * frontier channel to RocksDB and to the changelog. The cache holds the latest value
     * per key and writes it through at commit, so the writes per commit interval are
     * bounded by the keys touched rather than by records times channels; under
     * exactly-once the flush precedes the commit, so what a step persists is unchanged.
     *
     * @return the store builder
     */
    static org.apache.kafka.streams.state.StoreBuilder<org.apache.kafka.streams.state.KeyValueStore<
            org.apache.kafka.common.utils.Bytes, byte[]>> orderingStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ORDERING_STORE), Serdes.Bytes(), Serdes.ByteArray())
                .withLoggingEnabled(Map.of("cleanup.policy", "compact"))
                .withCachingEnabled();
    }

    /**
     * @param topic a received topic
     * @return the topology node name for its source
     */
    static String sourceName(String topic) {
        return "source-" + topic;
    }

    /**
     * @param topic a sent topic
     * @return the topology node name for its sink
     */
    static String sinkName(String topic) {
        return "sink-" + topic;
    }

    /**
     * Builds a topology with the default metadata budget, no start positions, and fresh
     * diagnostics.
     *
     * @param definition     the process to build
     * @param topics         resolved identity and width for every topic it uses
     * @param identitySource where topic identity is checked at task initialisation
     * @param statusInterval how often each task publishes its status
     * @return the topology
     */
    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          TopicIdentitySource identitySource, Duration statusInterval) {
        return build(definition, topics, identitySource, statusInterval, new ProcessDiagnostics());
    }

    /**
     * Builds a topology with the default metadata budget and no start positions, publishing
     * task status into {@code diagnostics}.
     *
     * @param definition     the process to build
     * @param topics         resolved identity and width for every topic it uses
     * @param identitySource where topic identity is checked at task initialisation
     * @param statusInterval how often each task publishes its status
     * @param diagnostics    where each task publishes its status
     * @return the topology
     */
    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          TopicIdentitySource identitySource, Duration statusInterval,
                          ProcessDiagnostics diagnostics) {
        return build(definition, topics, identitySource, Map.of(), statusInterval,
                io.github.tobyjamesclements.parsley.core.ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES, diagnostics);
    }

    /**
     * Builds a topology.
     *
     * @param definition          the process to build
     * @param topics              resolved identity and width for every topic it uses
     * @param identitySource      where topic identity is checked at task initialisation
     * @param startPositions      per received partition, the position the host feeds first,
     *                            as the bootstrap established it
     * @param statusInterval      how often each task publishes its status
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @param diagnostics         where each task publishes its status, read by
     *                            {@code ParsleyRuntime.status()}
     * @return the topology
     */
    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          TopicIdentitySource identitySource, Map<TopicPartition, Long> startPositions,
                          Duration statusInterval, int metadataBudgetBytes, ProcessDiagnostics diagnostics) {
        Topology topology = new Topology();
        String[] sources = definition.receivedTopics().stream().map(ProcessTopology::sourceName).toArray(String[]::new);
        for (String topic : definition.receivedTopics()) {
            topology.addSource(sourceName(topic), new ByteArrayDeserializer(), new ByteArrayDeserializer(), topic);
        }
        topology.addProcessor(PROCESSOR,
                () -> new ParsleyProcessor(definition, topics, identitySource, startPositions,
                        statusInterval, metadataBudgetBytes, diagnostics), sources);
        for (String topic : definition.sendTopics()) {
            topology.addSink(sinkName(topic), topic, new ByteArraySerializer(), new ByteArraySerializer(), PROCESSOR);
        }

        topology.addStateStore(orderingStore(), PROCESSOR);
        for (Store<?, ?> store : definition.stores()) {
            topology.addStateStore(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(store.name()), Serdes.Bytes(), Serdes.ByteArray()), PROCESSOR);
        }
        return topology;
    }
}
