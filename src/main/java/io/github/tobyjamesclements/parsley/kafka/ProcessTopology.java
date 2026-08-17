package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;

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

    /**
     * Kafka's topic-name length limit. Mirrors {@code KafkaNames.MAX_TOPIC_NAME_LENGTH} in
     * {@code api}, which is package-private there;
     * {@code TopologyWiringTest#composedChangelogNameIsBoundedAtExactlyKafkasLimit} pins
     * this side of the mirror at the 249/250 boundary, matching the declaration-site pin
     * in {@code ApiValidationTest}, so the two cannot drift apart with the suite green.
     */
    private static final int MAX_TOPIC_NAME_LENGTH = 249;

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
        if (changelog.length() > MAX_TOPIC_NAME_LENGTH) {
            throw new IllegalArgumentException("changelog topic name '" + changelog + "' exceeds"
                    + " Kafka's " + MAX_TOPIC_NAME_LENGTH + "-character limit; shorten the"
                    + " applicationIdPrefix, process name or store name");
        }
        return changelog;
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
     * Builds a topology with the default metadata budget, gathering facts on the calling
     * thread.
     *
     * @param definition    the process to build
     * @param topics        resolved identity and width for every topic it uses
     * @param factsSource   where broker facts come from
     * @param factsInterval how often to refresh them
     * @return the topology
     */
    static Topology build(ProcessDefinition definition, Map<String, TopicInfo> topics,
                          FactsSource factsSource, Duration factsInterval) {
        return build(definition, topics, factsSource, factsInterval, Runnable::run,
                io.github.tobyjamesclements.parsley.core.ProcessEngine.DEFAULT_METADATA_BUDGET_BYTES);
    }

    /**
     * Builds a topology.
     *
     * @param definition          the process to build
     * @param topics              resolved identity and width for every topic it uses
     * @param factsSource         where broker facts come from
     * @param factsInterval       how often to refresh them
     * @param factsExecutor       where the refresh runs, kept off the processing thread in
     *                            production so a slow broker query cannot stall delivery
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @return the topology
     */
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

        topology.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ORDERING_STORE), Serdes.Bytes(), Serdes.ByteArray())
                .withLoggingEnabled(Map.of("cleanup.policy", "compact")), PROCESSOR);
        for (Store<?, ?> store : definition.stores()) {
            topology.addStateStore(Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(store.name()), Serdes.Bytes(), Serdes.ByteArray()), PROCESSOR);
        }
        return topology;
    }
}
