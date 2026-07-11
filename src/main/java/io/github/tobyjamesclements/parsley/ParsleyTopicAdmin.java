package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Facade (GoF) narrowing the Kafka Admin operations Parsley performs at startup: resolving input
 * topics' stable UUIDs (the causal identity), reading their partition counts and {@code
 * cleanup.policy} (for the {@code parsley.topology.validation} checks), and creating the outbox
 * topic. Keeping the interface narrow lets tests implement it without the full ~40-method
 * {@link Admin} surface, and lets the processor path resolve UUIDs at {@code init()} from
 * {@link #ofConfigs}.
 */
interface ParsleyTopicAdmin extends AutoCloseable {

    /**
     * Returns the stable Kafka UUID for each name in {@code topics} — the identity Parsley keys
     * frontiers and dependencies by (stable across topic deletion/recreation; the name is not).
     *
     * @param topics the topic names to look up
     * @return topic UUIDs; must include every requested topic
     */
    Map<String, Uuid> topicIds(List<String> topics) throws Exception;

    /**
     * Returns the partition count for each name in {@code topics}.
     *
     * @param topics the topic names to look up
     * @return partition counts; must include every requested topic
     */
    Map<String, Integer> partitionCounts(List<String> topics) throws Exception;

    /**
     * Creates a topic with the given name and partition count. Implementations may silently ignore
     * this call (e.g. in tests that have no broker), but must not throw if the topic already
     * exists — callers handle that case themselves.
     *
     * @param name       the topic name to create
     * @param partitions the desired partition count
     */
    void createTopic(String name, int partitions) throws Exception;

    /**
     * Returns the effective {@code cleanup.policy} for each name in {@code topics} — the topic's own
     * override if set, otherwise the broker default.
     *
     * @param topics the topic names to look up
     * @return each topic's effective {@code cleanup.policy}; must include every requested topic
     */
    Map<String, String> cleanupPolicies(List<String> topics) throws Exception;

    /**
     * Returns a {@link ParsleyTopicAdmin} backed by a real Kafka {@link Admin} created from
     * {@code configs} — used by the processor path, which resolves UUIDs at {@code init()} from the
     * task's {@code appConfigs()} (so it inherits broker security settings, not just bootstrap). The
     * returned instance closes the underlying {@code Admin} on {@link #close()}. Admin tolerates the
     * extra Streams config keys (it logs them as unknown).
     *
     * @param configs the configuration to build the {@link Admin} from; must include
     *                {@code bootstrap.servers}
     * @return a live, closeable {@code ParsleyTopicAdmin}
     */
    static ParsleyTopicAdmin ofConfigs(Map<String, Object> configs) {
        Admin admin = Admin.create(new HashMap<>(configs));
        ParsleyTopicAdmin delegate = ofAdmin(admin);
        return new ParsleyTopicAdmin() {
            @Override
            public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                return delegate.topicIds(topics);
            }

            @Override
            public Map<String, Integer> partitionCounts(List<String> topics) throws Exception {
                return delegate.partitionCounts(topics);
            }

            @Override
            public void createTopic(String name, int partitions) throws Exception {
                delegate.createTopic(name, partitions);
            }

            @Override
            public Map<String, String> cleanupPolicies(List<String> topics) throws Exception {
                return delegate.cleanupPolicies(topics);
            }

            @Override
            public void close() {
                admin.close();
            }
        };
    }

    /**
     * Returns a {@link ParsleyTopicAdmin} backed by a caller-owned {@link Admin}. Unlike
     * {@link #ofConfigs}, the returned instance does <strong>not</strong> close {@code admin} on
     * {@link #close()} — the caller keeps ownership of its lifecycle. Used by {@link ParsleyTopics} to
     * resolve UUIDs through an {@code Admin} the application already manages.
     *
     * @param admin the Kafka admin client to resolve through; must not be {@code null}
     * @return a {@code ParsleyTopicAdmin} over {@code admin} whose {@link #close()} is a no-op
     */
    static ParsleyTopicAdmin ofAdmin(Admin admin) {
        return new ParsleyTopicAdmin() {
            @Override
            public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
                Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames().get();
                Map<String, Uuid> ids = new HashMap<>();
                descriptions.forEach((topic, desc) -> ids.put(topic, desc.topicId()));
                return ids;
            }

            @Override
            public Map<String, Integer> partitionCounts(List<String> topics) throws Exception {
                Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames().get();
                Map<String, Integer> counts = new HashMap<>();
                descriptions.forEach((topic, desc) -> counts.put(topic, desc.partitions().size()));
                return counts;
            }

            @Override
            public void createTopic(String name, int partitions) throws Exception {
                admin.createTopics(Set.of(new NewTopic(name, partitions, (short) 1))).all().get();
            }

            @Override
            public Map<String, String> cleanupPolicies(List<String> topics) throws Exception {
                List<ConfigResource> resources = topics.stream()
                        .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
                        .toList();
                Map<ConfigResource, Config> configs = admin.describeConfigs(resources).all().get();
                Map<String, String> policies = new HashMap<>();
                configs.forEach((resource, config) -> {
                    ConfigEntry entry = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
                    policies.put(resource.name(), entry != null ? entry.value() : TopicConfig.CLEANUP_POLICY_DELETE);
                });
                return policies;
            }

            @Override
            public void close() {
                // caller owns the Admin's lifecycle
            }
        };
    }
}
