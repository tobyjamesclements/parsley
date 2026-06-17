package io.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Narrow abstraction over the two Kafka Admin operations that {@link ParsleyConsumer} uses at
 * startup: describing input topics (to read their UUIDs and partition counts) and creating the
 * outbox topic. Keeping the interface narrow lets tests supply a {@code MockAdminClient} without
 * implementing the full ~40-method {@link Admin} surface.
 */
interface ParsleyTopicAdmin extends AutoCloseable {

    /**
     * Returns topic metadata keyed by topic name for each name in {@code topics}.
     *
     * @param topics the topic names to describe
     * @return metadata map; must include every requested topic
     */
    Map<String, TopicDescription> describeTopics(List<String> topics) throws Exception;

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
     * Returns a {@link ParsleyTopicAdmin} backed by a real Kafka {@link Admin} connected to
     * {@code bootstrap}. The returned instance closes the underlying {@code Admin} on
     * {@link #close()}.
     *
     * @param bootstrap the Kafka bootstrap servers string
     * @return a live, closeable {@code ParsleyTopicAdmin}
     */
    static ParsleyTopicAdmin ofBootstrap(String bootstrap) {
        Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
        return new ParsleyTopicAdmin() {
            @Override
            public Map<String, TopicDescription> describeTopics(List<String> topics) throws Exception {
                return admin.describeTopics(topics).allTopicNames().get();
            }

            @Override
            public void createTopic(String name, int partitions) throws Exception {
                admin.createTopics(Set.of(new NewTopic(name, partitions, (short) 1))).all().get();
            }

            @Override
            public void close() {
                admin.close();
            }
        };
    }
}
