package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * The {@link CausalTopics} implementation backed by a {@link ParsleyTopicAdmin}: resolves a topic
 * name to its stable Kafka UUID on first use and caches the result. The cache is thread-safe, since a
 * single resolver may be shared across the threads stamping records.
 */
final class ParsleyCausalTopics implements CausalTopics {

    private final ParsleyTopicAdmin admin;
    private final Map<String, Uuid> cache = new ConcurrentHashMap<>();

    ParsleyCausalTopics(ParsleyTopicAdmin admin) {
        this.admin = Objects.requireNonNull(admin, "admin must not be null");
    }

    @Override
    public Uuid topicId(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        return cache.computeIfAbsent(topic, this::resolve);
    }

    private Uuid resolve(String topic) {
        Map<String, Uuid> ids;
        try {
            ids = admin.topicIds(List.of(topic));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted resolving topic id for '" + topic + "'", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new IllegalArgumentException("unknown topic '" + topic + "'", e.getCause());
            }
            throw new IllegalStateException("failed resolving topic id for '" + topic + "'", e.getCause());
        } catch (Exception e) {
            throw new IllegalStateException("failed resolving topic id for '" + topic + "'", e);
        }
        Uuid id = ids.get(topic);
        if (id == null) {
            throw new IllegalArgumentException("unknown topic '" + topic + "'");
        }
        return id;
    }
}
