package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * The {@link ParsleyTopics} implementation. Two modes, chosen by constructor: wrapping a single
 * long-lived, caller-supplied {@link ParsleyTopicAdmin} (never closed here — used directly by tests
 * against a hand-rolled double), or resolving through a fresh, short-lived {@link ParsleyTopicAdmin}
 * built from {@link Properties} and closed immediately after each cache-miss lookup (the production
 * path — a resolver bound this way holds no live connection between calls). Either way, resolved
 * UUIDs are cached so a topic is described at most once; the cache is thread-safe, since a single
 * resolver may be shared across the threads stamping records.
 */
final class KafkaTopics implements ParsleyTopics {

    private final @Nullable ParsleyTopicAdmin admin;
    private final @Nullable Properties props;
    private final Map<String, Uuid> cache = new ConcurrentHashMap<>();

    KafkaTopics(ParsleyTopicAdmin admin) {
        this.admin = Objects.requireNonNull(admin, "admin must not be null");
        this.props = null;
    }

    KafkaTopics(Properties props) {
        this.admin = null;
        this.props = Objects.requireNonNull(props, "props must not be null");
    }

    @Override
    public Uuid topicId(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");
        return cache.computeIfAbsent(topic, this::resolve);
    }

    private Uuid resolve(String topic) {
        if (admin != null) {
            return resolveVia(admin, topic);
        }
        Map<String, Object> configs = new HashMap<>();
        Objects.requireNonNull(props).forEach((key, value) -> configs.put(String.valueOf(key), value));
        try (ParsleyTopicAdmin fresh = ParsleyTopicAdmin.ofConfigs(configs)) {
            return resolveVia(fresh, topic);
        } catch (RuntimeException e) {
            throw e; // already mapped by resolveVia; don't double-wrap
        } catch (Exception e) {
            throw new IllegalStateException("failed resolving topic id for '" + topic + "'", e);
        }
    }

    private static Uuid resolveVia(ParsleyTopicAdmin admin, String topic) {
        Map<String, Uuid> ids;
        try {
            ids = admin.topicIds(List.of(topic));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted resolving topic id for '" + topic + "'", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new IllegalArgumentException("unknown topic '" + topic + "'", cause);
            }
            throw new IllegalStateException("failed resolving topic id for '" + topic + "'", cause);
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
