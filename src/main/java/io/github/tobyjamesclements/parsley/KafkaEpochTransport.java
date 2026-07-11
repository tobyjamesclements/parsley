package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * The broker-backed {@link ParsleyEpochTransport}: side-channel raw Kafka clients over the topology's
 * single-partition {@code epoch-events} log, deliberately separate from the Streams runtime's business
 * producer. It owns:
 *
 * <ul>
 *   <li>an <strong>idempotent</strong> {@link KafkaProducer} for {@link #append} — the handshake rides
 *       its own producer, not the task's EOS transaction, so it is never rolled back with business
 *       output; correctness rests on the protocol's own idempotence (dedup by {@code epochId}), not on
 *       Streams transactions.
 *   <li>a {@link KafkaConsumer} <strong>manually assigned</strong> to partition 0 and seeked to the
 *       beginning — no consumer group, no committed offsets — so <strong>every instance independently
 *       replays the entire log</strong>. That full, offset-independent replay is what lets each node's
 *       {@link ParsleyEpochLog} fold arrive at the identical view without a leader.
 * </ul>
 *
 * <p>Client configuration is derived from the task's {@code appConfigs()} (so it inherits {@code
 * bootstrap.servers} and broker security), following the {@link ParsleyTopicAdmin#ofConfigs} precedent —
 * Kafka's client configs tolerate the extra Streams keys, logging them as unused. Only Parsley's required
 * overrides (byte-array serdes, idempotence, no auto-commit, no group) are forced on top.
 *
 * <p>Confined to the single {@link ParsleyEpochRuntime} thread that drives it; not thread-safe.
 */
final class KafkaEpochTransport implements ParsleyEpochTransport {

    private final String topic;
    private final TopicPartition partition;
    private final Producer<byte[], byte[]> producer;
    private final Consumer<byte[], byte[]> consumer;
    // The log's end offset at startup, captured lazily on the first caughtUp() check; the reader is
    // bootstrapped once its position reaches it. -1 until captured.
    private long bootstrapEndOffset = -1;

    /**
     * Builds the raw clients from {@code appConfigs} and assigns the consumer to the log's one
     * partition. Validates the topic's {@code cleanup.policy}/{@code retention.ms} first (see
     * {@link #requireEternalLogConfig}), before any client is built.
     */
    KafkaEpochTransport(Map<String, Object> appConfigs, String topic) {
        this(requireEternalLog(appConfigs, topic),
                new KafkaProducer<>(producerConfig(appConfigs)),
                new KafkaConsumer<>(consumerConfig(appConfigs)));
    }

    /** Injection point for a fake producer/consumer in tests; the public constructor builds real clients. */
    KafkaEpochTransport(String topic,
                               Producer<byte[], byte[]> producer,
                               Consumer<byte[], byte[]> consumer) {
        this.topic = topic;
        this.partition = new TopicPartition(topic, 0);
        this.producer = producer;
        this.consumer = consumer;
        // The protocol's total order IS partition 0's offset order: every append targets it and every
        // instance folds it from the beginning. A topic created with more partitions (e.g. a broker
        // default of 6) has no single total order to agree on, so fail fast here, at startup — before
        // this misconfiguration existed as a check, an append could land on a partition no fold ever
        // reads (the old null-key, unpinned send), silently losing a join or a publication forever.
        requireSinglePartition();
        // Read the whole log from the start on every instance — the fold's determinism depends on it.
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
    }

    /**
     * Describes {@code topic}'s configuration through a short-lived {@link Admin} over {@code
     * appConfigs} and validates it with {@link #requireEternalLogConfig}, returning {@code topic} so
     * the broker-backed constructor can run this before building any client. Mirrors {@link
     * #requireSinglePartition}: both misconfigurations silently corrupt the protocol rather than
     * failing loudly, so both are checked at startup, before the first append.
     */
    private static String requireEternalLog(Map<String, Object> appConfigs, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config;
        try (Admin admin = Admin.create(new HashMap<>(appConfigs))) {
            config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted describing epoch-events topic '" + topic + "'", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to describe epoch-events topic '" + topic
                    + "'; the leaderless epoch protocol requires it to exist (single partition, "
                    + "cleanup.policy=delete, retention.ms=-1) before a coordinated topology starts",
                    e.getCause());
        }
        requireEternalLogConfig(topic,
                configValue(config, TopicConfig.CLEANUP_POLICY_CONFIG),
                configValue(config, TopicConfig.RETENTION_MS_CONFIG));
        return topic;
    }

    private static @Nullable String configValue(@Nullable Config config, String key) {
        if (config == null) {
            return null;
        }
        ConfigEntry entry = config.get(key);
        return entry == null ? null : entry.value();
    }

    /**
     * Fails fast when the epoch-events topic could ever lose history. Every instance's {@link
     * ParsleyEpochLog} fold replays this log <em>from the beginning</em>, and its correctness depends
     * on the entire history surviving: a {@code cleanup.policy} that includes {@code compact}, or a
     * finite {@code retention.ms}, silently amputates old {@code JoinRequested}/{@code EpochCommitted}
     * events — after which a restarted instance folds the truncated log, sees {@code committedEpochId
     * == 0}, and treats an established domain as a cold start. That failure is silent and unbounded-in
     * -time, so — exactly like {@link #requireSinglePartition} — it is checked loudly at startup
     * instead. A {@code null} or unparseable value is tolerated (unknown, not provably wrong).
     */
    static void requireEternalLogConfig(String topic, @Nullable String cleanupPolicy, @Nullable String retentionMs) {
        if (cleanupPolicy != null && cleanupPolicy.contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
            throw new IllegalStateException("epoch-events topic '" + topic + "' has cleanup.policy="
                    + cleanupPolicy + "; the leaderless epoch protocol replays the whole log from the "
                    + "beginning on every start, and compaction removes events the fold needs — set "
                    + "cleanup.policy=delete with retention.ms=-1");
        }
        if (retentionMs == null || retentionMs.isBlank()) {
            return;
        }
        long retention;
        try {
            retention = Long.parseLong(retentionMs.trim());
        } catch (NumberFormatException e) {
            return;
        }
        if (retention >= 0) {
            throw new IllegalStateException("epoch-events topic '" + topic + "' has retention.ms="
                    + retention + "; the leaderless epoch protocol replays the whole log from the "
                    + "beginning on every start, and finite retention silently deletes the join/commit "
                    + "history the fold needs — set retention.ms=-1");
        }
    }

    private void requireSinglePartition() {
        int partitionCount = consumer.partitionsFor(topic).size();
        if (partitionCount != 1) {
            throw new IllegalStateException("epoch-events topic '" + topic + "' has " + partitionCount
                    + " partitions; the leaderless epoch protocol requires exactly 1 — its total order is that"
                    + " one partition's offset order, and events on any other partition would never be read."
                    + " Recreate the topic with a single partition");
        }
    }

    @Override
    public void append(ParsleyEpochEvent event) {
        // Pinned to partition 0 explicitly — the partition every fold reads — never left to the
        // producer's partitioner (a null-key send would scatter across partitions on a mis-created
        // topic, landing events where no fold ever looks). Null key: ordering is by offset, not key.
        // Block for the ack so a subsequent poll on this node can observe the appended event's effect
        // deterministically.
        try {
            producer.send(new ProducerRecord<>(topic, 0, null, event.toBytes())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted appending to " + topic, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to append to " + topic, e.getCause());
        }
    }

    @Override
    public List<ParsleyEpochEvent> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        List<ParsleyEpochEvent> events = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
            events.add(ParsleyEpochEvent.fromBytes(record.value()));
        }
        return events;
    }

    @Override
    public boolean caughtUp() {
        // Capture the end offset once (a snapshot of the backlog to fold); the log keeps growing but
        // bootstrap means "read up to what existed at startup", so we do not chase a moving end.
        if (bootstrapEndOffset < 0) {
            bootstrapEndOffset = consumer.endOffsets(List.of(partition)).getOrDefault(partition, 0L);
        }
        return consumer.position(partition) >= bootstrapEndOffset;
    }

    @Override
    public void close() {
        // Close both even if the first throws, so a client leak cannot mask the other's cleanup.
        try {
            producer.close();
        } finally {
            consumer.close();
        }
    }

    /**
     * The idempotent producer config: {@code appConfigs} as a base (bootstrap + security), with
     * byte-array serializers and idempotence forced on. No {@code transactional.id} — the handshake is
     * idempotent, not transactional.
     */
    static Map<String, Object> producerConfig(Map<String, Object> appConfigs) {
        Map<String, Object> config = new HashMap<>(appConfigs);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return config;
    }

    /**
     * The consumer config for a manual-assignment full-log reader: {@code appConfigs} as a base, with
     * byte-array deserializers, auto-commit disabled, and any {@code group.id} removed — the consumer
     * uses {@code assign} + {@code seekToBeginning}, so it belongs to no group and commits no offsets.
     */
    static Map<String, Object> consumerConfig(Map<String, Object> appConfigs) {
        Map<String, Object> config = new HashMap<>(appConfigs);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.remove(ConsumerConfig.GROUP_ID_CONFIG);
        return config;
    }
}
