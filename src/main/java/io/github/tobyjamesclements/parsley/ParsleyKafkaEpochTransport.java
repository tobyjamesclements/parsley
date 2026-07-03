package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.clients.consumer.Consumer;

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
final class ParsleyKafkaEpochTransport implements ParsleyEpochTransport {

    private final String topic;
    private final TopicPartition partition;
    private final Producer<byte[], byte[]> producer;
    private final Consumer<byte[], byte[]> consumer;
    // The log's end offset at startup, captured lazily on the first caughtUp() check; the reader is
    // bootstrapped once its position reaches it. -1 until captured.
    private long bootstrapEndOffset = -1;

    /** Builds the raw clients from {@code appConfigs} and assigns the consumer to the log's one partition. */
    ParsleyKafkaEpochTransport(Map<String, Object> appConfigs, String topic) {
        this(topic,
                new KafkaProducer<>(producerConfig(appConfigs)),
                new KafkaConsumer<>(consumerConfig(appConfigs)));
    }

    /** Injection point for a fake producer/consumer in tests; the public constructor builds real clients. */
    ParsleyKafkaEpochTransport(String topic,
                               Producer<byte[], byte[]> producer,
                               Consumer<byte[], byte[]> consumer) {
        this.topic = topic;
        this.partition = new TopicPartition(topic, 0);
        this.producer = producer;
        this.consumer = consumer;
        // Read the whole log from the start on every instance — the fold's determinism depends on it.
        // seekToBeginning is lazy (applied on the first poll), so no broker round-trip happens here.
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
    }

    @Override
    public void append(EpochEvent event) {
        // Null key: the log is single-partition, so ordering is by offset, not key. Block for the ack so
        // a subsequent poll on this node can observe the appended event's effect deterministically.
        try {
            producer.send(new ProducerRecord<>(topic, null, event.toBytes())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted appending to " + topic, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to append to " + topic, e.getCause());
        }
    }

    @Override
    public List<EpochEvent> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        List<EpochEvent> events = new ArrayList<>(records.count());
        for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
            events.add(EpochEvent.fromBytes(record.value()));
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
