package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.api.TaskStatus;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.DeliverableMessage;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.PositionFacts;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;
import io.github.tobyjamesclements.parsley.core.ReceivedMessage;

/**
 * One task of the kafka-clients host: partition {@code p} of every topic its process
 * receives, its engine, its ordering state and its application stores (D114).
 *
 * <p>The task never persists a held message. Its committed read position on a channel is
 * the position of the oldest held message when one exists, and the consumer's own position
 * otherwise, so a restart re-feeds the hold-back buffer from the log rather than from a
 * changelog. The engine is told where each channel resumes so it accepts the re-fed
 * messages rather than dropping them as covered.
 */
final class ProcessTask {
    private final ProcessDefinition definition;
    private final int partition;
    private final Map<String, ChannelId> channelByTopic = new HashMap<>();
    private final Map<ChannelId, String> topicByChannel = new HashMap<>();
    private final Map<ChannelId, TopicPartition> partitionByChannel = new TreeMap<>();
    private final ProcessEngine engine;
    private final TopicStore orderingStore;
    private final Map<String, TopicStore> stores = new LinkedHashMap<>();
    private final DeliverySeam seam;
    private long factsAppliedAtNanos;

    /**
     * @param definition          the process this task belongs to
     * @param topics              resolved identity and width for every topic it uses
     * @param partition           this task's partition
     * @param orderingTopic       the compacted topic holding ordering state
     * @param orderingView        partition {@code p} of it, latest value per key
     * @param storeViews          partition {@code p} of each declared store's changelog
     * @param changelogByStore    the changelog topic per declared store
     * @param resumeNextRead      per received partition, the position the consumer feeds next
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     * @param producer            the transactional producer state and emissions go through
     * @param onWrite             opens the transaction before the first write of a step
     */
    ProcessTask(ProcessDefinition definition, Map<String, TopicInfo> topics, int partition,
                String orderingTopic, Map<byte[], byte[]> orderingView,
                Map<String, Map<byte[], byte[]>> storeViews, Map<String, String> changelogByStore,
                Map<TopicPartition, Long> resumeNextRead, int metadataBudgetBytes,
                Producer<byte[], byte[]> producer, Runnable onWrite) {
        this.definition = definition;
        this.partition = partition;
        Map<ChannelId, Long> resumeByChannel = new HashMap<>();
        for (String topic : definition.receivedTopics()) {
            TopicInfo info = topics.get(topic);
            if (partition < info.partitions()) {
                ChannelId channel = new ChannelId(info.topicId(), partition);
                TopicPartition tp = new TopicPartition(topic, partition);
                channelByTopic.put(topic, channel);
                topicByChannel.put(channel, topic);
                partitionByChannel.put(channel, tp);
                Long next = resumeNextRead.get(tp);
                if (next != null) {
                    resumeByChannel.put(channel, next);
                }
            }
        }
        this.orderingStore = new TopicStore(orderingTopic, partition, orderingView, producer, onWrite);
        this.engine = new ProcessEngine(definition.name() + "-" + partition, topicByChannel, orderingStore,
                metadataBudgetBytes, resumeByChannel);

        Map<String, DeliverySeam.ByteStore> byteStores = new HashMap<>();
        for (Store<?, ?> store : definition.stores()) {
            TopicStore topicStore = new TopicStore(changelogByStore.get(store.name()), partition,
                    storeViews.getOrDefault(store.name(), Map.of()), producer, onWrite);
            stores.put(store.name(), topicStore);
            byteStores.put(store.name(), topicStore);
        }
        this.seam = new DeliverySeam(definition, byteStores, changelogByStore, engine::causesHeaderForEmission,
                (topic, key, value, headers, timestamp) -> {
                    onWrite.run();
                    producer.send(new ProducerRecord<>(topic, null, timestamp, key, value, headers));
                });
    }

    int partition() {
        return partition;
    }

    ProcessEngine engine() {
        return engine;
    }

    /** The received partitions this task reads, by channel. */
    Map<ChannelId, TopicPartition> partitions() {
        return partitionByChannel;
    }

    /** Feeds one record and delivers whatever that makes deliverable. */
    void receive(ConsumerRecord<byte[], byte[]> record) {
        ChannelId channel = channelByTopic.get(record.topic());
        if (channel == null) {
            throw new IllegalStateException(definition.name() + " fed from undeclared topic " + record.topic());
        }
        List<HeaderKV> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            headers.add(new HeaderKV(header.key(), header.value()));
        }
        engine.onReceive(new ReceivedMessage(
                channel, record.offset(), record.timestamp(), record.key(), record.value(), headers));
        drain();
    }

    /**
     * Applies the consumer's own read positions: after a poll, the position of a partition
     * asserts that every lower position was returned by a poll or will never arrive as a
     * record, which is exactly the report SPEC Host obligation 2 defines.
     */
    void reportPositions(ToLongFunction<TopicPartition> position) {
        Map<ChannelId, Long> nextRead = new HashMap<>();
        partitionByChannel.forEach((channel, tp) -> nextRead.put(channel, position.applyAsLong(tp)));
        engine.onFacts(new PositionFacts(nextRead, Map.of(), java.util.Set.of()));
        drain();
    }

    /** Applies a round of broker facts gathered through the admin client. */
    void onFacts(PositionFacts facts) {
        engine.onFacts(facts);
        factsAppliedAtNanos = System.nanoTime();
        drain();
    }

    private void drain() {
        while (true) {
            Optional<DeliverableMessage> next = engine.nextDeliverable();
            if (next.isEmpty()) {
                return;
            }
            DeliverableMessage message = next.get();
            engine.markDelivered(message.channel(), message.position());
            seam.deliver(definition.input(topicByChannel.get(message.channel())), message);
        }
    }

    /**
     * The read positions this task commits: the head of each channel's hold-back buffer
     * where something is held, else the consumer's position.
     */
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit(ToLongFunction<TopicPartition> position) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        partitionByChannel.forEach((channel, tp) -> {
            long next = engine.headPosition(channel).orElseGet(() -> position.applyAsLong(tp));
            offsets.put(tp, new OffsetAndMetadata(next, ClientRuntime.OFFSET_STAMP));
        });
        return offsets;
    }

    /** Sends every staged state write into the open transaction. */
    void flushWrites() {
        orderingStore.flush();
        stores.values().forEach(TopicStore::flush);
    }

    TaskStatus snapshot() {
        Optional<Duration> sinceLastFacts = factsAppliedAtNanos == 0
                ? Optional.empty()
                : Optional.of(Duration.ofNanos(System.nanoTime() - factsAppliedAtNanos));
        return TaskSnapshots.snapshot(engine, partition, channel -> {
            String topic = topicByChannel.get(channel);
            return topic != null ? topic : channel.toString();
        }, sinceLastFacts);
    }
}
