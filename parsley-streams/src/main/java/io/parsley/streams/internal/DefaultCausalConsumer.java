package io.parsley.streams.internal;

import io.parsley.BufferingPolicy;
import io.parsley.FenceToken;
import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.buffer.CausalViolationHandler;
import io.parsley.internal.ImmutableVectorClock;
import io.parsley.streams.CausalConsumer;
import io.parsley.streams.CausalProcessorSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultCausalConsumer<K, V> implements CausalConsumer<K, V> {

    private final KafkaStreams streams;
    private final LinkedBlockingQueue<ConsumerRecord<K, V>> readyQueue = new LinkedBlockingQueue<>();
    private final AtomicReference<ImmutableVectorClock> frontierRef =
            new AtomicReference<>(ImmutableVectorClock.empty());
    private final VectorClockSerialiser serialiser;

    public DefaultCausalConsumer(
            Collection<String> topics,
            BufferingPolicy policy,
            Map<String, Object> consumerConfig,
            Map<String, Object> streamsConfig) {

        serialiser = ServiceLoader.load(VectorClockSerialiser.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No VectorClockSerialiser on classpath — add parsley-serialisation"));

        CausalProcessorSupplier<K, V> causalSupplier =
                new CausalProcessorSupplier<>(policy, CausalViolationHandler.noop(), serialiser);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<K, V> stream = builder.stream(topics);
        stream.process(causalSupplier)
              .process(captureSupplier(), CausalProcessor.FRONTIER_STORE);

        Map<String, Object> merged = new HashMap<>(streamsConfig);
        merged.putAll(consumerConfig);
        org.apache.kafka.streams.StreamsConfig config =
                new org.apache.kafka.streams.StreamsConfig(merged);
        this.streams = new KafkaStreams(builder.build(), config);
        this.streams.start();
    }

    private ProcessorSupplier<K, V, Void, Void> captureSupplier() {
        return () -> new Processor<>() {
            private ProcessorContext<Void, Void> ctx;

            @Override
            public void init(ProcessorContext<Void, Void> context) {
                this.ctx = context;
            }

            @Override
            public void process(Record<K, V> record) {
                String topic = extractStringHeader(record, CausalProcessor.SOURCE_TOPIC_HEADER);
                int partition = extractIntHeader(record, CausalProcessor.SOURCE_PARTITION_HEADER);
                long offset = extractLongHeader(record, CausalProcessor.SOURCE_OFFSET_HEADER);

                ConsumerRecord<K, V> cr = new ConsumerRecord<>(
                        topic.isEmpty() ? "unknown" : topic,
                        partition, offset,
                        record.timestamp(), TimestampType.CREATE_TIME,
                        -1, -1,
                        record.key(), record.value(),
                        record.headers(), Optional.empty());

                readyQueue.add(cr);

                // update frontier from the frontier store maintained by CausalProcessor
                byte[] frontierBytes = ctx.<org.apache.kafka.streams.state.KeyValueStore<String, byte[]>>
                        getStateStore(CausalProcessor.FRONTIER_STORE)
                        .get(CausalProcessor.FRONTIER_KEY);
                if (frontierBytes != null) {
                    frontierRef.set((ImmutableVectorClock) serialiser.deserialise(frontierBytes));
                }
            }
        };
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> byPartition = new LinkedHashMap<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long remainingMillis = timeout.toMillis();

        try {
            ConsumerRecord<K, V> first = readyQueue.poll(remainingMillis, TimeUnit.MILLISECONDS);
            if (first != null) {
                accumulate(byPartition, first);
                List<ConsumerRecord<K, V>> drained = new ArrayList<>();
                readyQueue.drainTo(drained);
                drained.forEach(r -> accumulate(byPartition, r));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new ConsumerRecords<>(byPartition);
    }

    private void accumulate(Map<TopicPartition, List<ConsumerRecord<K, V>>> map, ConsumerRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        map.computeIfAbsent(tp, k -> new ArrayList<>()).add(record);
    }

    @Override
    public VectorClock frontier() {
        return frontierRef.get();
    }

    @Override
    public FenceToken fenceToken() {
        return FenceToken.of(frontierRef.get());
    }

    @Override
    public void close() {
        streams.close();
    }

    private static String extractStringHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static int extractIntHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? 0 : CausalProcessor.bytesToInt(h.value());
    }

    private static long extractLongHeader(Record<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? 0L : CausalProcessor.bytesToLong(h.value());
    }
}
