package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Reads a cluster into a {@link JepsenExport}: topic identities and log starts, every
 * committed record of every declared topic from earliest under {@code read_committed} with
 * headers, and the trace. Also the observations a test or a Jepsen client takes while the
 * run is going: each task's committed read positions bracketed by the trace ends, and the
 * lag that decides quiescence.
 *
 * <p>Nothing here trusts the engine. Identities, offsets and records come from the broker;
 * the trace comes from the trace topic the handlers wrote into their own transactions.
 */
final class JepsenClusterExport {
    private JepsenClusterExport() {
    }

    static Admin admin(String bootstrapServers) {
        return Admin.create(Map.of("bootstrap.servers", bootstrapServers, "request.timeout.ms", "30000"));
    }

    static String groupId(String prefix, String process) {
        return prefix + "-" + process;
    }

    /** Topic identities by name for every topic the declaration names, and the trace topic. */
    static Map<String, TopicDescription> describe(Admin admin, java.util.Collection<String> names) throws Exception {
        return admin.describeTopics(names).allTopicNames().get(60, TimeUnit.SECONDS);
    }

    /**
     * Like {@link #describe}, leaving out the names that no longer exist. A Jepsen run
     * deletes declared topics on purpose, and the dump must still be taken afterwards: the
     * checker learns of the dead incarnation from the fault that killed it, and judges the
     * records that survive.
     */
    static Map<String, TopicDescription> describeExisting(Admin admin, java.util.Collection<String> names)
            throws Exception {
        Map<String, TopicDescription> out = new LinkedHashMap<>();
        for (Map.Entry<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> entry
                : admin.describeTopics(names).topicNameValues().entrySet()) {
            try {
                out.put(entry.getKey(), entry.getValue().get(60, TimeUnit.SECONDS));
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }
        }
        return out;
    }

    static UUID id(TopicDescription description) {
        return new UUID(description.topicId().getMostSignificantBits(), description.topicId().getLeastSignificantBits());
    }

    /**
     * The end of every partition of {@code topic}, keyed by partition as text: the last
     * stable offset under {@code READ_COMMITTED}, the high watermark under
     * {@code READ_UNCOMMITTED}.
     */
    static Map<String, Long> traceEnds(Admin admin, String topic, int partitions, IsolationLevel isolation)
            throws Exception {
        Map<TopicPartition, OffsetSpec> query = new LinkedHashMap<>();
        for (int p = 0; p < partitions; p++) {
            query.put(new TopicPartition(topic, p), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result = admin
                .listOffsets(query, new ListOffsetsOptions(isolation)).all().get(30, TimeUnit.SECONDS);
        Map<String, Long> ends = new LinkedHashMap<>();
        result.forEach((tp, info) -> ends.put(String.valueOf(tp.partition()), info.offset()));
        return ends;
    }

    /** The last stable offset of every trace partition: everything below it is committed or aborted. */
    static Map<String, Long> traceEnds(Admin admin, String topic, int partitions) throws Exception {
        return traceEnds(admin, topic, partitions, IsolationLevel.READ_COMMITTED);
    }

    static Map<TopicPartition, Long> offsets(Admin admin, java.util.Collection<TopicPartition> partitions,
                                             OffsetSpec spec) throws Exception {
        return offsets(admin, partitions, spec, IsolationLevel.READ_COMMITTED);
    }

    static Map<TopicPartition, Long> offsets(Admin admin, java.util.Collection<TopicPartition> partitions,
                                             OffsetSpec spec, IsolationLevel isolation) throws Exception {
        Map<TopicPartition, OffsetSpec> query = new LinkedHashMap<>();
        for (TopicPartition tp : partitions) {
            query.put(tp, spec);
        }
        Map<TopicPartition, Long> out = new LinkedHashMap<>();
        admin.listOffsets(query, new ListOffsetsOptions(isolation)).all()
                .get(30, TimeUnit.SECONDS).forEach((tp, info) -> out.put(tp, info.offset()));
        return out;
    }

    /** The group's committed positions, empty when the group does not exist yet. */
    static Map<TopicPartition, Long> committed(Admin admin, String groupId) throws Exception {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
            Map<TopicPartition, Long> out = new LinkedHashMap<>();
            committed.forEach((tp, om) -> {
                if (om != null) {
                    out.put(tp, om.offset());
                }
            });
            return out;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                return Map.of();
            }
            throw e;
        }
    }

    /**
     * One observation of every task's committed read positions, bracketed by the trace
     * ends ({@link JepsenExport} says what the two bound). {@code endsLo} is the last stable
     * offset read before the group offsets: a trace record below it was resolved, so its
     * step's receipts are within the positions read after. {@code endsHi} is the high
     * watermark read after the group offsets: a trace record at or past it was not yet
     * acknowledged, so under {@code acks=all} its step had not committed when the positions
     * were read, and everything those positions cover was received before that step. Both
     * hold with several tasks sharing a trace partition and with a fenced zombie's open
     * transaction on it, which a last-stable-offset bracket on the far side would not.
     */
    static List<JepsenExport.Reads> observeReads(Admin admin, String prefix,
                                                 Map<String, JepsenExport.ProcessDecl> declaration,
                                                 Map<String, UUID> topicIds, String traceTopic, int tracePartitions,
                                                 long index, Long time) throws Exception {
        Map<String, Long> lo = traceEnds(admin, traceTopic, tracePartitions, IsolationLevel.READ_COMMITTED);
        Map<String, Map<TopicPartition, Long>> byProcess = new LinkedHashMap<>();
        for (String process : declaration.keySet()) {
            byProcess.put(process, committed(admin, groupId(prefix, process)));
        }
        Map<String, Long> hi = traceEnds(admin, traceTopic, tracePartitions, IsolationLevel.READ_UNCOMMITTED);
        List<JepsenExport.Reads> reads = new ArrayList<>();
        byProcess.forEach((process, committed) -> {
            Map<Integer, Map<Channel, Long>> byTask = new java.util.TreeMap<>();
            committed.forEach((tp, offset) -> {
                UUID id = topicIds.get(tp.topic());
                if (id != null) {
                    byTask.computeIfAbsent(tp.partition(), p -> new LinkedHashMap<>())
                            .put(new Channel(id, tp.partition()), offset);
                }
            });
            byTask.forEach((task, nextRead) ->
                    reads.add(new JepsenExport.Reads(index, time, process, task, lo, hi, nextRead, Map.of())));
        });
        return reads;
    }

    /** Status entries for one snapshot of {@link Parsley#status()}. */
    static List<JepsenExport.Status> statuses(Map<String, ProcessStatus> status, Map<String, Long> traceEnds,
                                              long index, Long time) {
        List<JepsenExport.Status> out = new ArrayList<>();
        status.forEach((process, s) -> out.add(new JepsenExport.Status(index, time, process, s.lifecycle(),
                s.refusalReason().orElse(null), s.failureDetail().orElse(null), traceEnds)));
        return out;
    }

    /**
     * The lag of every received partition of every process, as text lines, empty when every
     * process has committed reading to the end of each received partition. Processes in
     * {@code refused} are left out: a refusal is terminal, and its lag is expected.
     */
    static List<String> lag(Admin admin, String prefix, Map<String, JepsenExport.ProcessDecl> declaration,
                            Map<String, TopicDescription> topics, java.util.Set<String> refused) throws Exception {
        List<String> lagging = new ArrayList<>();
        for (JepsenExport.ProcessDecl decl : declaration.values()) {
            if (refused.contains(decl.name())) {
                continue;
            }
            List<TopicPartition> received = new ArrayList<>();
            for (String topic : decl.receives()) {
                TopicDescription description = topics.get(topic);
                for (int p = 0; p < description.partitions().size(); p++) {
                    received.add(new TopicPartition(topic, p));
                }
            }
            Map<TopicPartition, Long> ends = offsets(admin, received, OffsetSpec.latest());
            Map<TopicPartition, Long> committed = committed(admin, groupId(prefix, decl.name()));
            for (TopicPartition tp : received) {
                long end = ends.getOrDefault(tp, 0L);
                Long position = committed.get(tp);
                if (position == null ? end > 0 : position < end) {
                    lagging.add(decl.name() + " " + tp + " committed " + position + " of " + end);
                }
            }
        }
        return lagging;
    }

    /**
     * Dumps every declared topic and the trace into an export. {@code stampedFrom} names,
     * per uid, the record an external stamped send observed, as the producer client
     * recorded it.
     */
    static JepsenExport dump(String bootstrapServers, Map<String, JepsenExport.ProcessDecl> declaration,
                             String traceTopic, Map<String, JepsenExport.Position> stampedFrom) throws Exception {
        JepsenExport export = new JepsenExport();
        export.source = JepsenExport.Source.CLUSTER;
        export.traceTopic = traceTopic;
        export.processes.putAll(declaration);
        java.util.Set<String> names = new java.util.LinkedHashSet<>();
        declaration.values().forEach(decl -> {
            names.addAll(decl.receives());
            names.addAll(decl.sends());
        });
        names.add(traceTopic);
        try (Admin admin = admin(bootstrapServers)) {
            Map<String, TopicDescription> topics = describeExisting(admin, names);
            List<TopicPartition> partitions = new ArrayList<>();
            topics.forEach((name, description) -> {
                for (int p = 0; p < description.partitions().size(); p++) {
                    partitions.add(new TopicPartition(name, p));
                }
            });
            Map<TopicPartition, Long> logStarts = offsets(admin, partitions, OffsetSpec.earliest());
            Map<TopicPartition, Long> ends = offsets(admin, partitions, OffsetSpec.latest());
            // The high watermark counts every assigned offset, markers and aborted records included.
            Map<TopicPartition, Long> logEnds = offsets(admin, partitions, OffsetSpec.latest(),
                    IsolationLevel.READ_UNCOMMITTED);
            topics.forEach((name, description) -> {
                Map<Integer, Long> starts = new LinkedHashMap<>();
                Map<Integer, Long> logEnd = new LinkedHashMap<>();
                for (int p = 0; p < description.partitions().size(); p++) {
                    starts.put(p, logStarts.getOrDefault(new TopicPartition(name, p), 0L));
                    logEnd.put(p, logEnds.getOrDefault(new TopicPartition(name, p), 0L));
                }
                export.topics.add(new JepsenExport.TopicInfo(id(description), name, description.partitions().size(),
                        true, starts, logEnd));
            });
            Map<String, UUID> ids = new HashMap<>();
            topics.forEach((name, description) -> ids.put(name, id(description)));

            for (ConsumerRecord<byte[], byte[]> record : readAll(bootstrapServers, partitions, logStarts, ends)) {
                byte[] causes = causesHeader(record);
                String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
                String value = record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
                if (record.topic().equals(traceTopic)) {
                    JepsenTopology.TraceValue trace = JepsenTopology.parseTraceValue(value);
                    export.trace.add(new JepsenExport.TraceEntry(trace.process(), trace.task(),
                            String.valueOf(record.partition()), record.offset(), trace.topicId(), trace.partition(),
                            trace.offset(), trace.uid(), causes, trace.effects()));
                } else {
                    export.records.add(new JepsenExport.Rec(ids.get(record.topic()), record.partition(),
                            record.offset(), key, value, value, causes, stampedFrom.get(value)));
                }
            }
        }
        export.records.sort(Comparator.comparing((JepsenExport.Rec r) -> r.topicId().toString())
                .thenComparingInt(JepsenExport.Rec::partition).thenComparingLong(JepsenExport.Rec::offset));
        export.trace.sort(Comparator.comparing(JepsenExport.TraceEntry::tp).thenComparingLong(JepsenExport.TraceEntry::to));
        return export;
    }

    private static byte[] causesHeader(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().lastHeader(CausesCodec.HEADER_KEY);
        if (header == null) {
            return null;
        }
        return header.value() == null ? JepsenExport.NULL_HEADER_VALUE : header.value();
    }

    /**
     * Every committed record from the log start to the last stable offset of each
     * partition, in one pass with an unsubscribed consumer.
     */
    static List<ConsumerRecord<byte[], byte[]>> readAll(String bootstrapServers, List<TopicPartition> partitions,
                                                        Map<TopicPartition, Long> logStarts,
                                                        Map<TopicPartition, Long> ends) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(),
                new ByteArrayDeserializer())) {
            List<TopicPartition> pending = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                if (ends.getOrDefault(tp, 0L) > logStarts.getOrDefault(tp, 0L)) {
                    pending.add(tp);
                }
            }
            if (pending.isEmpty()) {
                return records;
            }
            consumer.assign(pending);
            consumer.seekToBeginning(pending);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (!pending.isEmpty()) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("dump did not reach the end of " + pending);
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(records::add);
                pending.removeIf(tp -> consumer.position(tp) >= ends.get(tp));
            }
        }
        return records;
    }
}
