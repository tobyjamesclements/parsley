package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Jepsen harness topology against the embedded KRaft broker, on two instances
 * with a restart in the middle, dumps the cluster into the export and judges it with the
 * {@link Oracle} replay: the first checker over a real trace. Negative controls on the
 * same export show the replay would have caught an inversion, a lost delivery and a
 * zombie's commit, so a clean verdict is evidence of something.
 */
@Timeout(value = 900, unit = TimeUnit.SECONDS)
class JepsenHarnessIntegrationTest {
    private static final int PARTITIONS = 3;
    private static final String PREFIX = "jep";

    private KafkaClusterTestKit cluster;
    private Admin admin;
    private KafkaProducer<String, String> producer;
    private Map<String, TopicDescription> topics;
    private Map<String, UUID> topicIds;
    private final Map<String, JepsenExport.Position> stampedFrom = new LinkedHashMap<>();
    private final List<JepsenExport.Reads> reads = new ArrayList<>();
    private final List<JepsenExport.Status> statuses = new ArrayList<>();
    private final List<JepsenExport.Fault> faults = new ArrayList<>();
    private final AtomicLong index = new AtomicLong();

    @TempDir
    Path stateDir;

    @BeforeEach
    void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of("log.retention.check.interval.ms", "500"));
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
        JepsenTopology.createTopics(admin, PARTITIONS, (short) 1, Map.of());
        topics = JepsenClusterExport.describe(admin, JepsenTopology.TOPICS);
        topicIds = new HashMap<>();
        topics.forEach((name, description) -> topicIds.put(name, JepsenClusterExport.id(description)));
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    void stopCluster() throws Exception {
        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }
        ClusterTestSupport.stopCluster(cluster, admin);
    }

    private ParsleyConfig config(String instance) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), PREFIX)
                .stateDir(stateDir.resolve(instance).toString())
                .build();
    }

    private Parsley start(String instance) {
        return Parsley.start(config(instance), JepsenTopology.processes(Set.of()).toArray(new Process[0]));
    }

    /** Produces one external record, acknowledged, and returns where it landed. */
    private JepsenExport.Position produce(String topic, Integer partition, String uid, RecordHeader... headers)
            throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, uid, uid);
        for (RecordHeader header : headers) {
            record.headers().add(header);
        }
        RecordMetadata metadata = producer.send(record).get(30, TimeUnit.SECONDS);
        return new JepsenExport.Position(topicIds.get(topic), metadata.partition(), metadata.offset());
    }

    private static RecordHeader causesHeader(Map<Channel, Long> causes) {
        return new RecordHeader(CausesCodec.HEADER_KEY, CausesCodec.encode(Causes.of(causes)));
    }

    private Map<String, Long> traceEnds() throws Exception {
        return JepsenClusterExport.traceEnds(admin, JepsenTopology.TRACE, PARTITIONS);
    }

    /** One observation: every task's committed positions bracketed by trace ends, and every instance's status. */
    private void observe(List<Parsley> instances) throws Exception {
        long at = index.getAndIncrement();
        long now = System.currentTimeMillis();
        reads.addAll(JepsenClusterExport.observeReads(admin, PREFIX, JepsenTopology.declaration(Set.of()), topicIds,
                JepsenTopology.TRACE, PARTITIONS, at, now));
        Map<String, Long> ends = traceEnds();
        for (Parsley instance : instances) {
            statuses.addAll(JepsenClusterExport.statuses(instance.status(), ends, index.getAndIncrement(), now));
        }
    }

    private void fault(String kind, Map<String, Object> details, Set<FailClosedException.Reason> justifies)
            throws Exception {
        Map<Object, Object> edn = new LinkedHashMap<>();
        details.forEach((key, value) -> edn.put(JepsenEdn.kw(key), value));
        faults.add(new JepsenExport.Fault(index.getAndIncrement(), System.currentTimeMillis(), kind,
                new LinkedHashSet<>(justifies), traceEnds(), edn));
    }

    private Set<String> refusedProcesses(List<Parsley> instances) {
        Set<String> refused = new LinkedHashSet<>();
        for (Parsley instance : instances) {
            instance.status().forEach((name, status) -> {
                if (status.refused()) {
                    refused.add(name);
                }
            });
        }
        return refused;
    }

    /**
     * Waits until every running process has committed reading to the end of each received
     * partition and the trace has stopped growing, observing all the while.
     */
    private void quiesce(List<Parsley> instances) throws Exception {
        Map<String, Long> previous = null;
        int stable = 0;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(300);
        while (stable < 3) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the run did not quiesce: " + JepsenClusterExport.lag(admin, PREFIX,
                        JepsenTopology.declaration(Set.of()), topics, refusedProcesses(instances)));
            }
            Thread.sleep(2_000);
            observe(instances);
            List<String> lag = JepsenClusterExport.lag(admin, PREFIX, JepsenTopology.declaration(Set.of()), topics,
                    refusedProcesses(instances));
            Map<String, Long> ends = traceEnds();
            stable = lag.isEmpty() && ends.equals(previous) ? stable + 1 : 0;
            previous = ends;
        }
    }

    private JepsenExport export() throws Exception {
        JepsenExport export = JepsenClusterExport.dump(cluster.bootstrapServers(), JepsenTopology.declaration(Set.of()),
                JepsenTopology.TRACE, stampedFrom);
        export.reads.addAll(reads);
        export.statuses.addAll(statuses);
        export.faults.addAll(faults);
        JepsenTopology.declaration(Set.of()).forEach((process, decl) -> {
            for (int p = 0; p < PARTITIONS; p++) {
                List<Channel> receives = new ArrayList<>();
                Map<Channel, Long> starts = new LinkedHashMap<>();
                for (String topic : decl.receives()) {
                    Channel channel = new Channel(topicIds.get(topic), p);
                    receives.add(channel);
                    starts.put(channel, 0L);
                }
                export.tasks.add(new JepsenExport.TaskInfo(process, p, receives));
                export.startPositions.add(new JepsenExport.StartPositions(process, p, starts));
            }
        });
        return export;
    }

    /**
     * Two instances, external unstamped and stamped records, an out-of-contract stamp that
     * holds until the channel's next record, an instance killed with its state wiped and
     * restarted so its tasks migrate and restore from the changelog: the export replays
     * clean, and the negative controls do not.
     */
    @Test
    void honestRunReplaysCleanAndNegativeControlsDoNot() throws Exception {
        Parsley first = start("first");
        Parsley second = start("second");
        List<Parsley> instances = new ArrayList<>(List.of(first, second));
        try {
            List<JepsenExport.Position> acked = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                acked.add(produce(JepsenTopology.SRC, null, "x" + i));
                if (i % 3 == 2) {
                    observe(instances);
                }
            }
            /*
             * Stamped sends, as the simulator's external producers make them: a record on b
             * naming a record the client had already observed committed on a, placed on the
             * same partition so the joiner task for it must deliver the cause first.
             */
            for (int i = 0; i < 6; i++) {
                JepsenExport.Position cause = produce(JepsenTopology.A, i % PARTITIONS, "ea" + i);
                String uid = "sb" + i;
                stampedFrom.put(uid, cause);
                produce(JepsenTopology.B, cause.partition(), uid,
                        causesHeader(Map.of(cause.channel(), cause.offset())));
            }
            observe(instances);

            // The first instance dies with its state; its tasks migrate, then it returns.
            fault("kill-instance", Map.of("instance", "first", "wipe", true), Set.of());
            first.close();
            instances.remove(first);
            deleteRecursively(stateDir.resolve("first"));
            for (int i = 12; i < 24; i++) {
                acked.add(produce(JepsenTopology.SRC, null, "x" + i));
            }
            observe(instances);
            first = start("first");
            instances.add(first);
            fault("restart-instance", Map.of("instance", "first"), Set.of());

            /*
             * An out-of-contract stamp: a record on b naming a's log-end offset, which no
             * committed record occupies yet (wire-format constraint 8). It is held, not
             * refused, until the next record on that partition of a settles the position.
             */
            TopicPartition aEnd = new TopicPartition(JepsenTopology.A, 0);
            long logEnd = JepsenClusterExport.offsets(admin, List.of(aEnd),
                    org.apache.kafka.clients.admin.OffsetSpec.latest()).get(aEnd);
            produce(JepsenTopology.B, 0, "sb-late", causesHeader(Map.of(new Channel(topicIds.get(JepsenTopology.A), 0), logEnd)));
            fault("out-of-contract-stamp", Map.of("channel", JepsenExport.channel(new Channel(topicIds.get(JepsenTopology.A), 0)),
                    "position", logEnd), Set.of());
            for (int i = 24; i < 30; i++) {
                acked.add(produce(JepsenTopology.SRC, null, "x" + i));
            }
            observe(instances);
            Thread.sleep(5_000);
            observe(instances);
            // The next record on a@0 settles the out-of-contract position.
            produce(JepsenTopology.A, 0, "ea-settle");

            quiesce(instances);
            Parsley restarted = first;
            assertTrue(restarted.healthy() && second.healthy(), () -> "every process must still be running: "
                    + restarted.status() + " " + second.status());

            String status = statusOverHttp(restarted);
            assertTrue(status.contains(":healthy true"), () -> "the status endpoint must serve EDN: " + status);
            assertTrue(status.contains("\"joiner\""), () -> "the status must list every process: " + status);
        } finally {
            for (Parsley instance : instances) {
                instance.close();
            }
        }
        observe(List.of());

        JepsenExport export = export();
        export.writeTo(Path.of("target", "jepsen-harness-run.edn"));
        assertFalse(export.trace.isEmpty(), "the trace must carry the run's deliveries");
        assertTrue(export.records.stream().anyMatch(r -> r.uid() != null && JepsenTopology.hops(r.uid()) >= 3),
                "the topology must forward across several hops");
        JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(export);
        assertEquals(List.of(), verdict.violations(), "the honest run must replay clean");

        JepsenExport reread = JepsenExport.read(export.write());
        assertEquals(List.of(), JepsenExportOracle.check(reread).violations(), "and so must its EDN round trip");

        // Negative control: two deliveries of one task swapped, an effect now before its cause.
        JepsenExport inverted = JepsenExport.read(export.write());
        assertTrue(invertOnePair(inverted), "the run must contain a task with two deliveries to invert");
        assertTrue(JepsenExportOracle.check(inverted).violations().stream()
                        .anyMatch(v -> v.startsWith("Safety 1") || v.startsWith("Safety 3")),
                () -> "an inversion must be caught: " + JepsenExportOracle.check(inverted).violations());

        // Negative control: one delivery erased from the trace, its effects orphaned.
        JepsenExport lost = JepsenExport.read(export.write());
        JepsenExport.TraceEntry erased = lost.trace.stream().filter(e -> !e.effects().isEmpty()).findFirst().orElseThrow();
        lost.trace.remove(erased);
        List<String> lostViolations = JepsenExportOracle.check(lost).violations();
        assertTrue(lostViolations.stream().anyMatch(v -> v.startsWith("Liveness 1") || v.startsWith("Host obligation 3")),
                () -> "a lost delivery must be caught: " + lostViolations);

        // Negative control: an effect committed twice, as a superseded execution's step would.
        JepsenExport zombie = JepsenExport.read(export.write());
        JepsenExport.Rec duplicated = zombie.records.stream().filter(r -> r.uid().contains(">")).findFirst().orElseThrow();
        zombie.records.add(new JepsenExport.Rec(duplicated.topicId(), duplicated.partition(), duplicated.offset() + 100_000,
                duplicated.key(), duplicated.value(), duplicated.uid(), duplicated.causesHeader(), null));
        assertTrue(JepsenExportOracle.check(zombie).violations().stream().anyMatch(v -> v.startsWith("Host obligation 6")),
                () -> "a zombie's commit must be caught: " + JepsenExportOracle.check(zombie).violations());
    }

    /**
     * A malformed {@code parsley.causes} header stops the receiving process with
     * {@code UNDECODABLE_METADATA}, nothing delivers past it, and the replay accepts the
     * refusal as justified by the injected fault while still judging the rest of the run.
     */
    @Test
    void malformedHeaderStopsTheProcessAndTheReplayAcceptsTheJustifiedRefusal() throws Exception {
        Parsley only = start("only");
        List<Parsley> instances = List.of(only);
        try {
            for (int i = 0; i < 6; i++) {
                produce(JepsenTopology.SRC, null, "x" + i);
            }
            quiesce(instances);
            fault("corrupt", Map.of("channel", JepsenExport.channel(new Channel(topicIds.get(JepsenTopology.SRC), 1))),
                    Set.of(FailClosedException.Reason.UNDECODABLE_METADATA));
            produce(JepsenTopology.SRC, 1, "garbage", new RecordHeader(CausesCodec.HEADER_KEY, new byte[] {99, 1, 2, 3}));
            produce(JepsenTopology.SRC, 1, "x-after-garbage");
            ClusterTestSupport.await("the splitter to refuse the undecodable header",
                    () -> only.status().get(JepsenTopology.SPLITTER).refused(), Duration.ofSeconds(120));
            observe(instances);
            assertEquals(FailClosedException.Reason.UNDECODABLE_METADATA,
                    only.status().get(JepsenTopology.SPLITTER).refusalReason().orElseThrow());
            quiesce(instances);
        } finally {
            only.close();
        }
        JepsenExport export = export();
        JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(export);
        assertEquals(List.of(), verdict.violations(), "a justified refusal is not a violation");
        assertFalse(export.records.stream().anyMatch(r -> "x-after-garbage>splitter>a".equals(r.uid())),
                "nothing may be delivered past the undecodable header");

        JepsenExport unjustified = JepsenExport.read(export.write());
        unjustified.faults.removeIf(f -> f.kind().equals("corrupt"));
        assertTrue(JepsenExportOracle.check(unjustified).violations().stream().anyMatch(v -> v.startsWith("Operational")),
                "a refusal no fault justifies must be flagged");
    }

    /**
     * A dump taken after a declared topic was deleted, as the Jepsen test's topic faults
     * leave the cluster, lists the topics that remain and none of the deleted one's records,
     * rather than failing on the name that no longer resolves.
     */
    @Test
    void dumpLeavesOutADeletedTopic() throws Exception {
        produce(JepsenTopology.SELF, 0, "held-self");
        produce(JepsenTopology.SRC, null, "x0");
        admin.deleteTopics(List.of(JepsenTopology.SELF)).all().get(60, TimeUnit.SECONDS);
        ClusterTestSupport.await("the topic to be gone",
                () -> {
                    try {
                        return !admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(JepsenTopology.SELF);
                    } catch (Exception e) {
                        return false;
                    }
                }, Duration.ofSeconds(60));

        JepsenExport export = JepsenClusterExport.dump(cluster.bootstrapServers(), JepsenTopology.declaration(Set.of()),
                JepsenTopology.TRACE, Map.of());
        assertFalse(export.topics.stream().anyMatch(t -> t.name().equals(JepsenTopology.SELF)),
                "the deleted topic must not be listed as alive");
        assertEquals(JepsenTopology.TOPICS.size() - 1, export.topics.size(), "every other topic must be listed");
        assertTrue(export.records.stream().anyMatch(r -> "x0".equals(r.uid())), "the surviving records must be dumped");
        assertFalse(export.records.stream().anyMatch(r -> "held-self".equals(r.uid())),
                "nothing can be dumped from the deleted topic");
    }

    /**
     * Swaps two deliveries of one task so an effect precedes what caused it: a delivery and a
     * later one whose uid descends from it, or failing that two deliveries from one channel,
     * which then arrive out of position order. Either is a violation the replay must see.
     */
    private static boolean invertOnePair(JepsenExport export) {
        Map<String, List<JepsenExport.TraceEntry>> byTask = new LinkedHashMap<>();
        for (JepsenExport.TraceEntry entry : export.trace) {
            byTask.computeIfAbsent(entry.taskName(), t -> new ArrayList<>()).add(entry);
        }
        for (List<JepsenExport.TraceEntry> entries : byTask.values()) {
            entries.sort(java.util.Comparator.comparingLong(JepsenExport.TraceEntry::to));
            for (int i = 0; i < entries.size(); i++) {
                for (int j = i + 1; j < entries.size(); j++) {
                    if (entries.get(j).uid().startsWith(entries.get(i).uid() + ">")) {
                        swap(export, entries.get(i), entries.get(j));
                        return true;
                    }
                }
            }
        }
        for (List<JepsenExport.TraceEntry> entries : byTask.values()) {
            for (int i = 0; i < entries.size(); i++) {
                for (int j = i + 1; j < entries.size(); j++) {
                    if (entries.get(i).channel().equals(entries.get(j).channel())) {
                        swap(export, entries.get(i), entries.get(j));
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static void swap(JepsenExport export, JepsenExport.TraceEntry first, JepsenExport.TraceEntry second) {
        export.trace.remove(first);
        export.trace.remove(second);
        export.trace.add(new JepsenExport.TraceEntry(first.process(), first.task(), first.tp(), second.to(),
                first.topicId(), first.partition(), first.offset(), first.uid(), first.causesHeader(), first.effects()));
        export.trace.add(new JepsenExport.TraceEntry(second.process(), second.task(), second.tp(), first.to(),
                second.topicId(), second.partition(), second.offset(), second.uid(), second.causesHeader(),
                second.effects()));
    }

    private static String statusOverHttp(Parsley parsley) throws Exception {
        JepsenHarness.StatusView view = new JepsenHarness.StatusView();
        view.running.add(parsley);
        var server = JepsenHarness.serveStatus(0, view);
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/status")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertNotNull(JepsenEdn.read(response.body()), "the status must be EDN");
            return response.body();
        } finally {
            server.stop(0);
        }
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (!java.nio.file.Files.exists(root)) {
            return;
        }
        try (var paths = java.nio.file.Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.delete(path);
            }
        }
    }
}
