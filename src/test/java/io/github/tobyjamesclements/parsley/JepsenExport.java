package io.github.tobyjamesclements.parsley;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static io.github.tobyjamesclements.parsley.JepsenEdn.asList;
import static io.github.tobyjamesclements.parsley.JepsenEdn.asMap;
import static io.github.tobyjamesclements.parsley.JepsenEdn.get;
import static io.github.tobyjamesclements.parsley.JepsenEdn.integer;
import static io.github.tobyjamesclements.parsley.JepsenEdn.keywordName;
import static io.github.tobyjamesclements.parsley.JepsenEdn.kw;
import static io.github.tobyjamesclements.parsley.JepsenEdn.map;
import static io.github.tobyjamesclements.parsley.JepsenEdn.optionalInteger;
import static io.github.tobyjamesclements.parsley.JepsenEdn.string;

/**
 * Everything a checker needs to judge one run, in the one shape the Jepsen test and the
 * simulator both produce: the declaration, topic identities, the final dump of every
 * topic, the trace, the observations of each task's committed read positions, the status
 * history and the injected faults.
 *
 * <p>Written as EDN so the Clojure checker in {@code parsley-jepsen} reads it directly
 * ({@code test/resources/exports} there is produced by {@link JepsenSimulatorExport}).
 * {@link JepsenExportOracle} replays the same structure through the {@link Oracle}.
 *
 * <p>Ordering between events and trace entries never relies on a clock. Every trace entry
 * carries its trace partition {@code tp} and offset {@code to}; every observation carries
 * the trace end offsets it saw, so "this observation happened before that step" is decided
 * from offsets the substrate assigned. A read observation records the trace ends twice:
 * {@code endsLo}, the last stable offset read before the group's positions, and
 * {@code endsHi}, the high watermark read after them. The positions are a lower bound on
 * what a task had received before any step at or past {@code endsHi}, and an upper bound
 * for any step below {@code endsLo}. In the simulator both are the trace ends at the commit.
 */
final class JepsenExport {
    static final long FORMAT = 1;

    /** Marks a {@code parsley.causes} header that was present with a null value. */
    static final byte[] NULL_HEADER_VALUE = new byte[0];

    enum Source { CLUSTER, SIMULATOR }

    /**
     * A topic incarnation. {@code logStart} is the earliest retained offset of each
     * partition and {@code logEnd} the first offset never assigned, both as the dump found
     * them: the last assigned position, which Structural 12 is judged against, is
     * {@code logEnd - 1}, and it counts the offsets that transaction markers and aborted
     * records took, which no committed record occupies. On a cluster a frontier names such
     * a position whenever an out-of-contract stamp was settled by the channel moving past it.
     */
    record TopicInfo(UUID id, String name, int partitions, boolean alive, Map<Integer, Long> logStart,
                     Map<Integer, Long> logEnd) {
    }

    record ProcessDecl(String name, List<String> receives, List<String> sends) {
    }

    record Position(UUID topicId, int partition, long offset) {
        Channel channel() {
            return new Channel(topicId, partition);
        }
    }

    record Rec(UUID topicId, int partition, long offset, String key, String value, String uid,
               byte[] causesHeader, Position stampedFrom) {
        Channel channel() {
            return new Channel(topicId, partition);
        }
    }

    record Effect(String topic, String uid) {
    }

    record TraceEntry(String process, int task, String tp, long to, UUID topicId, int partition, long offset,
                      String uid, byte[] causesHeader, List<Effect> effects) {
        Channel channel() {
            return new Channel(topicId, partition);
        }

        String taskName() {
            return JepsenExport.taskName(process, task);
        }
    }

    /**
     * One observation of a task's committed read positions. {@code execStart} is where the
     * execution that committed them began reading each channel, so the records the task
     * received are those in {@code [execStart, nextRead)}; empty means the task's recorded
     * start positions apply, as on a cluster where every execution resumes where the last
     * committed.
     */
    record Reads(long index, Long time, String process, int task, Map<String, Long> endsLo,
                 Map<String, Long> endsHi, Map<Channel, Long> nextRead, Map<Channel, Long> execStart) {
        String taskName() {
            return JepsenExport.taskName(process, task);
        }
    }

    record Status(long index, Long time, String process, ProcessStatus.Lifecycle lifecycle,
                  FailClosedException.Reason refusal, String detail, Map<String, Long> traceEnds) {
    }

    record Fault(long index, Long time, String kind, Set<FailClosedException.Reason> justifies,
                 Map<String, Long> traceEnds, Map<Object, Object> details) {
    }

    record StartPositions(String process, int task, Map<Channel, Long> positions) {
        String taskName() {
            return JepsenExport.taskName(process, task);
        }
    }

    /**
     * One task and the channels it receives: partition {@code task} of each received topic
     * on a cluster, an arbitrary channel set in the simulator. A change of declaration
     * during the run is a {@code declared} fault carrying the new set.
     */
    record TaskInfo(String process, int task, List<Channel> receives) {
        String taskName() {
            return JepsenExport.taskName(process, task);
        }
    }

    static String taskName(String process, int task) {
        return process + "-" + task;
    }

    Source source = Source.CLUSTER;
    Long seed;
    String sabotage;
    String hostFault;
    String traceTopic;
    final Map<String, ProcessDecl> processes = new LinkedHashMap<>();
    final List<TopicInfo> topics = new ArrayList<>();
    final List<TaskInfo> tasks = new ArrayList<>();
    final List<StartPositions> startPositions = new ArrayList<>();
    final List<Rec> records = new ArrayList<>();
    final List<TraceEntry> trace = new ArrayList<>();
    final List<Reads> reads = new ArrayList<>();
    final List<Status> statuses = new ArrayList<>();
    final List<Fault> faults = new ArrayList<>();

    // ---- EDN ----

    Map<Object, Object> toEdn() {
        Map<Object, Object> out = new LinkedHashMap<>();
        out.put(kw("format"), FORMAT);
        out.put(kw("source"), kw(source.name().toLowerCase()));
        out.put(kw("seed"), seed);
        out.put(kw("sabotage"), sabotage == null ? null : kw(sabotage));
        out.put(kw("host-fault"), hostFault == null ? null : kw(hostFault));
        out.put(kw("trace-topic"), traceTopic);
        Map<Object, Object> decl = new LinkedHashMap<>();
        processes.forEach((name, p) -> decl.put(name, map("receives", p.receives(), "sends", p.sends())));
        out.put(kw("processes"), decl);
        List<Object> topicList = new ArrayList<>();
        for (TopicInfo t : topics) {
            Map<Object, Object> starts = new LinkedHashMap<>();
            t.logStart().forEach((partition, start) -> starts.put((long) partition, start));
            Map<Object, Object> ends = new LinkedHashMap<>();
            t.logEnd().forEach((partition, end) -> ends.put((long) partition, end));
            topicList.add(map("id", t.id().toString(), "name", t.name(), "partitions", (long) t.partitions(),
                    "alive", t.alive(), "log-start", starts, "log-end", ends));
        }
        out.put(kw("topics"), topicList);
        List<Object> taskList = new ArrayList<>();
        for (TaskInfo t : tasks) {
            taskList.add(map("process", t.process(), "task", (long) t.task(), "receives", channels(t.receives())));
        }
        out.put(kw("tasks"), taskList);
        List<Object> starts = new ArrayList<>();
        for (StartPositions s : startPositions) {
            starts.add(map("process", s.process(), "task", (long) s.task(), "positions", channelMap(s.positions())));
        }
        out.put(kw("start-positions"), starts);
        List<Object> recs = new ArrayList<>();
        for (Rec r : records) {
            Map<Object, Object> m = map("topic", r.topicId().toString(), "partition", (long) r.partition(),
                    "offset", r.offset(), "key", r.key(), "value", r.value(), "uid", r.uid(),
                    "causes", header(r.causesHeader()));
            if (r.stampedFrom() != null) {
                m.put(kw("stamped-from"), position(r.stampedFrom()));
            }
            recs.add(m);
        }
        out.put(kw("records"), recs);
        List<Object> entries = new ArrayList<>();
        for (TraceEntry e : trace) {
            List<Object> effects = new ArrayList<>();
            for (Effect effect : e.effects()) {
                effects.add(List.of(effect.topic(), effect.uid()));
            }
            entries.add(map("process", e.process(), "task", (long) e.task(), "tp", e.tp(), "to", e.to(),
                    "channel", channel(e.channel()), "offset", e.offset(), "uid", e.uid(),
                    "causes", header(e.causesHeader()), "effects", effects));
        }
        out.put(kw("trace"), entries);
        List<Object> readList = new ArrayList<>();
        for (Reads r : reads) {
            readList.add(map("index", r.index(), "time", r.time(), "process", r.process(), "task", (long) r.task(),
                    "ends-lo", ends(r.endsLo()), "ends-hi", ends(r.endsHi()), "next-read", channelMap(r.nextRead()),
                    "exec-start", channelMap(r.execStart())));
        }
        out.put(kw("reads"), readList);
        List<Object> statusList = new ArrayList<>();
        for (Status s : statuses) {
            statusList.add(map("index", s.index(), "time", s.time(), "process", s.process(),
                    "lifecycle", kw(s.lifecycle().name()), "refusal", s.refusal() == null ? null : kw(s.refusal().name()),
                    "detail", s.detail(), "trace-ends", ends(s.traceEnds())));
        }
        out.put(kw("statuses"), statusList);
        List<Object> faultList = new ArrayList<>();
        for (Fault f : faults) {
            List<Object> justifies = new ArrayList<>();
            for (FailClosedException.Reason reason : f.justifies()) {
                justifies.add(kw(reason.name()));
            }
            faultList.add(map("index", f.index(), "time", f.time(), "kind", kw(f.kind()), "justifies", justifies,
                    "trace-ends", ends(f.traceEnds()), "details", f.details()));
        }
        out.put(kw("faults"), faultList);
        return out;
    }

    static JepsenExport fromEdn(Object edn) {
        JepsenExport export = new JepsenExport();
        if (integer(edn, "format") != FORMAT) {
            throw new IllegalArgumentException("unsupported export format " + integer(edn, "format"));
        }
        export.source = Source.valueOf(keywordName(get(edn, "source")).toUpperCase());
        export.seed = optionalInteger(edn, "seed");
        export.sabotage = keywordName(get(edn, "sabotage"));
        export.hostFault = keywordName(get(edn, "host-fault"));
        export.traceTopic = string(edn, "trace-topic");
        asMap(get(edn, "processes")).forEach((name, decl) -> export.processes.put(name.toString(),
                new ProcessDecl(name.toString(), strings(get(decl, "receives")), strings(get(decl, "sends")))));
        for (Object t : asList(get(edn, "topics"))) {
            Map<Integer, Long> logStart = new LinkedHashMap<>();
            asMap(get(t, "log-start")).forEach((partition, start) ->
                    logStart.put(((Number) partition).intValue(), ((Number) start).longValue()));
            Map<Integer, Long> logEnd = new LinkedHashMap<>();
            asMap(get(t, "log-end")).forEach((partition, end) ->
                    logEnd.put(((Number) partition).intValue(), ((Number) end).longValue()));
            export.topics.add(new TopicInfo(UUID.fromString(string(t, "id")), string(t, "name"),
                    (int) integer(t, "partitions"), Boolean.TRUE.equals(get(t, "alive")), logStart, logEnd));
        }
        for (Object t : asList(get(edn, "tasks"))) {
            export.tasks.add(new TaskInfo(string(t, "process"), (int) integer(t, "task"), channels(get(t, "receives"))));
        }
        for (Object s : asList(get(edn, "start-positions"))) {
            export.startPositions.add(new StartPositions(string(s, "process"), (int) integer(s, "task"),
                    channelMap(get(s, "positions"))));
        }
        for (Object r : asList(get(edn, "records"))) {
            export.records.add(new Rec(UUID.fromString(string(r, "topic")), (int) integer(r, "partition"),
                    integer(r, "offset"), string(r, "key"), string(r, "value"), string(r, "uid"),
                    header(get(r, "causes")), get(r, "stamped-from") == null ? null : position(get(r, "stamped-from"))));
        }
        for (Object e : asList(get(edn, "trace"))) {
            List<Effect> effects = new ArrayList<>();
            for (Object effect : asList(get(e, "effects"))) {
                List<Object> pair = asList(effect);
                effects.add(new Effect(pair.get(0).toString(), pair.get(1).toString()));
            }
            Channel channel = channel(get(e, "channel"));
            export.trace.add(new TraceEntry(string(e, "process"), (int) integer(e, "task"), string(e, "tp"),
                    integer(e, "to"), channel.topicId(), channel.partition(), integer(e, "offset"), string(e, "uid"),
                    header(get(e, "causes")), effects));
        }
        for (Object r : asList(get(edn, "reads"))) {
            export.reads.add(new Reads(integer(r, "index"), optionalInteger(r, "time"), string(r, "process"),
                    (int) integer(r, "task"), ends(get(r, "ends-lo")), ends(get(r, "ends-hi")),
                    channelMap(get(r, "next-read")), channelMap(get(r, "exec-start"))));
        }
        for (Object s : asList(get(edn, "statuses"))) {
            String refusal = keywordName(get(s, "refusal"));
            export.statuses.add(new Status(integer(s, "index"), optionalInteger(s, "time"), string(s, "process"),
                    ProcessStatus.Lifecycle.valueOf(keywordName(get(s, "lifecycle"))),
                    refusal == null ? null : FailClosedException.Reason.valueOf(refusal), string(s, "detail"),
                    ends(get(s, "trace-ends"))));
        }
        for (Object f : asList(get(edn, "faults"))) {
            Set<FailClosedException.Reason> justifies = new LinkedHashSet<>();
            for (Object reason : asList(get(f, "justifies"))) {
                justifies.add(FailClosedException.Reason.valueOf(keywordName(reason)));
            }
            export.faults.add(new Fault(integer(f, "index"), optionalInteger(f, "time"), keywordName(get(f, "kind")),
                    justifies, ends(get(f, "trace-ends")), asMap(get(f, "details"))));
        }
        return export;
    }

    String write() {
        return JepsenEdn.write(toEdn()) + "\n";
    }

    static JepsenExport read(String text) {
        return fromEdn(JepsenEdn.read(text));
    }

    void writeTo(Path path) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Files.writeString(path, write(), StandardCharsets.UTF_8);
    }

    static JepsenExport readFrom(Path path) throws IOException {
        return read(Files.readString(path, StandardCharsets.UTF_8));
    }

    // ---- element codecs ----

    static Object header(byte[] header) {
        if (header == null) {
            return null;
        }
        if (header == NULL_HEADER_VALUE) {
            return kw("null-value");
        }
        return HexFormat.of().formatHex(header);
    }

    static byte[] header(Object edn) {
        if (edn == null) {
            return null;
        }
        if (edn instanceof JepsenEdn.Kw) {
            return NULL_HEADER_VALUE;
        }
        return HexFormat.of().parseHex(edn.toString());
    }

    static List<Object> channel(Channel channel) {
        return List.of(channel.topicId().toString(), (long) channel.partition());
    }

    static Channel channel(Object edn) {
        List<Object> pair = asList(edn);
        return new Channel(UUID.fromString(pair.get(0).toString()), ((Number) pair.get(1)).intValue());
    }

    static List<Object> channels(List<Channel> channels) {
        List<Object> out = new ArrayList<>();
        for (Channel channel : channels) {
            out.add(channel(channel));
        }
        return out;
    }

    static List<Channel> channels(Object edn) {
        List<Channel> out = new ArrayList<>();
        for (Object channel : asList(edn)) {
            out.add(channel(channel));
        }
        return out;
    }

    static List<Object> position(Position position) {
        return List.of(position.topicId().toString(), (long) position.partition(), position.offset());
    }

    static Position position(Object edn) {
        List<Object> triple = asList(edn);
        return new Position(UUID.fromString(triple.get(0).toString()), ((Number) triple.get(1)).intValue(),
                ((Number) triple.get(2)).longValue());
    }

    static Map<Object, Object> channelMap(Map<Channel, Long> positions) {
        Map<Object, Object> out = new LinkedHashMap<>();
        new java.util.TreeMap<>(positions).forEach((channel, position) -> out.put(channel(channel), position));
        return out;
    }

    static Map<Channel, Long> channelMap(Object edn) {
        Map<Channel, Long> out = new LinkedHashMap<>();
        asMap(edn).forEach((channel, position) -> out.put(channel(channel), ((Number) position).longValue()));
        return out;
    }

    static Map<Object, Object> ends(Map<String, Long> ends) {
        return new LinkedHashMap<>(ends);
    }

    static Map<String, Long> ends(Object edn) {
        Map<String, Long> out = new LinkedHashMap<>();
        asMap(edn).forEach((tp, end) -> out.put(tp.toString(), ((Number) end).longValue()));
        return out;
    }

    private static List<String> strings(Object edn) {
        List<String> out = new ArrayList<>();
        for (Object o : asList(edn)) {
            out.add(o.toString());
        }
        return out;
    }
}
