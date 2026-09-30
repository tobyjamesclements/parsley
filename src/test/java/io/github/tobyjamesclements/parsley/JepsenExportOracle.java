package io.github.tobyjamesclements.parsley;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The first checker over a {@link JepsenExport}: replays the trace through the simulator's
 * {@link Oracle}, which judges causal order, duplicates, FIFO and expression from ground
 * truth reconstructed here, never from what the engine claims.
 *
 * <p>Happened-before comes from the trace and the declaration, not from headers. A record
 * sent by task T in the step that delivered D is caused by D, by every earlier delivery at
 * T, by the record an external client observed before a stamped send, by the causes of
 * every record T is known to have received before the step, and transitively by their
 * causes. That is a sound subset of the true causes: a receipt is known only from a read
 * observation bracketed by the trace ends, so a receipt no observation covers is not in
 * it, and the expression check catches what that misses, since every send must express
 * every cause its sender had delivered or seen expressed.
 *
 * <p>The judgements, cited as in {@code SPEC.md}: Safety 1 at delivery time and over
 * delivered pairs, Safety 2, Safety 3, Safety 7, Safety 8, Structural 12, 14 and 15,
 * over-expression, Liveness 1 at quiescence with the spec's exemptions, Host obligations 3
 * and 6 through the effects each step declared, Assumption 2, and Operational 1 and 6:
 * every refusal follows an injected fault that justifies it.
 */
final class JepsenExportOracle {

    record Verdict(List<String> violations) {
        boolean clean() {
            return violations.isEmpty();
        }
    }

    static Verdict check(JepsenExport export) {
        return new JepsenExportOracle(export).run();
    }

    private final JepsenExport export;
    private final Oracle oracle = new Oracle();
    private final List<String> violations = new ArrayList<>();

    private final Map<UUID, JepsenExport.TopicInfo> topics = new HashMap<>();
    private final Map<String, List<JepsenExport.TopicInfo>> topicsByName = new LinkedHashMap<>();
    private final Map<Channel, TreeMap<Long, JepsenExport.Rec>> recordsByChannel = new HashMap<>();
    private final Map<String, List<JepsenExport.Rec>> recordsByUid = new HashMap<>();
    private final Map<String, JepsenExport.TraceEntry> producerByEffectUid = new HashMap<>();
    private final Map<String, List<JepsenExport.TraceEntry>> traceByTask = new LinkedHashMap<>();
    private final Map<JepsenExport.TraceEntry, Integer> stepOf = new IdentityHashMap<>();
    private final Map<String, List<JepsenExport.Reads>> readsByTask = new HashMap<>();
    private final Map<String, Map<Channel, Long>> startByTask = new HashMap<>();
    private final Map<String, JepsenExport.TaskInfo> taskInfo = new LinkedHashMap<>();
    private final Map<String, List<JepsenExport.Fault>> declarationsByTask = new HashMap<>();
    private final Map<Channel, Long> lastAssigned = new HashMap<>();
    private final Map<String, JepsenExport.Status> firstRefusalByProcess = new LinkedHashMap<>();
    private final Map<Channel, Set<Long>> undecodableAt = new HashMap<>();
    private final Map<UUID, Long> createdAt = new HashMap<>();
    private final Map<UUID, Long> deadAt = new HashMap<>();

    private final Map<Channel, Map<Long, Instance>> instanceByPosition = new HashMap<>();
    private final Map<JepsenExport.Rec, Causes> metaByRecord = new java.util.IdentityHashMap<>();
    private final Map<String, List<Set<Instance>>> pastByTask = new HashMap<>();
    private final Map<String, Map<Channel, Set<Long>>> mergedReceiptsByTask = new HashMap<>();
    private final Map<String, Integer> computing = new HashMap<>();

    private JepsenExportOracle(JepsenExport export) {
        this.export = export;
    }

    private Verdict run() {
        index();
        Set<String> tasks = new LinkedHashSet<>(allTasks());
        tasks.addAll(traceByTask.keySet());
        for (String task : tasks) {
            replay(task);
        }
        oracle.finalChecks();
        violations.addAll(oracle.violations());
        for (String task : tasks) {
            liveness(task);
            safety8(task);
        }
        duplicatesAcrossTasks();
        refusalsJustified();
        orphans();
        recreations();
        return new Verdict(List.copyOf(violations));
    }

    // ---- indexing ----

    private void index() {
        for (JepsenExport.TopicInfo topic : export.topics) {
            topics.put(topic.id(), topic);
            topicsByName.computeIfAbsent(topic.name(), n -> new ArrayList<>()).add(topic);
            // The last assigned position counts the offsets markers and aborted records took, as the
            // simulator's world.lastAssigned does; the records alone would say a frontier that names a
            // settled out-of-contract position names an unassigned one.
            topic.logEnd().forEach((partition, end) -> {
                if (end > 0) {
                    lastAssigned.merge(new Channel(topic.id(), partition), end - 1, Math::max);
                }
            });
        }
        for (JepsenExport.Rec rec : export.records) {
            recordsByChannel.computeIfAbsent(rec.channel(), c -> new TreeMap<>()).put(rec.offset(), rec);
            if (rec.uid() != null) {
                recordsByUid.computeIfAbsent(rec.uid(), u -> new ArrayList<>()).add(rec);
            }
            lastAssigned.merge(rec.channel(), rec.offset(), Math::max);
            if (rec.causesHeader() != null && decode(rec.causesHeader()) == null) {
                undecodableAt.computeIfAbsent(rec.channel(), c -> new HashSet<>()).add(rec.offset());
            }
        }
        List<JepsenExport.TraceEntry> ordered = new ArrayList<>(export.trace);
        ordered.sort(java.util.Comparator.comparing(JepsenExport.TraceEntry::tp)
                .thenComparingLong(JepsenExport.TraceEntry::to));
        for (JepsenExport.TraceEntry entry : ordered) {
            List<JepsenExport.TraceEntry> entries = traceByTask.computeIfAbsent(entry.taskName(), t -> new ArrayList<>());
            stepOf.put(entry, entries.size());
            entries.add(entry);
            for (JepsenExport.Effect effect : entry.effects()) {
                JepsenExport.TraceEntry previous = producerByEffectUid.putIfAbsent(effect.uid(), entry);
                if (previous != null) {
                    violations.add("Safety 2: " + effect.uid() + " was sent by two committed steps, "
                            + describe(previous) + " and " + describe(entry));
                }
            }
        }
        for (JepsenExport.Reads reads : export.reads) {
            readsByTask.computeIfAbsent(reads.taskName(), t -> new ArrayList<>()).add(reads);
        }
        readsByTask.values().forEach(list -> list.sort(java.util.Comparator.comparingLong(JepsenExport.Reads::index)));
        for (JepsenExport.StartPositions start : export.startPositions) {
            Map<Channel, Long> positions = startByTask.computeIfAbsent(start.taskName(), t -> new LinkedHashMap<>());
            start.positions().forEach(positions::putIfAbsent);
        }
        for (JepsenExport.TaskInfo info : export.tasks) {
            taskInfo.put(info.taskName(), info);
        }
        List<JepsenExport.Fault> faults = new ArrayList<>(export.faults);
        faults.sort(java.util.Comparator.comparingLong(JepsenExport.Fault::index));
        for (JepsenExport.Fault fault : faults) {
            switch (fault.kind()) {
                case "declared" -> {
                    String process = JepsenEdn.string(fault.details(), "process");
                    Long task = JepsenEdn.optionalInteger(fault.details(), "task");
                    declarationsByTask.computeIfAbsent(JepsenExport.taskName(process, task == null ? 0 : task.intValue()),
                            t -> new ArrayList<>()).add(fault);
                }
                case "recreate" -> {
                    String old = JepsenEdn.string(fault.details(), "old");
                    String fresh = JepsenEdn.string(fault.details(), "new");
                    if (old != null) {
                        deadAt.putIfAbsent(UUID.fromString(old), fault.index());
                    }
                    if (fresh != null) {
                        createdAt.putIfAbsent(UUID.fromString(fresh), fault.index());
                    }
                }
                case "kill" -> {
                    Object channel = JepsenEdn.get(fault.details(), "channel");
                    if (channel != null) {
                        deadAt.putIfAbsent(JepsenExport.channel(channel).topicId(), fault.index());
                    }
                }
                default -> {
                }
            }
        }
        List<JepsenExport.Status> statuses = new ArrayList<>(export.statuses);
        statuses.sort(java.util.Comparator.comparingLong(JepsenExport.Status::index));
        for (JepsenExport.Status status : statuses) {
            if (status.refusal() != null) {
                firstRefusalByProcess.putIfAbsent(status.process(), status);
            }
        }
    }

    private List<String> allTasks() {
        List<String> tasks = new ArrayList<>();
        if (!taskInfo.isEmpty()) {
            tasks.addAll(taskInfo.keySet());
            return tasks;
        }
        for (JepsenExport.ProcessDecl decl : export.processes.values()) {
            int width = 0;
            for (String topic : decl.receives()) {
                for (JepsenExport.TopicInfo info : topicsByName.getOrDefault(topic, List.of())) {
                    width = Math.max(width, info.partitions());
                }
            }
            for (int p = 0; p < width; p++) {
                tasks.add(JepsenExport.taskName(decl.name(), p));
            }
        }
        return tasks;
    }

    private static String describe(JepsenExport.TraceEntry entry) {
        return entry.taskName() + " step " + entry.tp() + "@" + entry.to() + " delivering " + entry.uid();
    }

    private String processOf(String task) {
        return task.substring(0, task.lastIndexOf('-'));
    }

    private int partitionOf(String task) {
        return Integer.parseInt(task.substring(task.lastIndexOf('-') + 1));
    }

    // ---- received sets over time ----

    /** The task's channels at its first start: the export's own list, else partition {@code task} of each received topic. */
    private Set<Channel> declaredInitially(String task) {
        JepsenExport.TaskInfo info = taskInfo.get(task);
        if (info != null) {
            return new LinkedHashSet<>(info.receives());
        }
        JepsenExport.ProcessDecl decl = export.processes.get(processOf(task));
        Set<Channel> received = new LinkedHashSet<>();
        if (decl == null) {
            return received;
        }
        int partition = partitionOf(task);
        for (String topic : decl.receives()) {
            for (JepsenExport.TopicInfo topicInfo : topicsByName.getOrDefault(topic, List.of())) {
                if (partition < topicInfo.partitions()) {
                    received.add(new Channel(topicInfo.id(), partition));
                }
            }
        }
        return received;
    }

    private static Set<Channel> declaredBy(JepsenExport.Fault declaration) {
        return new LinkedHashSet<>(JepsenExport.channels(JepsenEdn.get(declaration.details(), "receives")));
    }

    /** The received set in force when {@code entry} was delivered: the last declaration before it. */
    private Set<Channel> receivedAt(String task, JepsenExport.TraceEntry entry) {
        Set<Channel> received = declaredInitially(task);
        for (JepsenExport.Fault declaration : declarationsByTask.getOrDefault(task, List.of())) {
            Long end = declaration.traceEnds().get(entry.tp());
            if (end != null && end <= entry.to()) {
                received = declaredBy(declaration);
            }
        }
        return received;
    }

    /** The received set in force at history index {@code index}: the last declaration before it. */
    private Set<Channel> receivedAtIndex(String task, long index) {
        Set<Channel> received = declaredInitially(task);
        for (JepsenExport.Fault declaration : declarationsByTask.getOrDefault(task, List.of())) {
            if (declaration.index() < index) {
                received = declaredBy(declaration);
            }
        }
        return received;
    }

    /** The received set at the end of the run, which is what quiescence is judged over. */
    private Set<Channel> receivedFinally(String task) {
        List<JepsenExport.Fault> declarations = declarationsByTask.getOrDefault(task, List.of());
        return declarations.isEmpty() ? declaredInitially(task) : declaredBy(declarations.get(declarations.size() - 1));
    }

    /** Every channel the task received at any point of its lifetime. */
    private Set<Channel> receivedEver(String task) {
        Set<Channel> received = new LinkedHashSet<>(declaredInitially(task));
        for (JepsenExport.Fault declaration : declarationsByTask.getOrDefault(task, List.of())) {
            received.addAll(declaredBy(declaration));
        }
        return received;
    }

    // ---- topics ----

    private boolean dead(Channel channel) {
        JepsenExport.TopicInfo info = topics.get(channel.topicId());
        return info == null || !info.alive();
    }

    private boolean aliveAt(UUID topicId, long index) {
        Long created = createdAt.get(topicId);
        Long died = deadAt.get(topicId);
        return (created == null || created < index) && (died == null || died > index);
    }

    /** Whether, at history index {@code index}, the channel's topic is dead while its name resolves to a live other id. */
    private boolean recreatedAt(Channel channel, long index) {
        Long died = deadAt.get(channel.topicId());
        if (died == null || died > index) {
            return false;
        }
        JepsenExport.TopicInfo info = topics.get(channel.topicId());
        if (info == null) {
            return false;
        }
        for (JepsenExport.TopicInfo other : topicsByName.getOrDefault(info.name(), List.of())) {
            if (!other.id().equals(channel.topicId()) && aliveAt(other.id(), index)) {
                return true;
            }
        }
        return false;
    }

    private long logStart(Channel channel) {
        JepsenExport.TopicInfo info = topics.get(channel.topicId());
        return info == null ? 0L : info.logStart().getOrDefault(channel.partition(), 0L);
    }

    private String topicName(UUID id) {
        JepsenExport.TopicInfo info = topics.get(id);
        return info == null ? id.toString() : info.name();
    }

    private boolean nothingDiscarded(String topicName) {
        for (JepsenExport.TopicInfo info : topicsByName.getOrDefault(topicName, List.of())) {
            if (!info.alive() || info.logStart().values().stream().anyMatch(start -> start > 0)) {
                return false;
            }
        }
        return true;
    }

    private JepsenExport.Rec recordAt(Channel channel, long offset) {
        TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
        return records == null ? null : records.get(offset);
    }

    /** Decodes a header, or returns {@code null} where the frozen grammar refuses it. */
    static Causes decode(byte[] header) {
        if (header == null) {
            return Causes.none();
        }
        try {
            return CausesCodec.decode(header == JepsenExport.NULL_HEADER_VALUE ? null : header);
        } catch (CausesCodec.UndecodableMetadataException e) {
            return null;
        }
    }

    // ---- what a task had received, from the read observations ----

    /** An interval of positions {@code [from, to)} on one channel. */
    record Span(long from, long to) {
        boolean contains(long position) {
            return position >= from && position < to;
        }
    }

    /**
     * The union of spans as disjoint spans in ascending order. Every observation of a task
     * contributes a span and most nest in the next, so walking the records of each span in
     * turn walks the same records once per observation, which made the replay quadratic in
     * a task's observations; walking the union walks them once.
     */
    static List<Span> merged(List<Span> spans) {
        List<Span> sorted = new ArrayList<>(spans);
        sorted.sort(java.util.Comparator.comparingLong(Span::from).thenComparingLong(Span::to));
        List<Span> merged = new ArrayList<>();
        for (Span span : sorted) {
            if (!merged.isEmpty() && span.from() <= merged.get(merged.size() - 1).to()) {
                Span last = merged.remove(merged.size() - 1);
                merged.add(new Span(last.from(), Math.max(last.to(), span.to())));
            } else {
                merged.add(span);
            }
        }
        return merged;
    }

    /** {@link #decode} once per record: the expression bound reads every received record's header at every step. */
    private Causes decodedMeta(JepsenExport.Rec rec) {
        return metaByRecord.computeIfAbsent(rec, r -> decode(r.causesHeader()));
    }

    /** Where the execution behind an observation began reading {@code channel}: its own start, else the task's recorded one. */
    private long executionStart(String task, JepsenExport.Reads reads, Channel channel) {
        Long start = reads.execStart().get(channel);
        if (start != null) {
            return start;
        }
        return startByTask.getOrDefault(task, Map.of()).getOrDefault(channel, 0L);
    }

    /**
     * Positions the task is known to have received on {@code channel} before {@code entry}:
     * for every observation taken before the step, the span from where its execution began
     * reading the channel to the position it committed. Empty when no observation covers
     * it, which under-approximates receipt.
     */
    private List<Span> receivedSpansBefore(String task, Channel channel, JepsenExport.TraceEntry entry) {
        List<Span> spans = new ArrayList<>();
        for (JepsenExport.Reads reads : readsByTask.getOrDefault(task, List.of())) {
            Long end = reads.endsHi().get(entry.tp());
            Long next = reads.nextRead().get(channel);
            if (end != null && end <= entry.to() && next != null) {
                long from = executionStart(task, reads, channel);
                if (next > from) {
                    spans.add(new Span(from, next));
                }
            }
        }
        return spans;
    }

    /** Every span of positions the task is known to have received on {@code channel} at any time. */
    private List<Span> receivedSpansEver(String task, Channel channel) {
        List<Span> spans = new ArrayList<>();
        for (JepsenExport.Reads reads : readsByTask.getOrDefault(task, List.of())) {
            Long next = reads.nextRead().get(channel);
            if (next != null) {
                long from = executionStart(task, reads, channel);
                if (next > from) {
                    spans.add(new Span(from, next));
                }
            }
        }
        return spans;
    }

    /**
     * Every span of positions the task could have received on {@code channel} before
     * {@code entry}: the spans of every observation taken before the step, and the span of
     * the first observation taken after it, whose positions cover the step's own receipts.
     * With no observation after the step, everything on the channel. Over-approximates
     * receipt, and survives a rewind, after which the committed position drops below what
     * earlier executions received.
     */
    private List<Span> receivedSpansUpTo(String task, Channel channel, JepsenExport.TraceEntry entry) {
        List<Span> spans = new ArrayList<>(receivedSpansBefore(task, channel, entry));
        JepsenExport.Reads first = null;
        for (JepsenExport.Reads reads : readsByTask.getOrDefault(task, List.of())) {
            Long end = reads.endsLo().get(entry.tp());
            if (end != null && end > entry.to() && (first == null || reads.index() < first.index())) {
                first = reads;
            }
        }
        if (first == null) {
            spans.add(new Span(0, Long.MAX_VALUE));
        } else {
            Long next = first.nextRead().get(channel);
            if (next != null) {
                long from = executionStart(task, first, channel);
                if (next > from) {
                    spans.add(new Span(from, next));
                }
            }
        }
        return spans;
    }

    // ---- ground truth: instances and their true causes ----

    private Instance instanceOf(Channel channel, long offset, String uidHint) {
        Map<Long, Instance> byOffset = instanceByPosition.computeIfAbsent(channel, c -> new HashMap<>());
        Instance existing = byOffset.get(offset);
        if (existing != null) {
            return existing;
        }
        JepsenExport.Rec rec = recordAt(channel, offset);
        String uid = rec != null && rec.uid() != null ? rec.uid() : uidHint == null ? "?" : uidHint;
        byte[] header = rec == null ? null : rec.causesHeader();
        Causes meta = decode(header);
        List<Header> headers = header == null ? List.of()
                : List.of(new Header(CausesCodec.HEADER_KEY, header == JepsenExport.NULL_HEADER_VALUE ? null : header));
        Set<Instance> causes = trueCausesOf(uid, rec);
        Instance instance = new Instance(channel, offset, uid,
                rec == null || rec.key() == null ? null : rec.key().getBytes(StandardCharsets.UTF_8),
                rec == null || rec.value() == null ? null : rec.value().getBytes(StandardCharsets.UTF_8),
                headers, meta == null ? Causes.none() : meta, causes);
        byOffset.put(offset, instance);
        return instance;
    }

    private Set<Instance> trueCausesOf(String uid, JepsenExport.Rec rec) {
        if (rec != null && rec.stampedFrom() != null) {
            JepsenExport.Position from = rec.stampedFrom();
            if (recordAt(from.channel(), from.offset()) == null) {
                return Set.of();
            }
            Instance observed = instanceOf(from.channel(), from.offset(), null);
            Set<Instance> causes = new HashSet<>(observed.trueCauses);
            causes.add(observed);
            return causes;
        }
        JepsenExport.TraceEntry producer = producerByEffectUid.get(uid);
        return producer == null ? Set.of() : pastAfter(producer);
    }

    /**
     * The producing task's causal past once its step {@code entry} had delivered: every
     * earlier delivery and its causes, the causes of every record known received before
     * the step, and the delivery itself with its causes.
     */
    private Set<Instance> pastAfter(JepsenExport.TraceEntry entry) {
        String task = entry.taskName();
        int step = stepOf.get(entry);
        List<Set<Instance>> snapshots = pastByTask.computeIfAbsent(task, t -> new ArrayList<>());
        Integer inProgress = computing.get(task);
        if (inProgress != null && step >= inProgress) {
            violations.add("Causal cycle: " + describe(entry) + " is among the causes of what it delivered");
            return snapshots.isEmpty() ? Set.of() : snapshots.get(snapshots.size() - 1);
        }
        List<JepsenExport.TraceEntry> entries = traceByTask.get(task);
        Map<Channel, Set<Long>> merged = mergedReceiptsByTask.computeIfAbsent(task, t -> new HashMap<>());
        while (snapshots.size() <= step) {
            int next = snapshots.size();
            computing.put(task, next);
            JepsenExport.TraceEntry delivered = entries.get(next);
            Set<Instance> past = new HashSet<>(next == 0 ? Set.of() : snapshots.get(next - 1));
            for (Channel channel : receivedEver(task)) {
                TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
                if (records == null) {
                    continue;
                }
                Set<Long> done = merged.computeIfAbsent(channel, c -> new HashSet<>());
                for (Span span : merged(receivedSpansBefore(task, channel, delivered))) {
                    for (JepsenExport.Rec rec : records.subMap(span.from(), span.to()).values()) {
                        if (done.add(rec.offset())) {
                            past.addAll(instanceOf(rec.channel(), rec.offset(), rec.uid()).trueCauses);
                        }
                    }
                }
            }
            Instance instance = instanceOf(delivered.channel(), delivered.offset(), delivered.uid());
            past.add(instance);
            past.addAll(instance.trueCauses);
            computing.remove(task);
            snapshots.add(past);
        }
        return snapshots.get(step);
    }

    private Channel traceChannel(JepsenExport.TraceEntry entry) {
        if (export.traceTopic != null) {
            for (JepsenExport.TopicInfo info : topicsByName.getOrDefault(export.traceTopic, List.of())) {
                if (info.alive()) {
                    try {
                        return new Channel(info.id(), Integer.parseInt(entry.tp()));
                    } catch (NumberFormatException e) {
                        break;
                    }
                }
            }
        }
        return new Channel(UUID.nameUUIDFromBytes(("trace:" + entry.taskName()).getBytes(StandardCharsets.UTF_8)), 0);
    }

    /**
     * Everything the task could have seen expressed by the time of {@code entry}: what it
     * had delivered, every position it could have received on a channel it ever received,
     * and every position the records there name. {@code null} when records the task may have
     * received are gone from the export, since what they named is gone with them: retention
     * discarded them and the export does not hold them (the simulator's keeps every record its
     * retention discarded), or their topic was deleted and nothing of it could be dumped.
     */
    private Map<Channel, Long> expressionBound(String task, JepsenExport.TraceEntry entry, Map<Channel, Long> delivered) {
        Map<Channel, Long> bound = new HashMap<>(delivered);
        for (Channel channel : receivedEver(task)) {
            TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
            List<Span> spans = merged(receivedSpansUpTo(task, channel, entry));
            if (records == null) {
                // A deleted topic could not be dumped at all; the same goes for what the task received there.
                if (dead(channel) && !spans.isEmpty()) {
                    return null;
                }
                continue;
            }
            /*
             * Retention discarded records the task may have received, and what they named
             * went with them unless the export kept them, as the simulator's does: nothing
             * then bounds what the sender had seen expressed.
             */
            long logStart = logStart(channel);
            if (logStart > 0 && !spans.isEmpty() && spans.get(0).from() < logStart
                    && records.headMap(logStart).isEmpty()) {
                return null;
            }
            for (Span span : spans) {
                for (JepsenExport.Rec rec : records.subMap(span.from(), span.to()).values()) {
                    bound.merge(channel, rec.offset(), Math::max);
                    Causes meta = decodedMeta(rec);
                    if (meta != null) {
                        meta.byChannel().forEach((named, position) -> bound.merge(named, position, Math::max));
                    }
                }
            }
        }
        return bound;
    }

    // ---- what a task owes at quiescence ----

    /**
     * The records the task owes a delivery of: on every channel it ever received, those in
     * a span some observation shows it received and committed. With no read observations at
     * all, every record at or above the start position on its final channels.
     */
    private Map<Channel, List<JepsenExport.Rec>> owed(String task) {
        Map<Channel, Long> start = startByTask.getOrDefault(task, Map.of());
        boolean observed = !readsByTask.getOrDefault(task, List.of()).isEmpty();
        Map<Channel, List<JepsenExport.Rec>> owed = new LinkedHashMap<>();
        for (Channel channel : observed ? receivedEver(task) : receivedFinally(task)) {
            TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
            if (records == null) {
                continue;
            }
            TreeMap<Long, JepsenExport.Rec> range = new TreeMap<>();
            if (observed) {
                for (Span span : merged(receivedSpansEver(task, channel))) {
                    range.putAll(records.subMap(span.from(), span.to()));
                }
            } else {
                range.putAll(records.tailMap(start.getOrDefault(channel, 0L)));
            }
            owed.put(channel, new ArrayList<>(range.values()));
        }
        return owed;
    }

    // ---- replay ----

    private void replay(String task) {
        Map<Channel, Long> start = startByTask.getOrDefault(task, Map.of());
        oracle.onStart(task);
        owed(task).forEach((channel, records) -> {
            for (JepsenExport.Rec rec : records) {
                oracle.onFed(task, instanceOf(rec.channel(), rec.offset(), rec.uid()));
            }
        });
        Map<Channel, Long> maxDelivered = new HashMap<>();
        for (JepsenExport.TraceEntry entry : traceByTask.getOrDefault(task, List.of())) {
            Instance instance = instanceOf(entry.channel(), entry.offset(), entry.uid());
            JepsenExport.Rec rec = recordAt(entry.channel(), entry.offset());
            if (rec != null && !Objects.equals(rec.uid(), entry.uid())) {
                violations.add("Trace: " + task + " reports delivering " + entry.uid() + " at " + entry.channel() + "@"
                        + entry.offset() + " but the committed record there carries " + rec.uid());
            }
            if (rec == null && !dead(entry.channel()) && logStart(entry.channel()) <= entry.offset()) {
                violations.add("Trace: " + task + " reports delivering " + entry.channel() + "@" + entry.offset()
                        + " (" + entry.uid() + ") but no committed record exists there");
            }
            if (undecodableAt.getOrDefault(entry.channel(), Set.of()).contains(entry.offset())) {
                violations.add("Safety 7: " + task + " delivered " + instance
                        + " whose causal metadata is present and undecodable");
            }
            Set<Channel> receivedNow = receivedAt(task, entry);
            if (!receivedNow.contains(entry.channel())) {
                violations.add("Declaration: " + task + " delivered " + instance + " from a channel it does not receive");
            }
            oracle.onDelivered(task, instance, settledNow(task, instance, entry, receivedNow, start, maxDelivered));
            maxDelivered.merge(entry.channel(), entry.offset(), Math::max);

            Map<Channel, Long> upper = expressionBound(task, entry, maxDelivered);
            Set<Instance> past = pastAfter(entry);
            Set<Instance> excused = new HashSet<>();
            for (Instance cause : past) {
                if (dead(cause.channel)) {
                    excused.add(cause);
                }
            }
            List<Oracle.Sent> sents = new ArrayList<>();
            Causes traceMeta = decode(entry.causesHeader());
            if (traceMeta == null) {
                violations.add("Trace: the frontier " + task + " expressed at step " + entry.tp() + "@" + entry.to()
                        + " is undecodable");
                traceMeta = Causes.none();
            }
            Instance traceInstance = new Instance(traceChannel(entry), entry.to(), "trace:" + task + ":" + entry.to(),
                    null, null, List.of(), traceMeta, past);
            sents.add(new Oracle.Sent(traceInstance, upper, lastAssigned, excused));
            for (JepsenExport.Effect effect : entry.effects()) {
                List<JepsenExport.Rec> matches = new ArrayList<>();
                for (JepsenExport.Rec candidate : recordsByUid.getOrDefault(effect.uid(), List.of())) {
                    if (topicName(candidate.topicId()).equals(effect.topic())) {
                        matches.add(candidate);
                    }
                }
                if (matches.isEmpty()) {
                    if (nothingDiscarded(effect.topic())) {
                        violations.add("Host obligation 3: " + describe(entry) + " committed a send of " + effect.uid()
                                + " to " + effect.topic() + " but no committed record carries it");
                    }
                    continue;
                }
                if (matches.size() > 1) {
                    violations.add("Host obligation 6: " + effect.uid() + " is committed " + matches.size() + " times on "
                            + effect.topic() + "; a superseded execution's step was committed too");
                }
                for (JepsenExport.Rec match : matches) {
                    sents.add(new Oracle.Sent(instanceOf(match.channel(), match.offset(), match.uid()), upper,
                            lastAssigned, excused));
                }
            }
            oracle.commitStep(task, sents);
        }
    }

    /**
     * The causes of {@code instance} settled by evidence at this moment: on a channel the
     * task does not receive, below the task's start position, or on a dead channel the task
     * is not known to have received them from. A cause the task had received is never
     * settled by its channel's death: the task owes its delivery (Safety 9).
     */
    private Set<Instance> settledNow(String task, Instance instance, JepsenExport.TraceEntry entry,
                                     Set<Channel> received, Map<Channel, Long> start, Map<Channel, Long> maxDelivered) {
        Set<Instance> settled = new HashSet<>();
        for (Instance cause : instance.trueCauses) {
            if (!received.contains(cause.channel)) {
                settled.add(cause);
            } else if (cause.position < start.getOrDefault(cause.channel, 0L)) {
                settled.add(cause);
            } else if (dead(cause.channel) && !receivedBefore(task, cause, entry, maxDelivered)) {
                settled.add(cause);
            }
        }
        return settled;
    }

    private boolean receivedBefore(String task, Instance cause, JepsenExport.TraceEntry entry,
                                   Map<Channel, Long> maxDelivered) {
        Long delivered = maxDelivered.get(cause.channel);
        if (delivered != null && delivered > cause.position) {
            return true;
        }
        for (Span span : receivedSpansBefore(task, cause.channel, entry)) {
            if (span.contains(cause.position)) {
                return true;
            }
        }
        return false;
    }

    // ---- final judgements ----

    private boolean refused(String task) {
        return firstRefusalByProcess.containsKey(processOf(task));
    }

    private void liveness(String task) {
        if (refused(task)) {
            return;
        }
        Map<Channel, Long> start = startByTask.getOrDefault(task, Map.of());
        Set<Instance> exempt = exemptions(task, receivedFinally(task), start);
        Map<Channel, List<Instance>> undelivered = oracle.undeliveredOwedByChannel(task);
        List<Instance> stranded = new ArrayList<>();
        undelivered.values().forEach(stranded::addAll);
        stranded.sort(java.util.Comparator.comparing((Instance i) -> i.channel).thenComparingLong(i -> i.position));
        for (Instance instance : stranded) {
            if (!exempt.contains(instance)) {
                violations.add("Liveness 1: " + task + " never delivered " + instance
                        + ", which it received on a channel it declares at or above its start position");
            }
        }
    }

    /**
     * Records a task is not owed at quiescence: those held behind an undecodable header on
     * their channel (Safety 7 and 3), and those held behind a stamp naming a position no
     * later record on that channel settles (wire-format constraint 8), transitively.
     */
    private Set<Instance> exemptions(String task, Set<Channel> received, Map<Channel, Long> start) {
        Set<Instance> exempt = new HashSet<>();
        Map<Channel, List<Instance>> owed = new HashMap<>();
        for (Channel channel : received) {
            TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
            if (records == null) {
                continue;
            }
            List<Instance> instances = new ArrayList<>();
            boolean behindUndecodable = false;
            for (JepsenExport.Rec rec : records.tailMap(start.getOrDefault(channel, 0L)).values()) {
                Instance instance = instanceOf(rec.channel(), rec.offset(), rec.uid());
                instances.add(instance);
                if (undecodableAt.getOrDefault(channel, Set.of()).contains(rec.offset())) {
                    behindUndecodable = true;
                }
                if (behindUndecodable) {
                    exempt.add(instance);
                }
            }
            owed.put(channel, instances);
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<Channel, List<Instance>> entry : owed.entrySet()) {
                boolean behindExempt = false;
                for (Instance instance : entry.getValue()) {
                    if (exempt.contains(instance)) {
                        behindExempt = true;
                        continue;
                    }
                    if (behindExempt || heldByUnsettledStamp(instance, received, start, exempt, owed)) {
                        exempt.add(instance);
                        behindExempt = true;
                        changed = true;
                    }
                }
            }
        }
        return exempt;
    }

    private boolean heldByUnsettledStamp(Instance instance, Set<Channel> received, Map<Channel, Long> start,
                                         Set<Instance> exempt, Map<Channel, List<Instance>> owed) {
        for (Map.Entry<Channel, Long> named : instance.meta.byChannel().entrySet()) {
            Channel channel = named.getKey();
            long position = named.getValue();
            if (!received.contains(channel) || dead(channel) || position < start.getOrDefault(channel, 0L)) {
                continue;
            }
            Long last = lastAssigned.get(channel);
            if (last == null || last < position) {
                return true;
            }
            for (Instance candidate : owed.getOrDefault(channel, List.of())) {
                if (candidate.position <= position && exempt.contains(candidate)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void safety8(String task) {
        if (refused(task)) {
            return;
        }
        Map<Channel, Long> start = startByTask.getOrDefault(task, Map.of());
        Map<Channel, Long> lastRead = new HashMap<>();
        List<JepsenExport.Reads> reads = readsByTask.getOrDefault(task, List.of());
        if (!reads.isEmpty()) {
            lastRead.putAll(reads.get(reads.size() - 1).nextRead());
        }
        Map<Channel, Long> maxDelivered = new HashMap<>();
        for (JepsenExport.TraceEntry entry : traceByTask.getOrDefault(task, List.of())) {
            maxDelivered.merge(entry.channel(), entry.offset(), Math::max);
        }
        for (Channel channel : receivedFinally(task)) {
            if (dead(channel)) {
                continue;
            }
            long covered = Math.max(start.getOrDefault(channel, 0L), Math.max(
                    maxDelivered.getOrDefault(channel, Long.MIN_VALUE) + 1, lastRead.getOrDefault(channel, 0L)));
            long logStart = logStart(channel);
            if (logStart > covered) {
                violations.add("Safety 8: " + task + " sailed past discarded positions on " + channel
                        + " (earliest retained " + logStart + " > covered position " + covered + ") without failing closed");
            }
        }
    }

    private void duplicatesAcrossTasks() {
        Map<String, Map<Channel, Map<Long, String>>> seen = new HashMap<>();
        for (Map.Entry<String, List<JepsenExport.TraceEntry>> entry : traceByTask.entrySet()) {
            String process = processOf(entry.getKey());
            for (JepsenExport.TraceEntry delivered : entry.getValue()) {
                String previous = seen.computeIfAbsent(process, p -> new HashMap<>())
                        .computeIfAbsent(delivered.channel(), c -> new HashMap<>())
                        .putIfAbsent(delivered.offset(), entry.getKey());
                if (previous != null && !previous.equals(entry.getKey())) {
                    violations.add("Safety 2: " + process + " delivered " + delivered.channel() + "@" + delivered.offset()
                            + " at two tasks, " + previous + " and " + entry.getKey());
                }
            }
        }
    }

    private void refusalsJustified() {
        firstRefusalByProcess.forEach((process, status) -> {
            boolean justified = false;
            for (JepsenExport.Fault fault : export.faults) {
                if (fault.justifies().contains(status.refusal()) && fault.index() < status.index()) {
                    justified = true;
                    break;
                }
            }
            if (!justified) {
                violations.add("Operational 1/6: " + process + " refused " + status.refusal()
                        + " (\"" + status.detail() + "\") though no injected fault before it justifies that reason");
            }
        });
    }

    private void orphans() {
        for (JepsenExport.Rec rec : export.records) {
            if (rec.uid() != null && rec.uid().contains(">") && !producerByEffectUid.containsKey(rec.uid())) {
                violations.add("Host obligation 3: " + topicName(rec.topicId()) + "@" + rec.partition() + "@" + rec.offset()
                        + " carries " + rec.uid() + ", which no committed step's trace claims to have sent");
            }
        }
    }

    /**
     * SPEC Assumption 2, judged as the simulator judges it at every commit: a step committed
     * while a received channel's topic is dead and its name resolves to a live other id is a
     * step on the wrong log. On a cluster the host re-creates the task only later, so the
     * judgement starts at the re-initialisation the fault records, if it records one.
     */
    private void recreations() {
        if (export.source == JepsenExport.Source.SIMULATOR) {
            Set<String> flagged = new HashSet<>();
            for (JepsenExport.Reads reads : export.reads) {
                for (Channel channel : receivedAtIndex(reads.taskName(), reads.index())) {
                    String key = reads.taskName() + "/" + channel.topicId();
                    if (!flagged.contains(key) && recreatedAt(channel, reads.index())) {
                        violations.add("Assumption 2: " + reads.taskName() + " committed a step while its received topic "
                                + topicName(channel.topicId()) + " (" + channel + ") had been deleted and recreated"
                                + " under the same name, without failing closed");
                        flagged.add(key);
                    }
                }
            }
            return;
        }
        for (JepsenExport.Fault fault : export.faults) {
            if (!fault.kind().equals("recreate")) {
                continue;
            }
            Object boundary = JepsenEdn.get(fault.details(), "reinitialised-ends");
            String topic = JepsenEdn.string(fault.details(), "topic");
            if (boundary == null || topic == null) {
                continue;
            }
            Map<String, Long> ends = JepsenExport.ends(boundary);
            for (Map.Entry<String, List<JepsenExport.TraceEntry>> byTask : traceByTask.entrySet()) {
                JepsenExport.ProcessDecl decl = export.processes.get(processOf(byTask.getKey()));
                if (decl == null || !decl.receives().contains(topic)) {
                    continue;
                }
                for (JepsenExport.TraceEntry entry : byTask.getValue()) {
                    Long end = ends.get(entry.tp());
                    if (end != null && entry.to() >= end) {
                        violations.add("Assumption 2: " + byTask.getKey() + " delivered " + entry.uid()
                                + " after its received topic " + topic
                                + " was deleted and recreated under the same name, without failing closed");
                        break;
                    }
                }
            }
        }
    }
}
