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
 * The first checker over a {@link JepsenExport}: judges causal order, duplicates, FIFO and
 * expression from ground truth reconstructed here, never from what the engine claims, by
 * the rules the simulator's {@link Oracle} applies, and the same rules as the Clojure
 * checker in {@code parsley-jepsen}, which it cross-checks.
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
 * <p>A causal past is a frontier: for each channel, the greatest position in it, standing
 * for every position on that channel up to it. That is the true past only where a task
 * delivers each channel in position order (Safety 3), which is judged on its own: under
 * FIFO a position is delivered exactly when every record before it on its channel is, so
 * "every cause delivered" is "the greatest cause on each channel delivered", and "every
 * cause expressed" is "the greatest expressed". The simulator's {@link Oracle} keeps the
 * sets themselves, which is exact and quadratic in a run; a frontier is a map of a few
 * channels, which is what lets an hour's trace be judged.
 *
 * <p>The judgements, cited as in {@code SPEC.md}: Safety 1 at delivery time and over
 * delivered pairs, Safety 2, Safety 3, Safety 7, Safety 8, Structural 12, 14 and 15,
 * over-expression, Liveness 1 at quiescence with the spec's exemptions, Host obligations 3
 * and 6 through the effects each step declared, Host obligation 5 through the frontier each
 * step expressed, which never falls along a task's trace unless the host lost committed
 * state, Assumption 2, and Operational 1 and 6: every refusal follows an injected fault that
 * justifies it.
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

    private final Map<Channel, Map<Long, Map<Channel, Long>>> causesByPosition = new HashMap<>();
    private final Map<JepsenExport.Rec, Causes> metaByRecord = new java.util.IdentityHashMap<>();
    private final Map<String, List<Map<Channel, Long>>> pastByTask = new HashMap<>();
    private final Map<String, Map<Channel, Set<Long>>> mergedReceiptsByTask = new HashMap<>();
    private final Map<String, Integer> computing = new HashMap<>();
    /** Per channel, in offset order: the records' offsets, and what the records up to each could let a receiver see expressed. */
    private final Map<Channel, PrefixBounds> namedByChannel = new HashMap<>();
    /** Per task and trace-ends key ({@code lo} or {@code hi}) and trace partition: each observation's end, in index order, or null where they are not ascending. */
    private final Map<String, Map<String, long[]>> endsByTask = new HashMap<>();
    /** Per task and channel: for each k, the merged spans of the first k observations' receipts. */
    private final Map<String, Map<Channel, List<List<Span>>>> prefixSpansByTask = new HashMap<>();
    /** Per task: the delivered positions in trace order, and the first index at which each was delivered. */
    private final Map<String, List<Position>> deliveredByTask = new LinkedHashMap<>();
    private final Map<String, Map<Position, Integer>> firstIndexByTask = new HashMap<>();

    record Position(Channel channel, long offset) {
        @Override
        public String toString() {
            return channel + "@" + offset;
        }
    }

    /**
     * What the first n records of a channel could let a receiver see expressed, for any n:
     * a checkpoint every {@link #EVERY} records and a walk of the rest, which keeps the
     * memory a fraction of a bound per record.
     */
    record PrefixBounds(long[] offsets, List<JepsenExport.Rec> records, List<Map<Channel, Long>> checkpoints) {
        static final int EVERY = 32;

        /** Merges what the first {@code n} records let a receiver see expressed into {@code bound}. */
        void mergeFirst(int n, Channel channel, Map<Channel, Long> bound, JepsenExportOracle oracle) {
            int checkpoint = n / EVERY;
            checkpoints.get(checkpoint).forEach((named, position) -> bound.merge(named, position, Math::max));
            for (int i = checkpoint * EVERY; i < n; i++) {
                oracle.mergeNamed(bound, channel, records.get(i));
            }
        }
    }

    /** Merges one record's offset and the positions its metadata names into {@code bound}. */
    private void mergeNamed(Map<Channel, Long> bound, Channel channel, JepsenExport.Rec rec) {
        bound.merge(channel, rec.offset(), Math::max);
        Causes meta = decodedMeta(rec);
        if (meta != null) {
            meta.byChannel().forEach((named, position) -> bound.merge(named, position, Math::max));
        }
    }

    /** How many of the ascending {@code offsets} are at or below {@code offset}. */
    static int countAtOrBelow(long[] offsets, long offset) {
        int lo = 0;
        int hi = offsets.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (offsets[mid] <= offset) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

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
        for (String task : tasks) {
            finalOrderChecks(task);
        }
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
        recordsByChannel.forEach((channel, records) -> {
            long[] offsets = new long[records.size()];
            List<JepsenExport.Rec> inOrder = new ArrayList<>(records.values());
            List<Map<Channel, Long>> checkpoints = new ArrayList<>();
            checkpoints.add(Map.of());
            Map<Channel, Long> bound = new HashMap<>();
            for (int i = 0; i < inOrder.size(); i++) {
                JepsenExport.Rec rec = inOrder.get(i);
                offsets[i] = rec.offset();
                mergeNamed(bound, channel, rec);
                if ((i + 1) % PrefixBounds.EVERY == 0) {
                    checkpoints.add(Map.copyOf(bound));
                }
            }
            namedByChannel.put(channel, new PrefixBounds(offsets, inOrder, checkpoints));
        });
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

    /** The span of positions on {@code channel} one observation shows the task received, or null. */
    private Span readSpan(String task, JepsenExport.Reads reads, Channel channel) {
        Long next = reads.nextRead().get(channel);
        if (next == null) {
            return null;
        }
        long from = executionStart(task, reads, channel);
        return next > from ? new Span(from, next) : null;
    }

    /*
     * The observations of a task are kept in index order, and the trace ends they carry
     * only grow along it (a cluster's stale observations are set aside first, and the
     * simulator's trace ends are counts), so the observations taken before a step are a
     * prefix of them. Each prefix's receipt spans are merged once and then found by one
     * binary search, which keeps a task's replay linear in its observations.
     */

    /**
     * Each observation's end for trace partition {@code tp}, in index order, under the
     * {@code endsLo} or {@code endsHi} key; an observation that carries no end for it had
     * seen nothing of it, which is 0. Null where they are not ascending.
     */
    private long[] endsVector(String task, boolean hi, String tp) {
        String key = (hi ? "hi:" : "lo:") + tp;
        Map<String, long[]> byKey = endsByTask.computeIfAbsent(task, t -> new HashMap<>());
        if (byKey.containsKey(key)) {
            return byKey.get(key);
        }
        List<JepsenExport.Reads> reads = readsByTask.getOrDefault(task, List.of());
        long[] ends = new long[reads.size()];
        boolean ascending = true;
        for (int i = 0; i < reads.size(); i++) {
            Map<String, Long> map = hi ? reads.get(i).endsHi() : reads.get(i).endsLo();
            ends[i] = map.getOrDefault(tp, 0L);
            ascending &= i == 0 || ends[i - 1] <= ends[i];
        }
        byKey.put(key, ascending ? ends : null);
        return byKey.get(key);
    }

    /** For each k, the merged spans of the first k observations' receipts on {@code channel}. */
    private List<List<Span>> prefixSpans(String task, Channel channel) {
        return prefixSpansByTask.computeIfAbsent(task, t -> new HashMap<>()).computeIfAbsent(channel, c -> {
            List<List<Span>> prefixes = new ArrayList<>();
            List<Span> merged = List.of();
            prefixes.add(merged);
            for (JepsenExport.Reads reads : readsByTask.getOrDefault(task, List.of())) {
                Span span = readSpan(task, reads, channel);
                if (span != null) {
                    List<Span> all = new ArrayList<>(merged);
                    all.add(span);
                    merged = merged(all);
                }
                prefixes.add(merged);
            }
            return prefixes;
        });
    }

    /**
     * Positions the task is known to have received on {@code channel} before {@code entry}:
     * for every observation taken before the step, the span from where its execution began
     * reading the channel to the position it committed, merged. Empty when no observation
     * covers it, which under-approximates receipt.
     */
    private List<Span> receivedSpansBefore(String task, Channel channel, JepsenExport.TraceEntry entry) {
        long[] ends = endsVector(task, true, entry.tp());
        if (ends == null) {
            List<Span> spans = new ArrayList<>();
            for (JepsenExport.Reads reads : readsByTask.getOrDefault(task, List.of())) {
                Long end = reads.endsHi().get(entry.tp());
                Span span = readSpan(task, reads, channel);
                if (end != null && end <= entry.to() && span != null) {
                    spans.add(span);
                }
            }
            return merged(spans);
        }
        return prefixSpans(task, channel).get(countAtOrBelow(ends, entry.to()));
    }

    /** Every span of positions the task is known to have received on {@code channel} at any time, merged. */
    private List<Span> receivedSpansEver(String task, Channel channel) {
        List<List<Span>> prefixes = prefixSpans(task, channel);
        return prefixes.get(prefixes.size() - 1);
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
        List<JepsenExport.Reads> reads = readsByTask.getOrDefault(task, List.of());
        long[] ends = endsVector(task, false, entry.tp());
        JepsenExport.Reads first = null;
        if (ends == null) {
            for (JepsenExport.Reads candidate : reads) {
                Long end = candidate.endsLo().get(entry.tp());
                if (end != null && end > entry.to()) {
                    first = candidate;
                    break;
                }
            }
        } else {
            int k = countAtOrBelow(ends, entry.to());
            first = k < reads.size() ? reads.get(k) : null;
        }
        if (first == null) {
            spans.add(new Span(0, Long.MAX_VALUE));
        } else {
            Span span = readSpan(task, first, channel);
            if (span != null) {
                spans.add(span);
            }
        }
        return spans;
    }

    // ---- ground truth: true causes, as frontiers ----

    private static void mergeFrontier(Map<Channel, Long> into, Map<Channel, Long> other) {
        other.forEach((channel, position) -> into.merge(channel, position, Math::max));
    }

    /**
     * The true causes of the record at a position, as a frontier: the record an external
     * stamper observed and its causes, or the producing task's past once its step delivered.
     */
    private Map<Channel, Long> trueCausesOf(Channel channel, long offset, String uidHint) {
        Map<Long, Map<Channel, Long>> byOffset = causesByPosition.computeIfAbsent(channel, c -> new HashMap<>());
        Map<Channel, Long> known = byOffset.get(offset);
        if (known != null) {
            return known;
        }
        JepsenExport.Rec rec = recordAt(channel, offset);
        String uid = rec != null && rec.uid() != null ? rec.uid() : uidHint;
        Map<Channel, Long> causes;
        if (rec != null && rec.stampedFrom() != null) {
            JepsenExport.Position from = rec.stampedFrom();
            if (recordAt(from.channel(), from.offset()) == null) {
                causes = Map.of();
            } else {
                causes = new HashMap<>(trueCausesOf(from.channel(), from.offset(), null));
                causes.merge(from.channel(), from.offset(), Math::max);
                causes = Map.copyOf(causes);
            }
        } else {
            JepsenExport.TraceEntry producer = uid == null ? null : producerByEffectUid.get(uid);
            causes = producer == null ? Map.of() : pastAfter(producer);
        }
        byOffset.put(offset, causes);
        return causes;
    }

    /**
     * The producing task's causal past once its step {@code entry} had delivered: every
     * earlier delivery and its causes, the causes of every record known received before
     * the step, and the delivery itself with its causes. One frontier per step.
     */
    private Map<Channel, Long> pastAfter(JepsenExport.TraceEntry entry) {
        String task = entry.taskName();
        int step = stepOf.get(entry);
        List<Map<Channel, Long>> snapshots = pastByTask.computeIfAbsent(task, t -> new ArrayList<>());
        Integer inProgress = computing.get(task);
        if (inProgress != null && step >= inProgress) {
            violations.add("Causal cycle: " + describe(entry) + " is among the causes of what it delivered");
            return snapshots.isEmpty() ? Map.of() : snapshots.get(snapshots.size() - 1);
        }
        List<JepsenExport.TraceEntry> entries = traceByTask.get(task);
        Map<Channel, Set<Long>> merged = mergedReceiptsByTask.computeIfAbsent(task, t -> new HashMap<>());
        while (snapshots.size() <= step) {
            int next = snapshots.size();
            computing.put(task, next);
            JepsenExport.TraceEntry delivered = entries.get(next);
            Map<Channel, Long> past = new HashMap<>(next == 0 ? Map.of() : snapshots.get(next - 1));
            for (Channel channel : receivedEver(task)) {
                TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
                if (records == null) {
                    continue;
                }
                Set<Long> done = merged.computeIfAbsent(channel, c -> new HashSet<>());
                for (Span span : receivedSpansBefore(task, channel, delivered)) {
                    for (JepsenExport.Rec rec : records.subMap(span.from(), span.to()).values()) {
                        if (done.add(rec.offset())) {
                            mergeFrontier(past, trueCausesOf(rec.channel(), rec.offset(), rec.uid()));
                        }
                    }
                }
            }
            past.merge(delivered.channel(), delivered.offset(), Math::max);
            mergeFrontier(past, trueCausesOf(delivered.channel(), delivered.offset(), delivered.uid()));
            computing.remove(task);
            snapshots.add(Map.copyOf(past));
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
            PrefixBounds prefix = namedByChannel.get(channel);
            for (Span span : spans) {
                if (span.from() <= prefix.offsets()[0]) {
                    // A span from the channel's first record is answered from the prefix bounds.
                    prefix.mergeFirst(countAtOrBelow(prefix.offsets(), span.to() - 1), channel, bound, this);
                } else {
                    for (JepsenExport.Rec rec : records.subMap(span.from(), span.to()).values()) {
                        mergeNamed(bound, channel, rec);
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
        List<Position> delivered = new ArrayList<>();
        Map<Position, Integer> firstIndex = new HashMap<>();
        deliveredByTask.put(task, delivered);
        firstIndexByTask.put(task, firstIndex);
        /*
         * Mirrors the engine's persisted delivered-past clamp: delivered positions merged
         * with each delivered message's expressed frontier, coarser than the true causes and
         * deliberately so, because the engine's sanctioned drops are judged by expression.
         */
        Map<Channel, Long> enginePast = new HashMap<>();
        Map<Channel, Long> maxDelivered = new HashMap<>();
        Map<Channel, Long> expressed = new HashMap<>();
        for (JepsenExport.TraceEntry entry : traceByTask.getOrDefault(task, List.of())) {
            Position pos = new Position(entry.channel(), entry.offset());
            String described = entry.uid() + "(" + pos + ")";
            JepsenExport.Rec rec = recordAt(entry.channel(), entry.offset());
            if (rec != null && !Objects.equals(rec.uid(), entry.uid())) {
                violations.add("Trace: " + task + " reports delivering " + entry.uid() + " at " + pos
                        + " but the committed record there carries " + rec.uid());
            }
            if (rec == null && !dead(entry.channel()) && logStart(entry.channel()) <= entry.offset()) {
                violations.add("Trace: " + task + " reports delivering " + pos
                        + " (" + entry.uid() + ") but no committed record exists there");
            }
            if (undecodableAt.getOrDefault(entry.channel(), Set.of()).contains(entry.offset())) {
                violations.add("Safety 7: " + task + " delivered " + described
                        + " whose causal metadata is present and undecodable");
            }
            Set<Channel> receivedNow = receivedAt(task, entry);
            if (!receivedNow.contains(entry.channel())) {
                violations.add("Declaration: " + task + " delivered " + described + " from a channel it does not receive");
            }
            /*
             * Safety 1 at this very moment: the greatest cause on every channel must already be
             * delivered here, settled by evidence the world corroborates, or lie within the
             * delivered past the engine is entitled to drop behind. Everything delivered lies
             * within the engine's past, so a cause not within it was not delivered either. The
             * end-of-run check compares delivered pairs only, so a premature delivery whose
             * cause never delivers is visible only here.
             */
            Map<Channel, Long> causes = trueCausesOf(entry.channel(), entry.offset(), entry.uid());
            Set<Channel> settled = settledNow(task, causes, entry, receivedNow, start, maxDelivered);
            causes.forEach((channel, offset) -> {
                if (settled.contains(channel)) {
                    return;
                }
                long bound = enginePast.getOrDefault(channel, Long.MIN_VALUE);
                if (offset > bound) {
                    violations.add("Safety 1 (delivery-time): " + task + " delivered " + described
                            + " while its cause " + new Position(channel, offset)
                            + " was neither delivered, nor settled by evidence, nor within the delivered past (bound: "
                            + (bound == Long.MIN_VALUE ? "none" : bound) + ")");
                }
            });
            delivered.add(pos);
            firstIndex.putIfAbsent(pos, delivered.size() - 1);
            maxDelivered.merge(entry.channel(), entry.offset(), Math::max);
            enginePast.merge(entry.channel(), entry.offset(), Math::max);
            Causes recMeta = rec == null ? null : decodedMeta(rec);
            if (recMeta != null) {
                recMeta.byChannel().forEach((channel, position) -> enginePast.merge(channel, position, Math::max));
            }

            Map<Channel, Long> upper = expressionBound(task, entry, maxDelivered);
            Map<Channel, Long> past = pastAfter(entry);
            Set<Channel> excused = new HashSet<>();
            for (Channel channel : past.keySet()) {
                if (dead(channel)) {
                    excused.add(channel);
                }
            }
            Causes traceMeta = decode(entry.causesHeader());
            if (traceMeta == null) {
                violations.add("Trace: the frontier " + task + " expressed at step " + entry.tp() + "@" + entry.to()
                        + " is undecodable");
                traceMeta = Causes.none();
            }
            /*
             * A task's expressed frontier only grows along its own trace: it is its committed
             * state, and a step that expresses less than an earlier committed step did resumed
             * from a state older than the one committed, which the host lost. A channel that
             * no longer exists is the one exception: its causes no longer matter and may be
             * discarded (Structural 13).
             */
            for (Map.Entry<Channel, Long> named : traceMeta.byChannel().entrySet()) {
                Long before = expressed.get(named.getKey());
                if (before != null && named.getValue() < before && !dead(named.getKey())) {
                    violations.add("Host obligation 5: " + task + " expressed " + named.getKey() + "@" + named.getValue()
                            + " at step " + entry.tp() + "@" + entry.to() + " after expressing " + before
                            + " at an earlier committed step: it resumed from a state older than the one it had committed,"
                            + " which the host lost");
                }
            }
            traceMeta.byChannel().forEach((channel, position) -> expressed.merge(channel, position, Math::max));
            Position tracePos = new Position(traceChannel(entry), entry.to());
            checkExpression("trace:" + task + ":" + entry.to() + "(" + tracePos + ")", tracePos, traceMeta, past, upper, excused);
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
                    Position sent = new Position(match.channel(), match.offset());
                    Causes meta = decodedMeta(match);
                    if (meta == null) {
                        violations.add("Trace: the frontier expressed by " + sent + " is undecodable");
                        continue;
                    }
                    checkExpression(match.uid() + "(" + sent + ")", sent, meta, past, upper, excused);
                }
            }
        }
    }

    /**
     * The expression checks on one send: Structural 14 and 12, over-expression, Structural
     * 15. {@code past} is the sender's causal past as a frontier and {@code excused} the
     * channels in it that no longer exist. With {@code upper} null, what the sender could
     * have seen expressed is not known, because records it received are gone from the
     * export, and the two checks that need it are not made.
     */
    private void checkExpression(String instance, Position sent, Causes meta, Map<Channel, Long> past,
                                 Map<Channel, Long> upper, Set<Channel> excused) {
        meta.byChannel().forEach((channel, position) -> {
            if (channel.equals(sent.channel()) && position >= sent.offset()) {
                violations.add("Structural 14: " + instance
                        + " expresses dependency on own channel at or above itself: " + position);
            }
            if (upper == null) {
                return;
            }
            Long last = lastAssigned.get(channel);
            Long bound = upper.get(channel);
            /*
             * Structural 12 lets a process express a position it learned from the metadata of a
             * message it received, assigned or not: an out-of-contract stamp naming the log end
             * is held, and its position travels in the holder's frontier meanwhile. The bound
             * covers what the sender had received, so a position within it was learned.
             */
            if (last != null && position > last && (bound == null || position > bound)) {
                violations.add("Structural 12: " + instance + " expresses position " + channel + "@" + position
                        + " which was unassigned at send time (last assigned: " + last + ")");
            }
            if (bound == null || position > bound) {
                violations.add("Over-expression: " + instance + " expresses " + channel + "@" + position
                        + " above anything its sender had delivered or seen expressed at send time (bound: "
                        + bound + ")");
            }
        });
        past.forEach((channel, offset) -> {
            if (excused.contains(channel)) {
                return;
            }
            Long expressed = meta.byChannel().get(channel);
            if (expressed == null || expressed < offset) {
                violations.add("Structural 15: " + instance + " fails to express cause " + new Position(channel, offset)
                        + " (expressed: " + expressed + ")");
            }
        });
    }

    /**
     * The channels of the delivered message's causes settled by evidence at this moment:
     * a channel the task does not receive, one whose cause lies below the task's start
     * position, or a dead channel the task is not known to have received the cause from. A
     * cause the task had received is never settled by its channel's death: the task owes
     * its delivery (Safety 9).
     */
    private Set<Channel> settledNow(String task, Map<Channel, Long> causes, JepsenExport.TraceEntry entry,
                                    Set<Channel> received, Map<Channel, Long> start, Map<Channel, Long> maxDelivered) {
        Set<Channel> settled = new HashSet<>();
        causes.forEach((channel, offset) -> {
            if (!received.contains(channel)) {
                settled.add(channel);
            } else if (offset < start.getOrDefault(channel, 0L)) {
                settled.add(channel);
            } else if (dead(channel) && !receivedBefore(task, channel, offset, entry, maxDelivered)) {
                settled.add(channel);
            }
        });
        return settled;
    }

    private boolean receivedBefore(String task, Channel channel, long offset, JepsenExport.TraceEntry entry,
                                   Map<Channel, Long> maxDelivered) {
        Long delivered = maxDelivered.get(channel);
        if (delivered != null && delivered > offset) {
            return true;
        }
        for (Span span : receivedSpansBefore(task, channel, entry)) {
            if (span.contains(offset)) {
                return true;
            }
        }
        return false;
    }

    // ---- final judgements ----

    /** Safety 2, Safety 3 and Safety 1 over the task's committed deliveries. */
    private void finalOrderChecks(String task) {
        List<Position> delivered = deliveredByTask.getOrDefault(task, List.of());
        Map<Position, Integer> firstIndex = firstIndexByTask.getOrDefault(task, Map.of());
        for (int i = 0; i < delivered.size(); i++) {
            int previous = firstIndex.get(delivered.get(i));
            if (previous != i) {
                violations.add("Safety 2: " + task + " delivered " + delivered.get(i) + " twice (indexes "
                        + previous + " and " + i + ")");
            }
        }
        Map<Channel, Long> lastPerChannel = new HashMap<>();
        for (Position pos : delivered) {
            Long last = lastPerChannel.put(pos.channel(), pos.offset());
            if (last != null && pos.offset() <= last) {
                violations.add("Safety 3: " + task + " delivered " + pos + " after position " + last
                        + " of the same channel");
            }
        }
        /*
         * Per channel, the delivered positions in order with the latest first-delivery index
         * among those up to each: a frontier's cause on that channel was delivered after the
         * effect exactly when that index is past the effect's.
         */
        Map<Channel, long[]> offsetsByChannel = new HashMap<>();
        Map<Channel, int[]> latestByChannel = new HashMap<>();
        Map<Channel, TreeMap<Long, Integer>> byChannel = new HashMap<>();
        firstIndex.forEach((pos, index) ->
                byChannel.computeIfAbsent(pos.channel(), c -> new TreeMap<>()).put(pos.offset(), index));
        byChannel.forEach((channel, positions) -> {
            long[] offsets = new long[positions.size()];
            int[] latest = new int[positions.size()];
            int i = 0;
            int max = -1;
            for (Map.Entry<Long, Integer> e : positions.entrySet()) {
                offsets[i] = e.getKey();
                max = Math.max(max, e.getValue());
                latest[i++] = max;
            }
            offsetsByChannel.put(channel, offsets);
            latestByChannel.put(channel, latest);
        });
        for (int i = 0; i < delivered.size(); i++) {
            Position effect = delivered.get(i);
            int effectIndex = i;
            trueCausesOf(effect.channel(), effect.offset(), null).forEach((channel, offset) -> {
                long[] offsets = offsetsByChannel.get(channel);
                if (offsets == null) {
                    return;
                }
                int n = countAtOrBelow(offsets, offset);
                if (n == 0) {
                    return;
                }
                int causeIndex = latestByChannel.get(channel)[n - 1];
                if (causeIndex > effectIndex) {
                    violations.add("Safety 1: " + task + " delivered effect " + effect + " (index " + effectIndex
                            + ") before its cause " + delivered.get(causeIndex) + " (index " + causeIndex + ")");
                }
            });
        }
    }

    private boolean refused(String task) {
        return firstRefusalByProcess.containsKey(processOf(task));
    }

    private void liveness(String task) {
        if (refused(task)) {
            return;
        }
        Map<Channel, Long> start = startByTask.getOrDefault(task, Map.of());
        Set<Position> exempt = exemptions(task, receivedFinally(task), start);
        Set<Position> deliveredSet = new HashSet<>(deliveredByTask.getOrDefault(task, List.of()));
        owed(task).forEach((channel, records) -> {
            for (JepsenExport.Rec rec : records) {
                Position pos = new Position(rec.channel(), rec.offset());
                if (!deliveredSet.contains(pos) && !exempt.contains(pos)) {
                    violations.add("Liveness 1: " + task + " never delivered " + rec.uid() + "(" + pos
                            + "), which it received on a channel it declares at or above its start position");
                }
            }
        });
    }

    /**
     * Records a task is not owed at quiescence: those held behind an undecodable header on
     * their channel (Safety 7 and 3), and those held behind a stamp naming a position no
     * later record on that channel settles (wire-format constraint 8), transitively.
     */
    private Set<Position> exemptions(String task, Set<Channel> received, Map<Channel, Long> start) {
        Set<Position> exempt = new HashSet<>();
        Map<Channel, List<JepsenExport.Rec>> owed = new HashMap<>();
        for (Channel channel : received) {
            TreeMap<Long, JepsenExport.Rec> records = recordsByChannel.get(channel);
            if (records == null) {
                continue;
            }
            List<JepsenExport.Rec> onChannel = new ArrayList<>();
            boolean behindUndecodable = false;
            for (JepsenExport.Rec rec : records.tailMap(start.getOrDefault(channel, 0L)).values()) {
                onChannel.add(rec);
                if (undecodableAt.getOrDefault(channel, Set.of()).contains(rec.offset())) {
                    behindUndecodable = true;
                }
                if (behindUndecodable) {
                    exempt.add(new Position(rec.channel(), rec.offset()));
                }
            }
            owed.put(channel, onChannel);
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Map.Entry<Channel, List<JepsenExport.Rec>> entry : owed.entrySet()) {
                boolean behindExempt = false;
                for (JepsenExport.Rec rec : entry.getValue()) {
                    Position pos = new Position(rec.channel(), rec.offset());
                    if (exempt.contains(pos)) {
                        behindExempt = true;
                        continue;
                    }
                    if (behindExempt || heldByUnsettledStamp(rec, received, start, exempt, owed)) {
                        exempt.add(pos);
                        behindExempt = true;
                        changed = true;
                    }
                }
            }
        }
        return exempt;
    }

    private boolean heldByUnsettledStamp(JepsenExport.Rec rec, Set<Channel> received, Map<Channel, Long> start,
                                         Set<Position> exempt, Map<Channel, List<JepsenExport.Rec>> owed) {
        Causes meta = decodedMeta(rec);
        if (meta == null) {
            return false;
        }
        for (Map.Entry<Channel, Long> named : meta.byChannel().entrySet()) {
            Channel channel = named.getKey();
            long position = named.getValue();
            if (!received.contains(channel) || dead(channel) || position < start.getOrDefault(channel, 0L)) {
                continue;
            }
            Long last = lastAssigned.get(channel);
            if (last == null || last < position) {
                return true;
            }
            for (JepsenExport.Rec candidate : owed.getOrDefault(channel, List.of())) {
                if (candidate.offset() <= position && exempt.contains(new Position(candidate.channel(), candidate.offset()))) {
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
