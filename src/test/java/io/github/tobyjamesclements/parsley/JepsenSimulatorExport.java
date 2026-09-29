package io.github.tobyjamesclements.parsley;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.SimWorld.SimChannel;

/**
 * Exports simulator runs into the {@link JepsenExport} format, so the checker in
 * {@code parsley-jepsen} can be calibrated against every {@link Sabotage} mode before it
 * ever sees a cluster: a checker is worth nothing until it has caught something.
 *
 * <p>An observer on {@link Scenario} and {@link SimProcess} turns the simulated world into
 * what a cluster run yields: every committed record with its header, one trace entry per
 * committed delivery carrying the frontier the step expressed and the sends it made, the
 * read positions at every commit, the refusals, and the faults with the refusals each
 * justifies. Each simulated process is one task, so its trace partition is its name.
 */
final class JepsenSimulatorExport implements Scenario.Observer {

    /** One export with the outcome the checker must reach on it. */
    record Calibration(String file, String mode, String hostFault, long seed, boolean expectClean,
                       List<String> oracleViolations) {
    }

    private final JepsenExport export = new JepsenExport();
    private long index;
    private final Map<String, Long> traceEnds = new TreeMap<>();
    private final Map<String, List<JepsenExport.TraceEntry>> pendingByProcess = new HashMap<>();
    private final Map<Instance, Instance> observedByInstance = new IdentityHashMap<>();
    private final Set<String> started = new LinkedHashSet<>();

    JepsenSimulatorExport(long seed, SabotageMode mode, SimProcess.HostFault hostFault) {
        export.source = JepsenExport.Source.SIMULATOR;
        export.seed = seed;
        export.sabotage = mode.name();
        export.hostFault = hostFault == SimProcess.HostFault.NONE ? null : hostFault.name();
    }

    JepsenExport export() {
        return export;
    }

    private Map<String, Long> ends() {
        return new TreeMap<>(traceEnds);
    }

    private final Map<String, Set<Channel>> declaredByProcess = new HashMap<>();
    private final Map<String, Map<Channel, Long>> startByProcess = new HashMap<>();
    private final Map<String, Map<Channel, Long>> executionStartByProcess = new HashMap<>();

    @Override
    public void started(String process, Map<Channel, Long> startPositions) {
        traceEnds.putIfAbsent(process, 0L);
        executionStartByProcess.put(process, new TreeMap<>(startPositions));
        List<Channel> receives = new ArrayList<>(new TreeMap<>(startPositions).keySet());
        if (started.add(process)) {
            export.tasks.add(new JepsenExport.TaskInfo(process, 0, receives));
        }
        /*
         * The position a channel is first read from, recorded once per channel as the
         * simulated host's initialNextRead is: a channel joining the declaration later
         * starts where the host then reads it.
         */
        Map<Channel, Long> known = startByProcess.computeIfAbsent(process, p -> new TreeMap<>());
        Map<Channel, Long> fresh = new TreeMap<>();
        startPositions.forEach((channel, position) -> {
            if (known.putIfAbsent(channel, position) == null) {
                fresh.put(channel, position);
            }
        });
        if (!fresh.isEmpty()) {
            export.startPositions.add(new JepsenExport.StartPositions(process, 0, fresh));
        }
        Set<Channel> previous = declaredByProcess.put(process, new LinkedHashSet<>(receives));
        if (previous == null || !previous.equals(new LinkedHashSet<>(receives))) {
            Map<Object, Object> details = new LinkedHashMap<>();
            details.put(JepsenEdn.kw("process"), process);
            details.put(JepsenEdn.kw("receives"), JepsenExport.channels(receives));
            export.faults.add(new JepsenExport.Fault(index++, null, "declared", new LinkedHashSet<>(), ends(), details));
        }
        export.statuses.add(new JepsenExport.Status(index++, null, process, ProcessStatus.Lifecycle.RUNNING, null,
                null, ends()));
    }

    @Override
    public void delivered(String process, Instance instance, byte[] causesHeader, List<String> effectTopics,
                          List<String> effectUids) {
        List<JepsenExport.Effect> effects = new ArrayList<>();
        for (int i = 0; i < effectUids.size(); i++) {
            effects.add(new JepsenExport.Effect(effectTopics.get(i), effectUids.get(i)));
        }
        List<JepsenExport.TraceEntry> pending = pendingByProcess.computeIfAbsent(process, p -> new ArrayList<>());
        long to = traceEnds.getOrDefault(process, 0L) + pending.size();
        pending.add(new JepsenExport.TraceEntry(process, 0, process, to, instance.channel.topicId(),
                instance.channel.partition(), instance.position, instance.uid, causesHeader, effects));
    }

    @Override
    public void committed(String process, Map<Channel, Long> committedNextRead) {
        List<JepsenExport.TraceEntry> pending = pendingByProcess.remove(process);
        if (pending != null) {
            export.trace.addAll(pending);
            traceEnds.merge(process, (long) pending.size(), Long::sum);
        }
        export.reads.add(new JepsenExport.Reads(index++, null, process, 0, ends(), ends(),
                new TreeMap<>(committedNextRead), executionStartByProcess.getOrDefault(process, Map.of())));
    }

    @Override
    public void aborted(String process) {
        pendingByProcess.remove(process);
    }

    @Override
    public void refused(String process, FailClosedException failure) {
        export.statuses.add(new JepsenExport.Status(index++, null, process, ProcessStatus.Lifecycle.STOPPED,
                failure.reason(), failure.getMessage(), ends()));
    }

    @Override
    public void externalProduced(SimChannel channel, Instance instance, Instance observed) {
        if (observed != null) {
            observedByInstance.put(instance, observed);
        }
    }

    @Override
    public void fault(String kind, SimChannel channel, String process, Map<String, Object> details,
                      Set<FailClosedException.Reason> justifies) {
        Map<Object, Object> edn = new LinkedHashMap<>();
        if (channel != null) {
            edn.put(JepsenEdn.kw("channel"), JepsenExport.channel(channel.id()));
        }
        if (process != null) {
            edn.put(JepsenEdn.kw("process"), process);
        }
        details.forEach((key, value) -> edn.put(JepsenEdn.kw(key), detail(value)));
        export.faults.add(new JepsenExport.Fault(index++, null, kind, new LinkedHashSet<>(justifies), ends(), edn));
    }

    private static Object detail(Object value) {
        if (value instanceof Channel channel) {
            return JepsenExport.channel(channel);
        }
        if (value instanceof SimChannel channel) {
            return JepsenExport.channel(channel.id());
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>();
            for (Object item : list) {
                out.add(detail(item));
            }
            return out;
        }
        return value;
    }

    @Override
    public void finished(SimWorld world, List<SimProcess> processes) {
        for (SimProcess process : processes) {
            List<String> receives = new ArrayList<>();
            for (SimChannel channel : process.receivedChannels()) {
                receives.add(channel.topicName);
            }
            List<String> sends = new ArrayList<>();
            for (SimChannel channel : process.sendChannels()) {
                sends.add(channel.topicName);
            }
            export.processes.put(process.name, new JepsenExport.ProcessDecl(process.name,
                    new ArrayList<>(new LinkedHashSet<>(receives)), new ArrayList<>(new LinkedHashSet<>(sends))));
        }
        Map<UUID, List<SimChannel>> byTopic = new LinkedHashMap<>();
        for (SimChannel channel : world.allChannels()) {
            byTopic.computeIfAbsent(channel.id().topicId(), t -> new ArrayList<>()).add(channel);
        }
        byTopic.forEach((topicId, channels) -> {
            Map<Integer, Long> logStart = new TreeMap<>();
            boolean alive = true;
            for (SimChannel channel : channels) {
                logStart.put(channel.id().partition(), channel.logStart);
                alive &= !channel.dead;
            }
            export.topics.add(new JepsenExport.TopicInfo(topicId, channels.get(0).topicName, channels.size(), alive,
                    logStart));
            for (SimChannel channel : channels) {
                for (int position = 0; position < channel.slots.size(); position++) {
                    if (channel.slots.get(position) instanceof SimWorld.MessageSlot slot) {
                        Instance instance = slot.instance();
                        Instance observed = observedByInstance.get(instance);
                        export.records.add(new JepsenExport.Rec(topicId, channel.id().partition(), position,
                                text(instance.key), text(instance.value), instance.uid, causesHeader(instance),
                                observed == null ? null : new JepsenExport.Position(observed.channel.topicId(),
                                        observed.channel.partition(), observed.position)));
                    }
                }
            }
        });
    }

    private static String text(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] causesHeader(Instance instance) {
        for (Header header : instance.headers) {
            if (header.key().equals(CausesCodec.HEADER_KEY)) {
                return header.value() == null ? JepsenExport.NULL_HEADER_VALUE : header.value();
            }
        }
        return null;
    }

    // ---- running scenarios under observation ----

    /** Runs one seeded scenario and returns its export beside the simulator's own verdict. */
    static Map.Entry<JepsenExport, Scenario.Result> run(long seed, SabotageMode mode, SimProcess.HostFault hostFault) {
        JepsenSimulatorExport observer = new JepsenSimulatorExport(seed, mode, hostFault);
        Scenario.Result result = Scenario.run(seed, mode, hostFault, observer);
        return Map.entry(observer.export(), result);
    }

    /**
     * The deterministic scenario for {@code DELIVER_PAST_DEAD_HOLDS}, which no random seed
     * reaches: {@code SabotageMetaTest} constructs the same inversion. Under the honest
     * engine the same steps refuse at p's re-initialisation instead.
     */
    static JepsenExport deadHoldsScenario(SabotageMode mode) {
        JepsenSimulatorExport observer = new JepsenSimulatorExport(0, mode, SimProcess.HostFault.NONE);
        SimWorld world = new SimWorld(7);
        Oracle oracle = new Oracle();
        SimChannel chanA = world.createChannel("chanA");
        SimChannel chanB = world.createChannel("chanB");
        SimChannel cq = chanA.id().compareTo(chanB.id()) < 0 ? chanA : chanB;
        SimChannel cx = cq == chanA ? chanB : chanA;
        SimChannel c9 = world.createChannel("c9");
        SimChannel ct = world.createChannel("ct");
        SimProcess q = new SimProcess("q", world, oracle, List.of(cx, ct), List.of(cq),
                d -> d.uid.equals("T") ? List.of(cq) : List.of(), mode);
        SimProcess p = new SimProcess("p", world, oracle, List.of(cx, c9, cq), List.of(), d -> List.of(), mode);
        q.observe(observer);
        p.observe(observer);
        q.start();
        p.start();

        Instance n0 = external(world, c9, "N0");
        observer.externalProduced(c9, n0, null);
        Instance n1 = external(world, c9, "N1");
        observer.externalProduced(c9, n1, null);
        Map<Channel, Long> meta = new TreeMap<>(Map.of(n1.channel, n1.position));
        byte[] header = CausesCodec.encode(Causes.of(meta));
        Instance x1 = world.appendExternal(cx, (channel, pos) -> new Instance(channel, pos, "X1", "X1".getBytes(),
                "X1".getBytes(), List.of(new Header(CausesCodec.HEADER_KEY, header)), Causes.of(meta), Set.of(n1)));
        observer.externalProduced(cx, x1, n1);
        Instance t = external(world, ct, "T");
        observer.externalProduced(ct, t, null);

        q.feedOne(cx);
        q.drain();
        q.commitStep();
        p.feedOne(cx);
        p.drain();
        p.commitStep();

        world.killChannel(cx);
        observer.fault("kill", cx, null, Map.of("topic", cx.topicName),
                Set.of(FailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES));
        q.reinitialise();
        q.feedOne(ct);
        q.drain();
        q.commitStep();

        try {
            p.reinitialise();
            p.feedOne(cq);
            p.feedOne(c9);
            p.feedOne(c9);
            p.drain();
            p.commitStep();
        } catch (FailClosedException e) {
            p.failClosed(e);
        }
        observer.finished(world, List.of(q, p));
        return observer.export();
    }

    private static Instance external(SimWorld world, SimChannel target, String uid) {
        return world.appendExternal(target, (channel, pos) -> new Instance(channel, pos, uid, uid.getBytes(),
                uid.getBytes(), List.of(), Causes.none(), Set.of()));
    }

    /**
     * Writes the calibration set: honest runs the checker must pass, and for every sabotage
     * mode and the host fault, runs the simulator's oracle caught which the checker must
     * catch too. {@code index.edn} lists each file with its expected outcome and the
     * calibration figures: how many of the seeds tried the simulator caught, and how many of
     * those the export-based replay ({@link JepsenExportOracle}) also caught, which bounds
     * what any checker over the export can see.
     */
    static List<Calibration> writeCalibrationSet(Path dir, int seeds) throws IOException {
        Files.createDirectories(dir);
        List<Calibration> calibrations = new ArrayList<>();
        List<Object> indexEntries = new ArrayList<>();
        for (long seed = 1; seed <= Math.min(seeds, 6); seed++) {
            Map.Entry<JepsenExport, Scenario.Result> run = run(seed, SabotageMode.NONE, SimProcess.HostFault.NONE);
            String file = "honest-seed-" + seed + ".edn";
            run.getKey().writeTo(dir.resolve(file));
            calibrations.add(new Calibration(file, "NONE", null, seed, true, run.getValue().violations()));
            indexEntries.add(JepsenEdn.map("file", file, "mode", JepsenEdn.kw("NONE"), "host-fault", null,
                    "seed", seed, "expect-clean", true));
        }
        List<Map.Entry<SabotageMode, SimProcess.HostFault>> faults = new ArrayList<>();
        for (SabotageMode mode : SabotageMode.values()) {
            if (mode != SabotageMode.NONE && mode != SabotageMode.DELIVER_PAST_DEAD_HOLDS) {
                faults.add(Map.entry(mode, SimProcess.HostFault.NONE));
            }
        }
        faults.add(Map.entry(SabotageMode.NONE, SimProcess.HostFault.RESET_PAST_LOG_START));
        for (Map.Entry<SabotageMode, SimProcess.HostFault> fault : faults) {
            String label = fault.getValue() == SimProcess.HostFault.NONE ? fault.getKey().name() : fault.getValue().name();
            int simulatorCaught = 0;
            int exportCaught = 0;
            int written = 0;
            for (long seed = 1; seed <= seeds; seed++) {
                Map.Entry<JepsenExport, Scenario.Result> run = run(seed, fault.getKey(), fault.getValue());
                if (run.getValue().clean()) {
                    continue;
                }
                simulatorCaught++;
                JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(run.getKey());
                if (verdict.clean()) {
                    continue;
                }
                exportCaught++;
                if (written >= 3) {
                    continue;
                }
                String file = label + "-seed-" + seed + ".edn";
                run.getKey().writeTo(dir.resolve(file));
                calibrations.add(new Calibration(file, fault.getKey().name(),
                        fault.getValue() == SimProcess.HostFault.NONE ? null : fault.getValue().name(), seed, false,
                        run.getValue().violations()));
                indexEntries.add(JepsenEdn.map("file", file, "mode", JepsenEdn.kw(fault.getKey().name()),
                        "host-fault", fault.getValue() == SimProcess.HostFault.NONE ? null : JepsenEdn.kw(fault.getValue().name()),
                        "seed", seed, "expect-clean", false,
                        "simulator-violations", run.getValue().violations()));
                written++;
            }
            indexEntries.add(JepsenEdn.map("calibration", JepsenEdn.kw(label), "seeds-tried", (long) seeds,
                    "simulator-caught", (long) simulatorCaught, "export-caught", (long) exportCaught));
        }
        JepsenExport deadHolds = deadHoldsScenario(SabotageMode.DELIVER_PAST_DEAD_HOLDS);
        String file = "DELIVER_PAST_DEAD_HOLDS-scenario.edn";
        deadHolds.writeTo(dir.resolve(file));
        calibrations.add(new Calibration(file, "DELIVER_PAST_DEAD_HOLDS", null, 0, false, List.of()));
        indexEntries.add(JepsenEdn.map("file", file, "mode", JepsenEdn.kw("DELIVER_PAST_DEAD_HOLDS"), "host-fault", null,
                "seed", 0L, "expect-clean", false));
        JepsenExport deadHoldsHonest = deadHoldsScenario(SabotageMode.NONE);
        String honestFile = "honest-dead-holds-scenario.edn";
        deadHoldsHonest.writeTo(dir.resolve(honestFile));
        calibrations.add(new Calibration(honestFile, "NONE", null, 0, true, List.of()));
        indexEntries.add(JepsenEdn.map("file", honestFile, "mode", JepsenEdn.kw("NONE"), "host-fault", null,
                "seed", 0L, "expect-clean", true));
        Files.writeString(dir.resolve("index.edn"), JepsenEdn.write(indexEntries) + "\n", StandardCharsets.UTF_8);
        return calibrations;
    }
}
