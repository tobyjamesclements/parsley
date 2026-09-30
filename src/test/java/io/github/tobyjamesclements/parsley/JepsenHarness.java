package io.github.tobyjamesclements.parsley;

import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.tobyjamesclements.parsley.JepsenEdn.kw;
import static io.github.tobyjamesclements.parsley.JepsenEdn.map;

/**
 * The application the Jepsen test runs on every node: the {@link JepsenTopology} under
 * Parsley, with its status served over a local port and its log written to a file the test
 * collects. It is built from this test tree into one jar (Maven profile
 * {@code jepsen-harness}) that the DB adapter installs alongside the broker.
 *
 * <pre>
 * java -jar parsley-jepsen-harness.jar run --bootstrap n1:9092 --prefix jepsen \
 *      --state-dir /var/lib/parsley --status-port 8080 --log-file /var/log/parsley.log \
 *      [--drop-topic loop] [--metadata-budget 262144] [--streams key=value ...]
 * java -jar parsley-jepsen-harness.jar create-topics --bootstrap n1:9092 --partitions 3 \
 *      --replication 3 [--min-isr 2]
 * java -jar parsley-jepsen-harness.jar export --bootstrap n1:9092 --out run.edn \
 *      [--observations obs.edn] [--drop-topic loop]
 * java -jar parsley-jepsen-harness.jar check --in run.edn
 * java -jar parsley-jepsen-harness.jar export-simulator --out dir [--seeds 120]
 * java -jar parsley-jepsen-harness.jar codec-vectors --out vectors.edn
 * </pre>
 *
 * <p>{@code run} starts each declared process on its own, so a refusal at one process's
 * start leaves the others running, and keeps serving status after a process stops: a
 * refusal is terminal by design, and the status clients must be able to read it. The
 * status is EDN: {@code {:healthy false :processes {"joiner" {:lifecycle :STOPPED :refusal
 * :ORDERING_STATE_LOST :detail "..."}}}}. A refusal raised by {@code Parsley.start} itself
 * is reported under the process it refused, as {@code STOPPED} with the reason.
 */
public final class JepsenHarness {
    private JepsenHarness() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        Map<String, String> opts = options(args);
        switch (args[0]) {
            case "run" -> run(opts);
            case "create-topics" -> createTopics(opts);
            case "export" -> export(opts);
            case "check" -> check(opts);
            case "export-simulator" -> JepsenSimulatorExport.writeCalibrationSet(
                    Path.of(require(opts, "out")), Integer.parseInt(opts.getOrDefault("seeds", "120")));
            case "codec-vectors" -> JepsenCodecVectors.writeTo(Path.of(require(opts, "out")));
            default -> usage();
        }
    }

    private static void usage() {
        System.err.println("usage: run | create-topics | export | check | export-simulator | codec-vectors"
                + " with --key value options; see the class Javadoc");
        System.exit(2);
    }

    static Map<String, String> options(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (!args[i].startsWith("--")) {
                throw new IllegalArgumentException("expected --option, got " + args[i]);
            }
            String key = args[i].substring(2);
            String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
            if (key.equals("streams") || key.equals("drop-topic")) {
                opts.merge(key, value, (a, b) -> a + "," + b);
            } else {
                opts.put(key, value);
            }
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return value;
    }

    static Set<String> dropped(Map<String, String> opts) {
        String dropped = opts.get("drop-topic");
        return dropped == null ? Set.of() : Set.of(dropped.split(","));
    }

    private static void configureLogging(Map<String, String> opts) {
        if (opts.containsKey("log-file")) {
            System.setProperty("org.slf4j.simpleLogger.logFile", opts.get("log-file"));
        }
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", opts.getOrDefault("log-level", "info"));
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", opts.getOrDefault("kafka-log-level", "warn"));
    }

    static ParsleyConfig config(Map<String, String> opts) {
        ParsleyConfig.Builder builder = ParsleyConfig.builder(require(opts, "bootstrap"), require(opts, "prefix"));
        if (opts.containsKey("state-dir")) {
            builder.stateDir(opts.get("state-dir"));
        }
        if (opts.containsKey("metadata-budget")) {
            builder.metadataBudgetBytes(Integer.parseInt(opts.get("metadata-budget")));
        }
        if (opts.containsKey("streams")) {
            for (String pair : opts.get("streams").split(",")) {
                int eq = pair.indexOf('=');
                builder.streamsProperty(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return builder.build();
    }

    /**
     * The status endpoint's view: every running handle, and for each process whose start
     * failed, what stopped it. Each declared process is started on its own, so a refusal at
     * one process's start leaves the others running, as separate applications would be.
     */
    static final class StatusView {
        final List<Parsley> running = new java.util.concurrent.CopyOnWriteArrayList<>();
        final Map<String, Throwable> startFailures = new java.util.concurrent.ConcurrentHashMap<>();
        volatile List<String> processNames = List.of();

        String edn() {
            Map<Object, Object> processes = new LinkedHashMap<>();
            boolean healthy = startFailures.isEmpty();
            for (Parsley handle : running) {
                healthy &= handle.healthy();
                handle.status().forEach((name, status) -> processes.put(name, map(
                        "lifecycle", kw(status.lifecycle().name()),
                        "refusal", status.refusalReason().map(reason -> (Object) kw(reason.name())).orElse(null),
                        "detail", status.failureDetail().orElse(null))));
            }
            startFailures.forEach((name, failure) -> {
                FailClosedException refusal = FailClosedException.findIn(failure);
                processes.put(name, map("lifecycle", kw("STOPPED"),
                        "refusal", refusal == null ? null : kw(refusal.reason().name()),
                        "detail", failure.getMessage()));
            });
            for (String name : processNames) {
                processes.putIfAbsent(name, map("lifecycle", kw("STOPPED"), "refusal", null, "detail", "starting"));
            }
            return JepsenEdn.write(map("healthy", healthy, "processes", processes)) + "\n";
        }
    }

    static HttpServer serveStatus(int port, StatusView view) throws java.io.IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", exchange -> {
            byte[] body = view.edn().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/edn; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        return server;
    }

    private static void run(Map<String, String> opts) throws Exception {
        configureLogging(opts);
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JepsenHarness.class);
        StatusView view = new StatusView();
        List<Process> processes = JepsenTopology.processes(dropped(opts));
        view.processNames = processes.stream().map(Process::name).toList();
        HttpServer server = serveStatus(Integer.parseInt(opts.getOrDefault("status-port", "8080")), view);
        log.info("harness starting: bootstrap {} prefix {} processes {}", opts.get("bootstrap"), opts.get("prefix"),
                view.processNames);
        ParsleyConfig config = config(opts);
        for (Process process : processes) {
            try {
                view.running.add(Parsley.start(config, process));
                log.info("harness started {}", process.name());
            } catch (RuntimeException e) {
                view.startFailures.put(process.name(), e);
                log.error("harness: {} refused to start; serving the refusal until stopped", process.name(), e);
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("harness stopping");
            for (Parsley handle : view.running) {
                handle.close();
            }
            server.stop(0);
        }));
        Set<String> reported = new java.util.HashSet<>();
        while (true) {
            for (Parsley handle : view.running) {
                if (handle.awaitStopped(Duration.ofSeconds(5))) {
                    handle.status().forEach((name, status) -> {
                        if (reported.add(name)) {
                            log.warn("process {} status: {}", name, status);
                        }
                    });
                }
            }
            if (view.running.isEmpty()) {
                Thread.sleep(5_000);
            }
        }
    }

    private static void createTopics(Map<String, String> opts) throws Exception {
        try (var admin = JepsenClusterExport.admin(require(opts, "bootstrap"))) {
            Map<String, String> configs = new LinkedHashMap<>();
            if (opts.containsKey("min-isr")) {
                configs.put("min.insync.replicas", opts.get("min-isr"));
            }
            configs.put("unclean.leader.election.enable", opts.getOrDefault("unclean-election", "false"));
            JepsenTopology.createTopics(admin, Integer.parseInt(opts.getOrDefault("partitions", "3")),
                    Short.parseShort(opts.getOrDefault("replication", "1")), configs);
        }
        System.out.println("created " + JepsenTopology.TOPICS);
    }

    private static void export(Map<String, String> opts) throws Exception {
        Map<String, JepsenExport.ProcessDecl> declaration = JepsenTopology.declaration(dropped(opts));
        Map<String, JepsenExport.Position> stampedFrom = new LinkedHashMap<>();
        Object observations = null;
        if (opts.containsKey("observations")) {
            observations = JepsenEdn.read(java.nio.file.Files.readString(Path.of(opts.get("observations"))));
            JepsenEdn.asMap(JepsenEdn.get(observations, "stamped-from")).forEach((uid, position) ->
                    stampedFrom.put(uid.toString(), JepsenExport.position(position)));
        }
        JepsenExport export = JepsenClusterExport.dump(require(opts, "bootstrap"), declaration, JepsenTopology.TRACE,
                stampedFrom);
        if (observations != null) {
            JepsenExport observed = JepsenExport.fromEdn(mergeObservations(export, observations));
            export.reads.addAll(observed.reads);
            export.statuses.addAll(observed.statuses);
            export.faults.addAll(observed.faults);
            export.startPositions.addAll(observed.startPositions);
        }
        export.writeTo(Path.of(require(opts, "out")));
        System.out.println("wrote " + opts.get("out") + ": " + export.records.size() + " records, "
                + export.trace.size() + " trace entries");
    }

    /** Lifts the observation lists into a full export map so one reader parses them. */
    private static Map<Object, Object> mergeObservations(JepsenExport base, Object observations) {
        Map<Object, Object> edn = base.toEdn();
        edn.put(JepsenEdn.kw("records"), List.of());
        edn.put(JepsenEdn.kw("trace"), List.of());
        for (String key : List.of("reads", "statuses", "faults", "start-positions")) {
            edn.put(JepsenEdn.kw(key), JepsenEdn.asList(JepsenEdn.get(observations, key)));
        }
        return edn;
    }

    private static void check(Map<String, String> opts) throws Exception {
        JepsenExport export = JepsenExport.readFrom(Path.of(require(opts, "in")));
        JepsenExportOracle.Verdict verdict = JepsenExportOracle.check(export);
        for (String violation : verdict.violations()) {
            System.out.println(violation);
        }
        System.out.println(verdict.clean() ? "clean" : verdict.violations().size() + " violations");
        if (!verdict.clean()) {
            System.exit(1);
        }
    }
}
