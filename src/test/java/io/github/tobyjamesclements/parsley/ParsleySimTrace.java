package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The schedule vocabulary of a {@link ParsleyTopologySim} run: a topology spec by value, a sealed
 * action hierarchy, and a line-based print/parse pair so a failing schedule can be copied out of a
 * test log and replayed verbatim.
 *
 * <p>Every action is <em>self-contained</em>: a {@link Poll} carries the delegate's per-released-
 * record emission decisions ({@link Emit}) that were drawn while it executed, so deleting an
 * action from a trace (the shrinker's move) never perturbs the decisions of the actions after it.
 * That property is what makes delta-debugging over a recorded schedule sound — replay never
 * touches the seeded PRNG at all.
 *
 * <p>Names are the identity used across runs (topic UUIDs are freshly random per sim instance),
 * so the text format assumes topic and node names contain no spaces or commas — true of every
 * name the harness and generator produce.
 *
 * <p><strong>Partition fields (T10).</strong> Actions carry the partition dimension, printed only
 * when it is informative (a nonzero partition, a nonzero key index): a single-partition run
 * prints exactly the pre-T10 text, and pre-T10 trace texts parse with the partition fields
 * defaulting to 0.
 */
final class ParsleySimTrace {

    private ParsleySimTrace() {}

    /** A topology by value — enough to rebuild a sim for replay without the builder calls. */
    record SimSpec(List<String> externalTopics, List<NodeSpec> nodes, int partitions) {

        /** Single-partition compatibility shape — the pre-T10 constructor. */
        SimSpec(List<String> externalTopics, List<NodeSpec> nodes) {
            this(externalTopics, nodes, 1);
        }

        /** One node: declared inputs (sorted), sinks, and the delegate's forward probability. */
        record NodeSpec(String name, List<String> inputs, List<String> sinks, double outputProbability) {}
    }

    /** One scheduler step. All state an action needs is in its fields — see the class Javadoc. */
    sealed interface Action {}

    /**
     * An external clockless producer appends one record to {@code topic} with key
     * {@code keyIndex} (always 0 on single-partition topologies — the key draw exists only where
     * a partitioner has work to do).
     */
    record ProduceExternal(String topic, int keyIndex) implements Action {}

    /** One observed coordinate of a stamped-external production. */
    record Observed(String topic, int partition, long offset) {}

    /**
     * A stamped external producer ({@code CausalClock.using(...).observe(...)} at the edge)
     * appends one record to {@code topic} whose clock and history derive from the observed
     * coordinates — the sim's only source of cross-partition claims.
     */
    record ProduceStamped(String topic, int keyIndex, List<Observed> observed) implements Action {}

    /** Every pending send of {@code node} is acknowledged. */
    record AckAll(String node) implements Action {}

    /**
     * {@code node} restarts from its durable stores with the given declared inputs — both the
     * clean in-place restart (same inputs) and the A5/A6 scope-change restart (different inputs).
     */
    record Rescope(String node, List<String> inputs) implements Action {}

    /**
     * {@code node} dies and rebuilds from its durable stores WITHOUT the clean shutdown: no final
     * ack wait, no {@code foldAcknowledgedOutputs}. Sends already appended stay in the logs; the
     * init-time sink end-offset seed (I8) must re-cover them.
     */
    record Crash(String node) implements Action {}

    /**
     * {@code node}'s consumer position on {@code input} at task {@code partition} rewinds to
     * {@code toCursor} — at-least-once redelivery (a rebalance re-fetch); the protocol must
     * absorb the replay (A11).
     */
    record Rollback(String node, String input, int partition, int toCursor) implements Action {}

    /**
     * {@code node}'s task {@code partition} polls {@code input} once; {@code emits} are the
     * delegate decisions drawn.
     */
    record Poll(String node, String input, int partition, List<Emit> emits) implements Action {}

    /** One delegate decision for one released record: forward to {@code sink}, or stay silent. */
    record Emit(boolean business, String sink) {
        static final Emit NONE = new Emit(false, "");
    }

    /** A parsed repro: the topology and the schedule, ready for {@code ParsleyTopologySim.replay}. */
    record Repro(SimSpec spec, List<Action> trace) {}

    static String print(SimSpec spec, List<Action> trace) {
        StringBuilder out = new StringBuilder("spec\n");
        if (spec.partitions() > 1) {
            out.append("partitions ").append(spec.partitions()).append('\n');
        }
        for (String topic : spec.externalTopics()) {
            out.append("ext ").append(topic).append('\n');
        }
        for (SimSpec.NodeSpec node : spec.nodes()) {
            out.append("node ").append(node.name())
                    .append(" in=").append(String.join(",", node.inputs()))
                    .append(" out=").append(node.sinks().isEmpty() ? "-" : String.join(",", node.sinks()))
                    .append(" p=").append(node.outputProbability())
                    .append('\n');
        }
        out.append("trace\n");
        for (Action action : trace) {
            out.append(printAction(action)).append('\n');
        }
        return out.toString();
    }

    private static String printAction(Action action) {
        return switch (action) {
            // Zero fields print the legacy shapes, so single-partition traces stay byte-stable.
            case ProduceExternal(String topic, int keyIndex) ->
                    "ext " + topic + (keyIndex == 0 ? "" : " k" + keyIndex);
            case ProduceStamped(String topic, int keyIndex, List<Observed> observed) ->
                    "exts " + topic + " k" + keyIndex + " " + observed.stream()
                            .map(o -> o.topic() + ":" + o.partition() + "@" + o.offset())
                            .collect(Collectors.joining(","));
            case AckAll(String node) -> "ack " + node;
            case Rescope(String node, List<String> inputs) -> "rescope " + node + " " + String.join(",", inputs);
            case Crash(String node) -> "crash " + node;
            case Rollback(String node, String input, int partition, int toCursor) -> "rollback " + node
                    + " " + input + (partition == 0 ? "" : " p" + partition) + " " + toCursor;
            case Poll(String node, String input, int partition, List<Emit> emits) -> "poll " + node
                    + " " + input + (partition == 0 ? "" : " p" + partition) + " ["
                    + emits.stream().map(e -> e.business() ? e.sink() : "-").collect(Collectors.joining(","))
                    + "]";
        };
    }

    static Repro parse(String text) {
        List<String> externals = new ArrayList<>();
        List<SimSpec.NodeSpec> nodes = new ArrayList<>();
        List<Action> trace = new ArrayList<>();
        int partitions = 1;
        boolean inTrace = false;
        for (String raw : text.split("\n")) {
            String line = raw.strip();
            if (line.isEmpty() || line.equals("spec")) {
                continue;
            }
            if (line.equals("trace")) {
                inTrace = true;
                continue;
            }
            String[] tokens = line.split(" ");
            if (!inTrace) {
                switch (tokens[0]) {
                    case "partitions" -> partitions = Integer.parseInt(tokens[1]);
                    case "ext" -> externals.add(tokens[1]);
                    case "node" -> nodes.add(new SimSpec.NodeSpec(tokens[1],
                            commaList(value(tokens[2], "in=")),
                            commaList(value(tokens[3], "out=")),
                            Double.parseDouble(value(tokens[4], "p="))));
                    default -> throw new IllegalArgumentException("unparseable spec line: " + line);
                }
                continue;
            }
            trace.add(parseAction(line, tokens));
        }
        return new Repro(new SimSpec(List.copyOf(externals), List.copyOf(nodes), partitions),
                List.copyOf(trace));
    }

    private static Action parseAction(String line, String[] tokens) {
        return switch (tokens[0]) {
            case "ext" -> new ProduceExternal(tokens[1],
                    tokens.length > 2 ? Integer.parseInt(tokens[2].substring(1)) : 0);
            case "exts" -> new ProduceStamped(tokens[1], Integer.parseInt(tokens[2].substring(1)),
                    parseObserved(tokens[3]));
            case "ack" -> new AckAll(tokens[1]);
            case "rescope" -> new Rescope(tokens[1], commaList(tokens[2]));
            case "crash" -> new Crash(tokens[1]);
            // rollback node input [pN] toCursor — the partition token is absent in legacy traces.
            case "rollback" -> tokens.length == 4
                    ? new Rollback(tokens[1], tokens[2], 0, Integer.parseInt(tokens[3]))
                    : new Rollback(tokens[1], tokens[2], Integer.parseInt(tokens[3].substring(1)),
                            Integer.parseInt(tokens[4]));
            // poll node input [pN] [emits] — the partition token is absent in legacy traces.
            case "poll" -> tokens[3].startsWith("[")
                    ? new Poll(tokens[1], tokens[2], 0, parseEmits(tokens[3]))
                    : new Poll(tokens[1], tokens[2], Integer.parseInt(tokens[3].substring(1)),
                            parseEmits(tokens[4]));
            default -> throw new IllegalArgumentException("unparseable trace line: " + line);
        };
    }

    private static List<Observed> parseObserved(String joined) {
        List<Observed> observed = new ArrayList<>();
        for (String token : joined.split(",")) {
            int colon = token.lastIndexOf(':');
            int at = token.indexOf('@', colon);
            observed.add(new Observed(token.substring(0, colon),
                    Integer.parseInt(token.substring(colon + 1, at)),
                    Long.parseLong(token.substring(at + 1))));
        }
        return List.copyOf(observed);
    }

    private static List<Emit> parseEmits(String bracketed) {
        String inner = bracketed.substring(1, bracketed.length() - 1);
        if (inner.isEmpty()) {
            return List.of();
        }
        List<Emit> emits = new ArrayList<>();
        for (String token : inner.split(",")) {
            emits.add(token.equals("-") ? Emit.NONE : new Emit(true, token));
        }
        return List.copyOf(emits);
    }

    private static List<String> commaList(String joined) {
        return joined.equals("-") ? List.of() : List.of(joined.split(","));
    }

    private static String value(String token, String prefix) {
        if (!token.startsWith(prefix)) {
            throw new IllegalArgumentException("expected " + prefix + " but found: " + token);
        }
        return token.substring(prefix.length());
    }
}
