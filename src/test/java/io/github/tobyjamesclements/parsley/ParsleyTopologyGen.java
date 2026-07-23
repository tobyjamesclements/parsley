package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Seed-deterministic random topology generation for the {@link ParsleyTopologySim} explorer,
 * with structural classification for class-level vacuity accounting: a sweep does not just
 * assert that each run exercised interesting paths, it asserts that the generated population
 * contained the shapes that CAN exercise them ({@link Feature}) — the guard against a generator
 * quietly producing only boring DAGs.
 *
 * <p>Construction is layered so specs are DAGs by default — each node draws its inputs from the
 * topics that exist before its own sinks are created — with two seed-chosen cycle closers: a
 * shared sink onto a topic an earlier node already consumes (a produces→consumes edge pointing
 * backward), and at most one <em>back-edge</em> (a later-produced topic added to an earlier
 * node's inputs, the node's own sink included). Cycles of any length are generated, including
 * the ≥ 3-node shapes on which the pre-fix I6 relay stormed (custody obliged relays; pinned
 * quiescent by {@code ParsleyGossipCycleQuiescenceTest} since the consumed-channel trigger).
 * When the finished spec contains a cycle every forward probability is capped subcritical: on a
 * feedback loop each consumer-with-sinks appends roughly one record per
 * delivered record (a business forward or a completeness-advert null message), so amplification
 * is governed by loop membership, not probability alone, and the sim's runaway guard classifies
 * anything that still goes hot as {@link ParsleyTopologySim.SupercriticalTopologyException} for
 * the sweep to skip and count.
 */
final class ParsleyTopologyGen {

    /** Structural shapes the sweep must prove it generated — see the class Javadoc. */
    enum Feature {
        /** Some topic is produced by two or more nodes — the interleaved-stamp shape. */
        SHARED_SINK,
        /** Some node consumes one of its own sinks — the reflected-claim cycle. */
        SELF_CONSUMER,
        /** The produces→consumes graph has a directed cycle (self-consumers included). */
        CYCLE,
        /**
         * Two or more distinct nodes sit on one directed cycle — the shape whose blind channels
         * stormed the pre-fix I6 relay (custody obliged relays), excluded from generation until
         * the consumed-channel trigger closed the defect. Guarded separately from {@link #CYCLE}
         * so a population of nothing but self-loops cannot satisfy the cycle guard vacuously.
         */
        MULTI_NODE_CYCLE,
        /** Some consumer does not consume everything its upstream producer consumes — the ignore branch. */
        DIFFERING_SCOPE,
        /** Some node with sinks never forwards business records — its sinks carry only null messages. */
        SILENT_NODE,
        /** Some node consumes two or more topics — the crossing-wait / funnel shape. */
        FUNNEL,
        /**
         * The topology runs more than one partition — per-partition tasks, keyed externals, and
         * (in the sweep) stamped-external cross-partition claims. Guarded so a population of
         * nothing but single-partition topologies cannot claim the T10 dimension vacuously.
         */
        MULTI_PARTITION
    }

    private static final double CYCLE_PROBABILITY_CAP = 0.4;

    private ParsleyTopologyGen() {}

    /** Generates the spec for {@code seed}: 2–7 nodes, 1–2 external topics, DAG + optional back-edge. */
    static ParsleySimTrace.SimSpec generate(long seed) {
        Random random = new Random(seed);
        int externalCount = 1 + random.nextInt(2);
        int nodeCount = 2 + random.nextInt(6);
        List<String> externals = new ArrayList<>();
        List<String> created = new ArrayList<>();
        int nextTopic = 1;
        for (int i = 0; i < externalCount; i++) {
            String name = "c" + nextTopic++;
            externals.add(name);
            created.add(name);
        }

        List<Set<String>> inputs = new ArrayList<>();
        List<List<String>> sinks = new ArrayList<>();
        List<Double> probabilities = new ArrayList<>();
        List<String> internals = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            Set<String> nodeInputs = new LinkedHashSet<>();
            if (i == 0) {
                // Something must consume an external topic or no business record ever flows.
                nodeInputs.add(externals.get(random.nextInt(externals.size())));
            }
            int inputCount = 1 + random.nextInt(3);
            while (nodeInputs.size() < Math.min(inputCount, created.size())) {
                nodeInputs.add(created.get(random.nextInt(created.size())));
            }
            List<String> nodeSinks = new ArrayList<>();
            int sinkCount = random.nextInt(3);
            for (int s = 0; s < sinkCount; s++) {
                // Mostly fresh topics; sometimes an existing internal one — the shared-sink
                // shape. Internals an earlier node already consumes are candidates too: that
                // points a produces→consumes edge backward and closes a multi-node cycle, the
                // shape the pre-fix I6 relay stormed on (re-enabled since the consumed-channel
                // trigger; the cycle cap below keeps the loop subcritical).
                if (random.nextInt(10) < 3) {
                    List<String> shareable = internals.stream()
                            .filter(topic -> !nodeSinks.contains(topic))
                            .toList();
                    if (!shareable.isEmpty()) {
                        nodeSinks.add(shareable.get(random.nextInt(shareable.size())));
                        continue;
                    }
                }
                String fresh = "c" + nextTopic++;
                nodeSinks.add(fresh);
                internals.add(fresh);
                created.add(fresh);
            }
            inputs.add(nodeInputs);
            sinks.add(nodeSinks);
            probabilities.add(random.nextInt(8) / 10.0);
        }

        // One optional back-edge: a node additionally consumes any internal topic not already
        // among its inputs — its own sink (the self-loop) or any other node's (closing a
        // multi-node cycle of arbitrary length). Cycles of length >= 3 contain channels some
        // cycle member neither produces nor consumes ("blind"): the pre-fix I6 relay stormed on
        // exactly those (custody obliged relays), so they were excluded here until the
        // consumed-channel trigger closed the defect — pinned quiescent by
        // ParsleyGossipCycleQuiescenceTest.
        if (random.nextInt(10) < 4) {
            int grower = random.nextInt(nodeCount);
            List<String> candidates = internals.stream()
                    .filter(topic -> !inputs.get(grower).contains(topic))
                    .toList();
            if (!candidates.isEmpty()) {
                inputs.get(grower).add(candidates.get(random.nextInt(candidates.size())));
            }
        }

        List<ParsleySimTrace.SimSpec.NodeSpec> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(new ParsleySimTrace.SimSpec.NodeSpec("p" + (i + 1),
                    inputs.get(i).stream().sorted().toList(),
                    List.copyOf(sinks.get(i)),
                    probabilities.get(i)));
        }
        // Drawn LAST so the topology shape for a given generator seed is unchanged from the
        // single-partition era — the count is the only new draw and nothing draws after it.
        // Single-partition keeps the majority so the pre-T10 population stays the baseline;
        // 2 or 3 partitions open the T10 dimension (per-partition tasks, keyed externals,
        // stamped-external cross-partition claims in the sweep).
        int partitions = random.nextInt(10) < 6 ? 1 : 2 + random.nextInt(2);
        ParsleySimTrace.SimSpec spec = new ParsleySimTrace.SimSpec(
                List.copyOf(externals), List.copyOf(nodes), partitions);
        // Whatever closed a cycle — the back-edge above or a shared sink onto an already-consumed
        // topic — caps every forward probability subcritical (see the class Javadoc: on a
        // feedback loop, amplification is governed by loop membership, not probability alone).
        if (classify(spec).contains(Feature.CYCLE)) {
            List<ParsleySimTrace.SimSpec.NodeSpec> capped = new ArrayList<>();
            for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
                capped.add(new ParsleySimTrace.SimSpec.NodeSpec(node.name(), node.inputs(),
                        node.sinks(), Math.min(node.outputProbability(), CYCLE_PROBABILITY_CAP)));
            }
            spec = new ParsleySimTrace.SimSpec(spec.externalTopics(), List.copyOf(capped), partitions);
        }
        return spec;
    }

    /** The structural features present in {@code spec} — see {@link Feature}. */
    static Set<Feature> classify(ParsleySimTrace.SimSpec spec) {
        EnumSet<Feature> features = EnumSet.noneOf(Feature.class);
        if (spec.partitions() > 1) {
            features.add(Feature.MULTI_PARTITION);
        }
        Map<String, List<ParsleySimTrace.SimSpec.NodeSpec>> producersByTopic = new HashMap<>();
        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            for (String sink : node.sinks()) {
                producersByTopic.computeIfAbsent(sink, t -> new ArrayList<>()).add(node);
            }
        }
        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            if (node.inputs().size() >= 2) {
                features.add(Feature.FUNNEL);
            }
            if (node.outputProbability() == 0.0 && !node.sinks().isEmpty()) {
                features.add(Feature.SILENT_NODE);
            }
            if (node.inputs().stream().anyMatch(node.sinks()::contains)) {
                features.add(Feature.SELF_CONSUMER);
                features.add(Feature.CYCLE);
            }
            for (String input : node.inputs()) {
                for (ParsleySimTrace.SimSpec.NodeSpec producer : producersByTopic.getOrDefault(input, List.of())) {
                    if (!node.inputs().containsAll(producer.inputs())) {
                        features.add(Feature.DIFFERING_SCOPE);
                    }
                }
            }
        }
        if (producersByTopic.values().stream().anyMatch(producers -> producers.size() >= 2)) {
            features.add(Feature.SHARED_SINK);
        }
        Map<String, Set<String>> reachableByNode = reachability(spec);
        for (ParsleySimTrace.SimSpec.NodeSpec node : spec.nodes()) {
            if (reachableByNode.get(node.name()).contains(node.name())) {
                features.add(Feature.CYCLE);
            }
        }
        for (ParsleySimTrace.SimSpec.NodeSpec a : spec.nodes()) {
            for (ParsleySimTrace.SimSpec.NodeSpec b : spec.nodes()) {
                if (!a.name().equals(b.name())
                        && reachableByNode.get(a.name()).contains(b.name())
                        && reachableByNode.get(b.name()).contains(a.name())) {
                    features.add(Feature.MULTI_NODE_CYCLE);
                }
            }
        }
        return features;
    }

    /**
     * Per node, every node reachable from it through produces→consumes edges (a node is on a
     * cycle iff it reaches itself; two distinct mutually reachable nodes share a multi-node
     * cycle).
     */
    private static Map<String, Set<String>> reachability(ParsleySimTrace.SimSpec spec) {
        Map<String, Set<String>> reachableByNode = new HashMap<>();
        for (ParsleySimTrace.SimSpec.NodeSpec start : spec.nodes()) {
            Set<String> reachable = new HashSet<>();
            List<ParsleySimTrace.SimSpec.NodeSpec> frontier = new ArrayList<>(List.of(start));
            while (!frontier.isEmpty()) {
                ParsleySimTrace.SimSpec.NodeSpec current = frontier.remove(frontier.size() - 1);
                for (ParsleySimTrace.SimSpec.NodeSpec consumer : spec.nodes()) {
                    if (consumer.inputs().stream().anyMatch(current.sinks()::contains)
                            && reachable.add(consumer.name())) {
                        frontier.add(consumer);
                    }
                }
            }
            reachableByNode.put(start.name(), reachable);
        }
        return reachableByNode;
    }
}
