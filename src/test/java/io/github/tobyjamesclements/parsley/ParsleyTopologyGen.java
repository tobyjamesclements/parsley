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
 * topics that exist before its own sinks are created — with at most one seed-chosen
 * <em>back-edge</em> (a later-produced topic added to an earlier node's inputs, the node's own
 * sink included) closing cycles. When a back-edge is present every forward probability is capped
 * subcritical: on a feedback loop each consumer-with-sinks appends roughly one record per
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
        /** Some consumer does not consume everything its upstream producer consumes — the ignore branch. */
        DIFFERING_SCOPE,
        /** Some node with sinks never forwards business records — its sinks carry only null messages. */
        SILENT_NODE,
        /** Some node consumes two or more topics — the crossing-wait / funnel shape. */
        FUNNEL
    }

    private static final double BACK_EDGE_PROBABILITY_CAP = 0.4;

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
            String name = "t" + nextTopic++;
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
                // shape. Only internals nobody consumes YET are candidates: sharing a topic an
                // earlier node already consumes would point a produces→consumes edge backward,
                // closing a multi-node cycle — see the back-edge comment below for why those are
                // excluded wholesale.
                if (random.nextInt(10) < 3) {
                    List<String> shareable = internals.stream()
                            .filter(topic -> inputs.stream().noneMatch(in -> in.contains(topic)))
                            .filter(topic -> !nodeSinks.contains(topic))
                            .toList();
                    if (!shareable.isEmpty()) {
                        nodeSinks.add(shareable.get(random.nextInt(shareable.size())));
                        continue;
                    }
                }
                String fresh = "t" + nextTopic++;
                nodeSinks.add(fresh);
                internals.add(fresh);
                created.add(fresh);
            }
            inputs.add(nodeInputs);
            sinks.add(nodeSinks);
            probabilities.add(random.nextInt(8) / 10.0);
        }

        // One optional back-edge: a node additionally consumes ONE OF ITS OWN SINKS — the
        // self-loop, the only cycle shape generated. Multi-node cycles of length >= 3 contain a
        // channel some cycle member neither produces nor consumes ("blind"), and on those the I6
        // relay provably never quiesces — each relay's own offset is the blind node's next
        // lesson — a KNOWN liveness defect pinned by ParsleyGossipCycleQuiescenceTest, excluded
        // here until the protocol closes it. Two-node cycles quiesce but compose with DAG paths
        // into longer cycles, so only the self-loop (whose every channel its node produces) is
        // safe to add blindly; the two-node shape is covered by the dedicated quiescence test
        // and the cyclic-reflection IT.
        if (random.nextInt(10) < 4) {
            int grower = random.nextInt(nodeCount);
            List<String> ownSinks = sinks.get(grower).stream()
                    .filter(sink -> !inputs.get(grower).contains(sink))
                    .toList();
            if (!ownSinks.isEmpty()) {
                inputs.get(grower).add(ownSinks.get(random.nextInt(ownSinks.size())));
                for (int i = 0; i < nodeCount; i++) {
                    probabilities.set(i, Math.min(probabilities.get(i), BACK_EDGE_PROBABILITY_CAP));
                }
            }
        }

        List<ParsleySimTrace.SimSpec.NodeSpec> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(new ParsleySimTrace.SimSpec.NodeSpec("N" + (i + 1),
                    inputs.get(i).stream().sorted().toList(),
                    List.copyOf(sinks.get(i)),
                    probabilities.get(i)));
        }
        return new ParsleySimTrace.SimSpec(List.copyOf(externals), List.copyOf(nodes));
    }

    /** The structural features present in {@code spec} — see {@link Feature}. */
    static Set<Feature> classify(ParsleySimTrace.SimSpec spec) {
        EnumSet<Feature> features = EnumSet.noneOf(Feature.class);
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
        if (hasCycle(spec, producersByTopic)) {
            features.add(Feature.CYCLE);
        }
        return features;
    }

    private static boolean hasCycle(ParsleySimTrace.SimSpec spec,
                                    Map<String, List<ParsleySimTrace.SimSpec.NodeSpec>> producersByTopic) {
        // A node is on a cycle iff it can reach itself through produces→consumes edges.
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
            if (reachable.contains(start.name())) {
                return true;
            }
        }
        return false;
    }
}
