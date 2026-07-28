package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.Topology;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A composition of causal stages into one Kafka Streams topology. Stages connect through
 * ordinary topics: one stage's sink is another's source, and the connecting topic is a causal
 * channel like any other — stamped on write, gated on read, and required to exist before the
 * application starts. Within one application the hops cost no coordination beyond the broker
 * round trip; stamping is synchronous at every hop (sequence claims), so a pipeline of stages
 * adds no acknowledgement waits.
 *
 * <p>Constraints checked at composition: stage names must be distinct (each names its state
 * store), and source topics must be disjoint across stages — Kafka Streams allows a topic to
 * feed only one source node per topology. A topic may be a sink of several stages.
 */
public final class CausalTopology {

    private final Map<String, CausalStage<?, ?, ?, ?>> stages;

    private CausalTopology(Map<String, CausalStage<?, ?, ?, ?>> stages) {
        this.stages = stages;
    }

    public static CausalTopology of(CausalStage<?, ?, ?, ?>... stages) {
        if (stages.length == 0) throw new IllegalArgumentException("no stages");
        Map<String, CausalStage<?, ?, ?, ?>> byName = new LinkedHashMap<>();
        Set<String> sourceTopics = new HashSet<>();
        for (CausalStage<?, ?, ?, ?> stage : stages) {
            if (byName.put(stage.name(), stage) != null) {
                throw new IllegalArgumentException("duplicate stage name: " + stage.name()
                        + " (name each stage with Builder.name)");
            }
            for (String topic : stage.sourceTopics()) {
                if (!sourceTopics.add(topic)) {
                    throw new IllegalArgumentException("topic " + topic + " is a source of two"
                            + " stages; Kafka Streams allows one source node per topic");
                }
            }
        }
        return new CausalTopology(byName);
    }

    /** A broker-less topology for {@code TopologyTestDriver}; see {@link CausalStage#testTopology}. */
    public Topology testTopology() {
        Topology t = new Topology();
        for (CausalStage<?, ?, ?, ?> stage : stages.values()) {
            stage.wireForTest();
            stage.addTo(t);
        }
        return t;
    }

    List<CausalStage<?, ?, ?, ?>> stages() {
        return List.copyOf(stages.values());
    }

    /** Assembles the shared Streams topology. Every stage must be wired. */
    Topology topology() {
        Topology t = new Topology();
        for (CausalStage<?, ?, ?, ?> stage : stages.values()) {
            stage.addTo(t);
        }
        return t;
    }
}
