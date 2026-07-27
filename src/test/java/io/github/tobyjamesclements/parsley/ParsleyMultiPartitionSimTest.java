package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic pins for the sim's multi-partition model, via PRNG-free {@code replay}
 * over hand-built traces: the two-branch gate's <em>partition dimension</em> — a dependency on a
 * consumed topic's other partition is ignored, a dependency on the task's own partition gates —
 * and custody of foreign-partition ancestry in stamps. The randomized coverage of the same
 * machinery lives in {@code ParsleyRandomTopologyPropertyTest}'s sweep (the
 * {@code MULTI_PARTITION} population feature and its vacuity guards); these tests pin the exact
 * semantics on the smallest topologies that can express them.
 *
 * <p>All topologies here run two partitions of every topic; keys {@code k0}/{@code k1} land on
 * partitions 0/1 under the sim's {@code keyIndex % partitions} partitioner.
 */
class ParsleyMultiPartitionSimTest {

    /** partitions=2, external c1, one relay node p1 {c1} → c2. */
    private static ParsleySimTrace.SimSpec relaySpec() {
        return new ParsleySimTrace.SimSpec(List.of("c1"),
                List.of(new ParsleySimTrace.SimSpec.NodeSpec("p1", List.of("c1"), List.of("c2"), 1.0)),
                2);
    }

    /** partitions=2, externals c1 and c3, one funnel node p1 {c1, c3} → c2. */
    private static ParsleySimTrace.SimSpec funnelSpec() {
        return new ParsleySimTrace.SimSpec(List.of("c1", "c3"),
                List.of(new ParsleySimTrace.SimSpec.NodeSpec("p1", List.of("c1", "c3"), List.of("c2"), 1.0)),
                2);
    }

    /**
     * A stamped external on partition 1 claims a coordinate on partition 0 of the same consumed
     * topic. The receiving task owns only partition 1, so the claim is out of its scope: the
     * two-branch gate's partition dimension must ignore it and deliver immediately — a gate that
     * conflated partitions would hold the record forever against a coordinate this task can never
     * deliver. The task's stamp must still carry the foreign-partition coordinate (custody
     * spans partitions), and its business output must land on its own partition (co-partitioning
     * by key preservation).
     *
     * Asserts that the claiming record is delivered with an empty buffer, the foreign cause was
     * counted as ignored, the outbound stamp claims the foreign coordinate, and the derived
     * record sits on the sink's partition 1.
     */
    @Test
    void foreignPartitionDependencyIsIgnoredNotGated() {
        List<ParsleySimTrace.Action> trace = List.of(
                new ParsleySimTrace.ProduceExternal("c1", 0),               // c1:0@0 (key k0)
                new ParsleySimTrace.ProduceExternal("c1", 1),               // c1:1@0 (key k1)
                new ParsleySimTrace.Poll("p1", "c1", 1, List.of(ParsleySimTrace.Emit.NONE)),
                // The edge producer observed c1:0@0 and stamps a k1 record with that claim —
                // a coordinate on the receiving task's OTHER partition.
                new ParsleySimTrace.ProduceStamped("c1", 1,
                        List.of(new ParsleySimTrace.Observed("c1", 0, 0))), // c1:1@1 claims c1:0@0
                new ParsleySimTrace.Poll("p1", "c1", 1,
                        List.of(new ParsleySimTrace.Emit(true, "c2"))));

        ParsleyTopologySim sim = ParsleyTopologySim.replay(relaySpec(), trace);

        ParsleyTopologySim.SimTask task1 = sim.nodeNamed("p1").task(1);
        assertEquals(0, task1.core.bufferSize(),
                "a foreign-partition claim must be ignored by the gate, never held against — "
                        + "task 1 can never deliver partition 0's coordinates");
        assertTrue(task1.delivered.contains(
                        new ParsleyTopologySim.SimCoord(sim.topicId("c1"), 1, 1L)),
                "the claiming record on the task's own partition must be delivered");
        assertTrue(sim.crossPartitionIgnoredCauses > 0,
                "the foreign-partition cause must be counted through the ignore branch");
        assertTrue(task1.channels.vectorTime().offsetFor(sim.topicId("c1"), 0) >= 0,
                "the task's stamp must still claim the foreign-partition ancestry it can never "
                        + "deliver — custody spans partitions");
        // Partition 1 carries task 1's progress-advert null message (the first, silent
        // delivery) plus the derived business record; partition 0 must stay untouched.
        assertEquals(2, sim.logSize("c2", 1),
                "the derived record (and task 1's progress advert) must land on the emitting "
                        + "task's own sink partition — key preservation IS co-partitioning");
        assertEquals(0, sim.logSize("c2", 0),
                "nothing may land on the sink partition the emitting task does not own");
    }

    /**
     * The contrast case: the same claim shape on the task's OWN partition gates. A record on
     * {@code c3} partition 0 claiming {@code c1:0@0} is held until the task delivers
     * {@code c1:0@0}, then released — cross-topic, same-partition dependencies still gate
     * exactly as in the single-partition world.
     *
     * Asserts that the claiming record is held while its cause is undelivered, and that the
     * release happens in causal order once the cause arrives.
     */
    @Test
    void ownPartitionDependencyStillGates() {
        List<ParsleySimTrace.Action> stalled = List.of(
                new ParsleySimTrace.ProduceExternal("c1", 0),               // c1:0@0
                new ParsleySimTrace.ProduceStamped("c3", 0,
                        List.of(new ParsleySimTrace.Observed("c1", 0, 0))), // c3:0@0 claims c1:0@0
                new ParsleySimTrace.Poll("p1", "c3", 0, List.of()));        // held: cause undelivered

        ParsleyTopologySim sim = ParsleyTopologySim.replay(funnelSpec(), stalled);
        ParsleyTopologySim.SimTask task0 = sim.nodeNamed("p1").task(0);
        assertEquals(1, task0.core.bufferSize(),
                "an own-partition dependency on a consumed topic must gate: the claiming record "
                        + "is held until its cause is delivered here");

        List<ParsleySimTrace.Action> released = List.of(
                new ParsleySimTrace.ProduceExternal("c1", 0),
                new ParsleySimTrace.ProduceStamped("c3", 0,
                        List.of(new ParsleySimTrace.Observed("c1", 0, 0))),
                new ParsleySimTrace.Poll("p1", "c3", 0, List.of()),
                new ParsleySimTrace.Poll("p1", "c1", 0,
                        List.of(ParsleySimTrace.Emit.NONE, ParsleySimTrace.Emit.NONE)));

        ParsleyTopologySim releasedSim = ParsleyTopologySim.replay(funnelSpec(), released);
        ParsleyTopologySim.SimTask releasedTask = releasedSim.nodeNamed("p1").task(0);
        assertEquals(0, releasedTask.core.bufferSize(),
                "delivering the cause must release the held record");
        assertEquals(List.of(0L), releasedTask.deliveredOffsetsByTopic.get("c1"),
                "the cause must be delivered");
        assertEquals(List.of(0L), releasedTask.deliveredOffsetsByTopic.get("c3"),
                "the dependent record must be delivered after its cause — causal order across "
                        + "topics on the task's own partition");
    }

    /**
     * The shrinker operates on multi-partition traces: a failing schedule (here: an oracle that
     * "fails" whenever the foreign-partition ignore fired) minimises through
     * {@code shortestFailingPrefix} + ddmin, and the minimised trace survives a
     * {@code print}/{@code parse} round trip with its partition and key fields intact.
     *
     * Asserts that the shrunken trace still triggers the oracle, is no longer than the original,
     * and replays identically after a text round trip.
     */
    @Test
    void shrinkerMinimisesAndRoundTripsMultiPartitionTraces() {
        ParsleySimTrace.SimSpec spec = relaySpec();
        List<ParsleySimTrace.Action> full = List.of(
                new ParsleySimTrace.ProduceExternal("c1", 0),
                new ParsleySimTrace.AckAll("p1"),
                new ParsleySimTrace.ProduceExternal("c1", 1),
                new ParsleySimTrace.Poll("p1", "c1", 1, List.of(ParsleySimTrace.Emit.NONE)),
                new ParsleySimTrace.ProduceStamped("c1", 1,
                        List.of(new ParsleySimTrace.Observed("c1", 0, 0))),
                new ParsleySimTrace.Poll("p1", "c1", 0, List.of(ParsleySimTrace.Emit.NONE)),
                new ParsleySimTrace.Poll("p1", "c1", 1, List.of(ParsleySimTrace.Emit.NONE)));

        ParsleySimShrinker.Oracle oracle = candidate -> {
            ParsleyTopologySim replayed = ParsleyTopologySim.replay(spec, candidate);
            return replayed.crossPartitionIgnoredCauses > 0 ? "cross-partition ignore fired" : null;
        };
        ParsleySimShrinker.Shrunk shrunk = ParsleySimShrinker.shrink(full, oracle);

        assertTrue(shrunk.trace().size() <= full.size(),
                "the shrunken trace must be no longer than the original");
        assertNotNull(oracle.failureSignature(shrunk.trace()),
                "the shrunken trace must still trigger the target signature");

        ParsleySimTrace.Repro roundTripped =
                ParsleySimTrace.parse(ParsleySimTrace.print(spec, shrunk.trace()));
        assertEquals(2, roundTripped.spec().partitions(),
                "the partition count must survive the text round trip");
        assertNotNull(oracle.failureSignature(roundTripped.trace()),
                "the parsed trace must replay to the same signature — partition and key fields "
                        + "survive print/parse");
    }

    /** A partition-free trace text parses with partition fields defaulting to zero. */
    @Test
    void legacyTraceTextParsesWithPartitionZeroDefaults() {
        String legacy = """
                spec
                ext c1
                node p1 in=c1 out=c2 p=0.5
                trace
                ext c1
                poll p1 c1 [c2]
                rollback p1 c1 0
                """;
        ParsleySimTrace.Repro repro = ParsleySimTrace.parse(legacy);
        assertEquals(1, repro.spec().partitions(),
                "a spec without a partitions line must default to single-partition");
        assertEquals(new ParsleySimTrace.ProduceExternal("c1", 0), repro.trace().get(0),
                "a legacy ext line must parse with key index 0");
        assertEquals(new ParsleySimTrace.Poll("p1", "c1", 0,
                        List.of(new ParsleySimTrace.Emit(true, "c2"))), repro.trace().get(1),
                "a legacy poll line must parse with partition 0");
        assertEquals(new ParsleySimTrace.Rollback("p1", "c1", 0, 0), repro.trace().get(2),
                "a legacy rollback line must parse with partition 0");
    }
}
