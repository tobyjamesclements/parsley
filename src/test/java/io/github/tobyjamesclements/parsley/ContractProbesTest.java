package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ContractProbes} against known-good and deliberately broken applications: every probe
 * must pass a clean pipeline and flag the specific violation it exists for, with the finding
 * naming the probe and the contract clause. A probe that cannot fail is not a probe.
 */
class ContractProbesTest {

    private static final Topic<String, String> T1 = Topic.of("t1", Codec.utf8(), Codec.utf8());
    private static final String APPLICATION_ID = "parsley-probes";
    private static final Topic<String, String> MID = Topic.of("mid", Codec.utf8(), Codec.utf8());
    private static final Topic<String, String> T3 = Topic.of("t3", Codec.utf8(), Codec.utf8());

    @TempDir
    Path stateDir;

    private static Parsley pipeline() {
        Stage first = Stage.named("first")
                .on(T1, m -> List.of(MID.send(m.key(), m.value() + ":a")))
                .into(MID)
                .build();
        Stage second = Stage.named("second")
                .on(MID, m -> List.of(T3.send(m.key(), m.value() + ":b")))
                .into(T3)
                .build();
        return Parsley.named(APPLICATION_ID, first, second);
    }

    private static boolean hasProbe(List<ContractProbes.Finding> findings, String probe) {
        return findings.stream().anyMatch(f -> f.probe().equals(probe));
    }

    private static List<ContractProbes.Finding> withProbe(List<ContractProbes.Finding> findings,
                                                          String probe) {
        return findings.stream().filter(f -> f.probe().equals(probe)).toList();
    }

    /**
     * A clean two-stage pipeline passes every probe, and the unsampled source, and only it,
     * surfaces as a coverage note. A note on every source, or on the sampled one instead, would
     * make the coverage report worthless as the blind-spot list it is meant to be.
     */
    @Test
    void cleanPipelinePassesAllProbes() {
        var report = ContractProbes.probe(pipeline(),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);
        assertTrue(report.ok(), "a clean pipeline must pass: " + report);
        var coverage = withProbe(report.notes(), "coverage");
        assertEquals(1, coverage.size(),
                "exactly one source went unsampled, so exactly one coverage note is due: " + report);
        assertTrue(coverage.get(0).detail().contains("'mid'"),
                "the coverage note must name the source that had no sample, not one that did: "
                        + coverage.get(0));
        assertTrue(hasProbe(report.notes(), "names"),
                "the pinned store names must surface as a stability note");
        report.assertOk();
    }

    /**
     * The names note pins exactly the stores a stage actually keeps. A stateful stage keys a
     * fold store as well as its protocol store, and the note is what tells the deployment which
     * names must stay stable. Naming a store a stage does not keep, or omitting one it does,
     * misdirects the one decision the note exists to inform.
     */
    @Test
    void theNamesNotePinsTheFoldStoreOnlyForStatefulStages() {
        Stage stateless = Stage.named("stateless")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .into(T3)
                .build();
        Stage stateful = Stage.named("stateful")
                .state(Codec.int64(), () -> 0L)
                .on(MID, (Long n, Message<String, String> m) ->
                        Step.of(n + 1, T3.send(m.key(), m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stateless, stateful),
                ContractProbes.Samples.of().on(T1, "k", "v").on(MID, "k", "v"), stateDir);

        String statelessNote = withProbe(report.notes(), "names").stream()
                .map(ContractProbes.Finding::detail)
                .filter(d -> d.contains("'stateless'")).findFirst().orElseThrow();
        assertTrue(statelessNote.contains("stateless-state"),
                "every stage pins its protocol store: " + statelessNote);
        assertFalse(statelessNote.contains("stateless-fold"),
                "a stateless stage keeps no fold store, so none may be pinned: " + statelessNote);

        String statefulNote = withProbe(report.notes(), "names").stream()
                .map(ContractProbes.Finding::detail)
                .filter(d -> d.contains("'stateful'")).findFirst().orElseThrow();
        assertTrue(statefulNote.contains("stateful-fold"),
                "a stateful stage's fold store name must be pinned too: " + statefulNote);
    }

    /** A key codec that is not canonical (decode then encode changes bytes) is flagged. */
    @Test
    void nonCanonicalKeyCodecIsFlagged() {
        Codec<String> folding = Codec.of(
                s -> s.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b -> new String(b, java.nio.charset.StandardCharsets.UTF_8).toLowerCase());
        Topic<String, String> byFoldedKey = Topic.of("t1", folding, Codec.utf8());
        Stage stage = Stage.named("stage")
                .on(byFoldedKey, m -> List.of(T3.send(m.key(), m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(byFoldedKey, "MixedCase", "v"), stateDir);
        assertTrue(hasProbe(report.failures(), "key-codec"),
                "a non-canonical key codec must fail the key-codec probe: " + report);
    }

    /** An emission to an undeclared sink is reported as the violated Logic clause. */
    @Test
    void undeclaredSinkEmissionIsFlagged() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .into(MID) // t3 deliberately undeclared
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);
        assertTrue(hasProbe(report.failures(), "declared-sinks"),
                "an undeclared-sink emission must fail the declared-sinks probe: " + report);
        assertFalse(report.ok(), "a report carrying failures is not ok: " + report);
        assertTrue(withProbe(report.failures(), "declared-sinks").get(0).detail()
                        .contains("undeclared sink topic: t3"),
                "the finding must carry the runtime's own message, which names the sink that was"
                        + " not declared: " + report);
        var thrown = assertThrows(AssertionError.class, report::assertOk,
                "assertOk must throw on failures");
        assertTrue(thrown.getMessage().contains("declared-sinks"),
                "the assertion must carry the findings: " + thrown.getMessage());
    }

    /**
     * An unresolvable source is reported once, as the missing topic it is. Read instead as a
     * partition count of zero it would also appear to disagree with its co-partitioned sibling,
     * and the operator would chase a co-partitioning problem that does not exist.
     */
    @Test
    void clusterProbeReportsOnlyTheTopicItCannotResolve() {
        Stage join = Stage.named("join")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .on(MID, m -> List.of(T3.send(m.key(), m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probeCluster(Parsley.named(APPLICATION_ID, join), fixed(Map.of("t1", 4, "t3", 4)));

        assertEquals(List.of("topic-exists"),
                report.failures().stream().map(ContractProbes.Finding::probe).toList(),
                "the missing source is the only finding due: " + report);
    }

    /**
     * A sample with a null value is not a contract violation. Null keys and values pass through
     * the runtime untouched, so a codec is never asked to encode one. Probing as though it
     * were would fail a perfectly legal application on its own probe.
     */
    @Test
    void aSampleWithANullValueIsNotAContractViolation() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(m.key(), m.value() == null ? "absent" : m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", null), stateDir);

        assertTrue(report.ok(), "a null value is absent, not undecodable: " + report);
    }

    /**
     * An emission with a null key is not undecodable either. The key is absent, so no key codec
     * runs on either side of the hop. Treating absence as a decode failure would flag every
     * application that emits null-keyed records.
     */
    @Test
    void aNullKeyedEmissionIsNotReportedAsUndecodable() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(T3.send(null, m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);

        assertTrue(report.ok(), "a null key is absent, not undecodable: " + report);
    }

    /**
     * The sample reaches the handler with the key it was declared with. The encoded key bytes
     * are what choose the partition and key the fold state, so a probe that pipes the sample
     * without its key exercises a different code path than the deployment will.
     */
    @Test
    void theSamplesKeyIsPipedAsDeclared() {
        Stage keyed = Stage.named("keyed")
                .on(T1, m -> {
                    if (m.key() == null) throw new IllegalStateException("the sample's key was lost");
                    return List.of(T3.send(m.key(), m.value()));
                })
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, keyed),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);

        assertTrue(report.ok(), "the sample must arrive at the handler keyed as declared: " + report);
    }

    /**
     * A sink only a tick reaches is still probed as covered. The probe advances the driver's
     * wall clock past the widest declared interval precisely so tick logic runs. Without that
     * advance a perfectly reachable sink reads as one no sample can reach, and the coverage note
     * points at a blind spot that is the probe's own.
     */
    @Test
    void aSinkReachedOnlyByATickIsProbedAsCovered() {
        Stage stage = Stage.named("ticker")
                .on(T1, m -> List.of())
                .ticks(Duration.ofSeconds(1), tick -> List.of(T3.send("tick", "fired")))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);

        assertTrue(withProbe(report.notes(), "coverage").stream()
                        .noneMatch(f -> f.detail().contains("'t3'")),
                "the tick must have fired and reached its declared sink: " + report);
        assertTrue(report.ok(), "a ticking stage must pass: " + report);
    }

    /** A handler that throws on its sample is reported, with the root cause in the detail. */
    @Test
    void handlerFailureIsFlagged() {
        Stage stage = Stage.named("stage")
                .on(T1, m -> {
                    throw new IllegalStateException("boom");
                })
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);
        assertTrue(report.failures().stream().anyMatch(
                        f -> f.probe().equals("logic") && f.detail().contains("boom")),
                "a throwing handler must fail the logic probe with its cause: " + report);
    }

    /** A sample on a topic no stage consumes is a probe misconfiguration, not a silent no-op. */
    @Test
    void sampleOnNonSourceTopicIsFlagged() {
        var report = ContractProbes.probe(pipeline(),
                ContractProbes.Samples.of().on(T1, "k", "v").on(T3, "k", "v"), stateDir);
        assertTrue(hasProbe(report.failures(), "samples"),
                "a sample on a non-source topic must be flagged: " + report);
    }

    /** Output that the sink topic's own codecs cannot decode fails the hop's consumer side. */
    @Test
    void undecodableSinkOutputIsFlagged() {
        Topic<String, String> rejecting = Topic.of("t3", Codec.utf8(), Codec.of(
                s -> s.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b -> {
                    throw new IllegalStateException("undecodable");
                }));
        Stage stage = Stage.named("stage")
                .on(T1, m -> List.of(rejecting.send(m.key(), m.value())))
                .into(rejecting)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);
        assertTrue(hasProbe(report.failures(), "sink-decode"),
                "output the sink's codec cannot decode must be flagged: " + report);
    }

    /** A ticking stage passes: the tick fires under the driver, stamped, and is noted. */
    @Test
    void tickingStagePassesWithTickNote() {
        Stage stage = Stage.named("ticking")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .ticks(Duration.ofSeconds(1), tick -> List.of(T3.send("tick", "t")))
                .into(T3)
                .build();

        var report = ContractProbes.probe(Parsley.named(APPLICATION_ID, stage),
                ContractProbes.Samples.of().on(T1, "k", "v"), stateDir);
        assertTrue(report.ok(), "a ticking stage must pass under the driver: " + report);
        assertTrue(report.notes().stream().anyMatch(
                        f -> f.probe().equals("tick-topic")
                                && f.detail().contains(APPLICATION_ID + "-ticking-ticks")),
                "the tick-topic requirement must surface as a note: " + report);
    }

    /** The cluster probe passes a cluster that satisfies every topic clause. */
    @Test
    void clusterProbePassesAConformingCluster() {
        var report = ContractProbes.probeCluster(pipeline(),
                fixed(Map.of("t1", 4, "mid", 4, "t3", 2)));
        assertTrue(report.ok(), "a conforming cluster must pass: " + report);
    }

    /** A missing declared topic is flagged: everything exists before the application starts. */
    @Test
    void clusterProbeFlagsMissingTopic() {
        var report = ContractProbes.probeCluster(pipeline(), fixed(Map.of("t1", 4, "mid", 4)));
        assertTrue(hasProbe(report.failures(), "topic-exists"),
                "the missing sink t3 must be flagged: " + report);
    }

    /** Sources of one stage with unequal partition counts fail the co-partitioning half. */
    @Test
    void clusterProbeFlagsPartitionCountMismatch() {
        Stage join = Stage.named("join")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .on(MID, m -> List.of(T3.send(m.key(), m.value())))
                .into(T3)
                .build();

        var report = ContractProbes.probeCluster(Parsley.named(APPLICATION_ID, join),
                fixed(Map.of("t1", 4, "mid", 3, "t3", 4)));
        assertTrue(hasProbe(report.failures(), "co-partition"),
                "unequal source partition counts must be flagged: " + report);
    }

    /** A tick topic must carry exactly the partition count of the stage's widest source. */
    @Test
    void clusterProbeFlagsTickTopicPartitionMismatch() {
        Stage ticking = Stage.named("ticking")
                .on(T1, m -> List.of(T3.send(m.key(), m.value())))
                .ticks(Duration.ofSeconds(1), tick -> List.of())
                .into(T3)
                .build();

        var report = ContractProbes.probeCluster(Parsley.named(APPLICATION_ID, ticking),
                fixed(Map.of("t1", 4, "t3", 4, APPLICATION_ID + "-ticking-ticks", 1)));
        assertTrue(hasProbe(report.failures(), "tick-topic"),
                "a mis-partitioned tick topic must be flagged: " + report);

        var conforming = ContractProbes.probeCluster(Parsley.named(APPLICATION_ID, ticking),
                fixed(Map.of("t1", 4, "t3", 4, APPLICATION_ID + "-ticking-ticks", 4)));
        assertTrue(conforming.ok(), "a correctly partitioned tick topic must pass: " + conforming);
    }

    /** Findings render probe, detail, and clause, the anchor a debugging session lands on. */
    @Test
    void findingsCarryTheContractClause() {
        var report = ContractProbes.probeCluster(pipeline(), fixed(Map.of()));
        // Four findings for three topics: 'mid' is flagged in both of its roles (first's
        // sink, second's source), each finding naming the stage it fails.
        assertEquals(4, report.failures().size(), "every topic role must be flagged: " + report);
        for (var finding : report.failures()) {
            assertTrue(finding.toString().contains("see Parsley"),
                    "each finding must name its contract clause: " + finding);
        }
    }

    /** A resolver over a fixed name-to-partition-count map. Absent names are unresolvable. */
    private static TopicIds fixed(Map<String, Integer> partitions) {
        return topic -> {
            Integer count = partitions.get(topic);
            if (count == null) throw new IllegalStateException("cannot resolve topic " + topic);
            return new TopicIds.Resolved(UUID.nameUUIDFromBytes(topic.getBytes()), count);
        };
    }
}
