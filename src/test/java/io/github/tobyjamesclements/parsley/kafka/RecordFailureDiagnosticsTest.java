package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes how a stream thread's uncaught failure is diagnosed and retained.
 *
 * <p>{@code recordFailure} is the runtime's last diagnostic seam: whatever it names is all
 * the operator gets for a mid-run stop (Operational 1/6, D81's taxonomy). Kafka Streams
 * wraps the triggering exception in layers of {@link StreamsException}/{@link KafkaException},
 * so the classification must walk the cause chain — bounded, because a cyclic chain must
 * not hang the uncaught-exception handler — and the failure {@code status()} later unwraps
 * is chosen by a merge whose precedence keeps the fail-closed refusal (D55) over any
 * follow-on transient.
 */
class RecordFailureDiagnosticsTest {
    private static final TopicPartition TP = new TopicPartition("orders", 0);

    /** Buries {@code trigger} under {@code depth} generic wrappers, as Streams wrapping does. */
    private static Throwable buriedAtDepth(int depth, Throwable trigger) {
        Throwable chain = trigger;
        for (int i = 0; i < depth; i++) {
            chain = new RuntimeException("wrapper layer " + i, chain);
        }
        return chain;
    }

    /** The two wrapper layers Streams typically adds around a client exception. */
    private static Throwable streamsWrapped(Throwable trigger) {
        return new StreamsException("stream thread died", new KafkaException("client failed", trigger));
    }

    /**
     * Catches the diagnosis regressing to the generic fallback for the loud half of
     * Safety 8's guard: retention passing surviving committed offsets must be named
     * POSITIONS_DISCARDED_UNREAD with its deliberate-reset remedy (D81), however deep
     * Streams buries the {@link OffsetOutOfRangeException}.
     */
    @Test
    void wrappedOffsetOutOfRangeNamesPositionsDiscardedUnread() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.POSITIONS_DISCARDED_UNREAD,
                ParsleyRuntime.classifyFailure(streamsWrapped(new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "an OffsetOutOfRangeException buried under Streams wrappers must be named as"
                        + " retention discarding committed positions, not logged generically");
    }

    /**
     * Catches the missing-source-topic diagnosis regressing to the generic fallback (D115):
     * a received topic deleted, or deleted and recreated, while the process ran surfaces
     * at the host's next rebalance as {@link MissingSourceTopicException}, by type when the
     * exception survives Streams' wrapping and by its message when only the text does. It
     * stays a transient — a restart diagnoses which it was — but names its condition.
     */
    @Test
    void wrappedMissingSourceTopicNamesTheMissingSourceTopic() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.SOURCE_TOPIC_MISSING,
                ParsleyRuntime.classifyFailure(streamsWrapped(
                        new MissingSourceTopicException("One or more source topics were missing during rebalance"))),
                "a MissingSourceTopicException buried under Streams wrappers must be named");
        assertEquals(ParsleyRuntime.FailureDiagnosis.SOURCE_TOPIC_MISSING,
                ParsleyRuntime.classifyFailure(
                        new StreamsException("One or more source topics were missing during rebalance")),
                "the message alone names the condition where the type is lost in a wrapper");
    }

    /**
     * Catches the no-offset diagnosis collapsing back into the partition-shape message it
     * was split from: D81 separates a received partition with no committed position (a
     * partition added mid-run, or offsets removed mid-run) into its own named condition.
     */
    @Test
    void wrappedNoOffsetForPartitionNamesTheMissingPosition() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.NO_COMMITTED_POSITION,
                ParsleyRuntime.classifyFailure(streamsWrapped(new NoOffsetForPartitionException(TP))),
                "a NoOffsetForPartitionException buried under Streams wrappers must be named as a"
                        + " missing committed position with the restart remedy, not logged generically");
    }

    /**
     * Catches the oversized-record diagnosis regressing to the generic fallback: a held
     * message whose persisted form outgrew the changelog's max.message.bytes needs the
     * raise-and-restart remedy named, because the metadata budget alone does not bound it
     * (D87).
     */
    @Test
    void wrappedRecordTooLargeNamesTheSizeLimit() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.RECORD_TOO_LARGE,
                ParsleyRuntime.classifyFailure(streamsWrapped(new RecordTooLargeException("2097152 bytes"))),
                "a RecordTooLargeException buried under Streams wrappers must name the size-limit"
                        + " condition and its max.message.bytes remedy, not log generically");
    }

    /**
     * Catches the message probe for the mid-run partition-shape change being dropped or
     * narrowed: Streams reports the assignor's refusal only as text, so the width-change
     * diagnosis (D59) keys on the "invalid partitions" substring — and only on it, so an
     * unrelated failure must still fall through to the generic log.
     */
    @Test
    void invalidPartitionsMessageNamesTheShapeChangeAndOtherTextDoesNot() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.PARTITION_SHAPE_CHANGED,
                ParsleyRuntime.classifyFailure(streamsWrapped(
                        new IllegalStateException("assignment failed: invalid partitions for task 0_1"))),
                "an otherwise-generic failure whose message reports invalid partitions must be"
                        + " named as a mid-run partition-shape change (D59)");
        assertEquals(ParsleyRuntime.FailureDiagnosis.UNRECOGNISED,
                ParsleyRuntime.classifyFailure(streamsWrapped(
                        new IllegalStateException("assignment failed: something unrelated"))),
                "a generic failure without the invalid-partitions text must not be dressed as a"
                        + " partition-shape diagnosis (Operational 6)");
    }

    /**
     * Catches the cause-chain bound shrinking or growing away from {@code findIn}'s: the
     * walk inspects depths 0 through 63 — a trigger at depth 63 is still named, one at
     * depth 64, the first excluded depth, is not — so a cyclic chain cannot hang the
     * uncaught-exception handler, and the classification reaches exactly as deep as the
     * refusal search {@code status()} relies on. Probing depth 65 instead let the bound
     * grow to 65 unnoticed, desynchronised from {@code findIn}'s 64.
     */
    @Test
    void depthSixtyThreeIsClassifiedAndDepthSixtyFourIsNot() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.POSITIONS_DISCARDED_UNREAD,
                ParsleyRuntime.classifyFailure(
                        buriedAtDepth(63, new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "a trigger at depth 63 sits inside the 64-link bound and must still be named");
        assertEquals(ParsleyRuntime.FailureDiagnosis.UNRECOGNISED,
                ParsleyRuntime.classifyFailure(
                        buriedAtDepth(64, new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "a trigger at depth 64 — the first depth past the bound — must fall to the"
                        + " generic diagnosis rather than risk walking a cyclic chain forever");
    }

    /**
     * Pins the precedence when one chain evidences two conditions. The walk is outward-in
     * and, within one link, the instanceof checks precede the message probe: an outer
     * link whose message reports invalid partitions is named before a deeper
     * OffsetOutOfRangeException is reached, while an OffsetOutOfRangeException whose own
     * message carries the text is still named by its type. Catches a reordering of the
     * branches or of the walk silently changing which condition the operator is told.
     */
    @Test
    void anOuterInvalidPartitionsLinkWinsAndWithinALinkTheTypeWins() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.PARTITION_SHAPE_CHANGED,
                ParsleyRuntime.classifyFailure(new StreamsException(
                        "assignment failed: invalid partitions for task 0_1",
                        new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "the walk is outward-in: an outer link's invalid-partitions text must be named"
                        + " before a deeper OffsetOutOfRangeException is reached");
        assertEquals(ParsleyRuntime.FailureDiagnosis.POSITIONS_DISCARDED_UNREAD,
                ParsleyRuntime.classifyFailure(
                        new OffsetOutOfRangeException("invalid partitions", Map.of(TP, 5L))),
                "within one link the instanceof checks precede the message probe: an out-of-range"
                        + " exception mentioning invalid partitions is still named by its type");
    }

    /**
     * Catches the retained failure regressing to last-writer-wins in either direction:
     * {@code status()} unwraps the fail-closed refusal for the operator (D55), so a
     * follow-on transient must never bury an already-recorded refusal, and a refusal
     * arriving after a transient must displace it.
     */
    @Test
    void aFailClosedDiagnosisIsKeptOverATransientInEitherOrder() {
        Throwable transientFailure = streamsWrapped(new IllegalStateException("broker away"));
        Throwable refusal = streamsWrapped(new ParsleyFailClosedException(
                ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED, "width changed"));

        assertSame(refusal, ParsleyRuntime.preferFailClosedDiagnosis(transientFailure, refusal),
                "a refusal arriving after a transient must displace it, or status() would show"
                        + " no refusalReason for a deliberate stop");
        assertSame(refusal, ParsleyRuntime.preferFailClosedDiagnosis(refusal, transientFailure),
                "a transient arriving after a refusal must never bury it: the refusal is what"
                        + " status() unwraps for the operator (D55)");
    }

    /**
     * Pins the tie cases to first-recorded-wins: two refusals keep the first (the second
     * is a consequence of the state the first already diagnosed), and two transients keep
     * the first (the earliest failure is the closest to the cause). Catches the merge
     * silently flipping to last-writer-wins where no refusal forces a choice.
     */
    @Test
    void betweenTwoRefusalsOrTwoTransientsTheFirstRecordedStands() {
        Throwable refusalA = streamsWrapped(new ParsleyFailClosedException(
                ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED, "first refusal"));
        Throwable refusalB = streamsWrapped(new ParsleyFailClosedException(
                ParsleyFailClosedException.Reason.ORDERING_STATE_LOST, "second refusal"));
        Throwable transientA = streamsWrapped(new IllegalStateException("first transient"));
        Throwable transientB = streamsWrapped(new IllegalStateException("second transient"));

        assertSame(refusalA, ParsleyRuntime.preferFailClosedDiagnosis(refusalA, refusalB),
                "the first recorded refusal stands; a second refusal must not displace it");
        assertSame(transientA, ParsleyRuntime.preferFailClosedDiagnosis(transientA, transientB),
                "the first recorded transient stands; a later transient must not displace it");
    }

    /**
     * Catches {@code recordFailure} bypassing the precedence merge — regressing to a
     * plain last-writer-wins put: the retained failure must be chosen by
     * {@code preferFailClosedDiagnosis}, so the refusal {@code status()} unwraps (D55)
     * survives a follow-on transient recorded after it. This is the wiring leg the
     * merge-precedence pins above cannot see; the runtime is built without an Admin,
     * which the failure path never touches.
     */
    @Test
    void recordFailureRetainsTheRefusalThroughTheMergeWiring() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        Throwable transientFailure = streamsWrapped(new IllegalStateException("broker away"));
        Throwable refusal = streamsWrapped(new ParsleyFailClosedException(
                ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED, "width changed"));

        // Captured and discarded: this test pins the merge, not the log lines, and the
        // scripted failures should not shout through the suite's output.
        PrintStream realErr = System.err;
        try {
            System.setErr(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
            runtime.recordFailure("shipper", transientFailure);
            runtime.recordFailure("shipper", refusal);
            assertSame(refusal, runtime.recordedFailure("shipper"),
                    "a refusal recorded after a transient must displace it, or status() would"
                            + " show no refusalReason for a deliberate stop");
            runtime.recordFailure("shipper", streamsWrapped(new IllegalStateException("follow-on")));
            assertSame(refusal, runtime.recordedFailure("shipper"),
                    "a follow-on transient must not bury the retained refusal: recordFailure has"
                            + " to merge through preferFailClosedDiagnosis, not overwrite");
        } finally {
            System.setErr(realErr);
        }
    }

    /**
     * Catches a diagnosis being wired to another condition's log line — say two switch
     * arms swapped in {@code recordFailure}: each classified failure must log its own
     * condition and remedy against its own process. The lines are read off
     * {@code System.err}, where slf4j-simple writes them (the BudgetWarningTest
     * technique); a distinct process name per trigger keys each line to the failure
     * that produced it.
     */
    @Test
    void eachDiagnosisLogsItsOwnConditionAndRemedyAgainstItsOwnProcess() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream realErr = System.err;
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            runtime.recordFailure("p-discarded",
                    streamsWrapped(new OffsetOutOfRangeException(Map.of(TP, 5L))));
            runtime.recordFailure("p-noposition", streamsWrapped(new NoOffsetForPartitionException(TP)));
            runtime.recordFailure("p-toolarge", streamsWrapped(new RecordTooLargeException("2097152 bytes")));
            runtime.recordFailure("p-shape", streamsWrapped(
                    new IllegalStateException("assignment failed: invalid partitions for task 0_1")));
            runtime.recordFailure("p-missing", streamsWrapped(
                    new MissingSourceTopicException("One or more source topics were missing during rebalance")));
            runtime.recordFailure("p-generic", streamsWrapped(new IllegalStateException("something else")));
        } finally {
            System.setErr(realErr);
        }
        String logged = captured.toString(StandardCharsets.UTF_8);

        String discarded = lineNaming(logged, "process p-discarded");
        assertTrue(discarded.contains("positions were discarded before they were read")
                        && discarded.contains("Reset the process's state and group offsets"),
                "the discarded-positions failure must log Safety 8's condition and its"
                        + " deliberate-reset remedy on its own process's line: " + discarded);
        String noPosition = lineNaming(logged, "process p-noposition");
        assertTrue(noPosition.contains("a received partition has no committed read position")
                        && noPosition.contains("Restart the application"),
                "the missing-position failure must log D81's split condition and the restart"
                        + " remedy on its own process's line: " + noPosition);
        String tooLarge = lineNaming(logged, "process p-toolarge");
        assertTrue(tooLarge.contains("a record exceeded a size limit")
                        && tooLarge.contains("Raise max.message.bytes"),
                "the oversized-record failure must log D87's condition and the"
                        + " max.message.bytes remedy on its own process's line: " + tooLarge);
        String shape = lineNaming(logged, "process p-shape");
        assertTrue(shape.contains("the partition shape of its topics changed while it ran")
                        && shape.contains("Restart the application"),
                "the shape-change failure must log D59's condition and the restart remedy on"
                        + " its own process's line: " + shape);
        String missing = lineNaming(logged, "process p-missing");
        assertTrue(missing.contains("a received topic was missing when the host rebalanced")
                        && missing.contains("CHANNEL_IDENTITY_CHANGED"),
                "the missing-source-topic failure must log D115's condition and what a restart"
                        + " diagnoses on its own process's line: " + missing);
        assertTrue(lineNaming(logged, "process p-generic")
                        .contains("failed; shutting its application down (failing closed)"),
                "an unrecognised failure must keep the generic shutting-down line, undressed"
                        + " as any named condition (Operational 6)");
    }

    /** The first captured log line naming {@code marker}; fails the test if none does. */
    private static String lineNaming(String logged, String marker) {
        for (String line : logged.split("\\R")) {
            if (line.contains(marker)) {
                return line;
            }
        }
        throw new AssertionError("no captured log line names '" + marker + "'; recordFailure"
                + " logged nothing for that process. Captured: " + logged);
    }
}
