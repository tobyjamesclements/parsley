package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
     * depth 65 is not — so a cyclic chain cannot hang the uncaught-exception handler, and
     * the classification reaches exactly as deep as the refusal search {@code status()}
     * relies on.
     */
    @Test
    void depthSixtyThreeIsClassifiedAndDepthSixtyFiveIsNot() {
        assertEquals(ParsleyRuntime.FailureDiagnosis.POSITIONS_DISCARDED_UNREAD,
                ParsleyRuntime.classifyFailure(
                        buriedAtDepth(63, new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "a trigger at depth 63 sits inside the 64-link bound and must still be named");
        assertEquals(ParsleyRuntime.FailureDiagnosis.UNRECOGNISED,
                ParsleyRuntime.classifyFailure(
                        buriedAtDepth(65, new OffsetOutOfRangeException(Map.of(TP, 5L)))),
                "a trigger past the 64-link bound must fall to the generic diagnosis rather than"
                        + " risk walking a cyclic chain forever");
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
}
