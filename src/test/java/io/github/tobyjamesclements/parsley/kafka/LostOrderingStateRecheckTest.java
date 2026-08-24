package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes what the lost-ordering-state refusal decides from its own second look.
 *
 * <p>{@code refuseLostOrderingState} sees committed read positions the bootstrap did not
 * stamp over a changelog partition that held no records — the shape of committed state
 * lost (D76, per partition since D88). But the view it holds was read before the offsets
 * were listed, and a pause of arbitrary duration lands between any two statements (SPEC
 * Fault model 2): a concurrent lifetime of this process can have created records — and
 * committed — in that window. So the refusal looks again immediately before firing, and
 * what the second look finds decides everything (D84): records now present is a healthy
 * concurrent sibling and must refuse as a retryable transient, because the state-loss
 * diagnosis's printed remedy — delete the group's offsets — would destroy what that
 * sibling just wrote. The recheck seam scripts the second look, following the
 * {@code readToEnds} / {@code ScriptedFacts} precedent (D92).
 */
@Timeout(value = 10)
class LostOrderingStateRecheckTest {
    private static final String APP = "app-shipper";
    private static final TopicPartition IN0 = new TopicPartition("orders", 0);
    private static final ParsleyRuntime.ChangelogView NO_RECORDS =
            new ParsleyRuntime.ChangelogView(Map.of(), Set.of());
    private static final OffsetAndMetadata STREAMS_STAMPED = new OffsetAndMetadata(5, "streams/v1");

    private static ParsleyRuntime.ChangelogView viewWithRecordsIn(int partition) {
        return new ParsleyRuntime.ChangelogView(Map.of(), Set.of(partition));
    }

    /**
     * Catches the recheck arm being deleted or its verdict inverted (SAFETY): ordering
     * records that appeared between the first read and the offset listing are a healthy
     * concurrent lifetime, and the refusal must be the retryable
     * ordering-records-appeared transient — misdiagnosing it as ORDERING_STATE_LOST hands
     * the operator a remedy that deletes the offsets that sibling just committed (D84).
     */
    @Test
    void recordsAppearingOnTheRecheckRefuseAsRetryableNotStateLoss() {
        ParsleyRuntime.RetryableStartException sibling =
                assertThrows(ParsleyRuntime.RetryableStartException.class,
                        () -> ParsleyRuntime.refuseLostOrderingState(APP, NO_RECORDS,
                                Map.of(IN0, STREAMS_STAMPED), () -> Optional.of(viewWithRecordsIn(0))),
                        "records present on the second look mean a concurrent lifetime, not state"
                                + " loss; the refusal must be the retryable transient");
        assertTrue(sibling.getMessage().contains("ordering records appeared while"
                        + " this start was determining prior state"),
                "the diagnosis must name what actually happened — records appearing mid-start: "
                        + sibling.getMessage());
        assertTrue(sibling.getMessage().contains("a concurrent lifetime of this process"),
                "the diagnosis must name the concurrent-lifetime condition, so the operator"
                        + " retries instead of deleting offsets: " + sibling.getMessage());
        assertTrue(sibling.getMessage().contains("Retry this start."),
                "the printed remedy must be a retry, never a destructive reset: " + sibling.getMessage());
    }

    /**
     * Catches the state-loss arm being weakened: a recheck that still shows the partition
     * recordless corroborates the loss, and the refusal must be terminal
     * ORDERING_STATE_LOST naming the emptied-partition shape — the operator has to know
     * the topic survived while its records did not (D84, per partition D88).
     */
    @Test
    void aRecheckStillShowingNoRecordsRefusesAsStateLossNamingTheEmptiedShape() {
        ParsleyFailClosedException loss = assertThrows(ParsleyFailClosedException.class,
                () -> ParsleyRuntime.refuseLostOrderingState(APP, NO_RECORDS,
                        Map.of(IN0, STREAMS_STAMPED), () -> Optional.of(NO_RECORDS)),
                "a corroborated recordless partition behind unstamped offsets is committed"
                        + " state lost and must refuse terminally");
        assertEquals(ParsleyFailClosedException.Reason.ORDERING_STATE_LOST, loss.reason(),
                "the refusal must carry ORDERING_STATE_LOST, the reason supervisors key on");
        assertTrue(loss.getMessage().contains("partition 0 of this process's ordering-store changelog"
                        + " holds no ordering records"),
                "the diagnosis must name the emptied-partition shape, not a missing topic: "
                        + loss.getMessage());
        assertTrue(loss.getMessage().contains("stamped by a previous Kafka Streams execution"),
                "a non-empty non-bootstrap stamp is a prior Streams execution's, and the"
                        + " diagnosis must say so: " + loss.getMessage());
    }

    /**
     * Catches the two loss shapes being conflated: a recheck finding no changelog topic at
     * all must refuse as ORDERING_STATE_LOST naming the does-not-exist shape — restoring a
     * deleted topic is a different operator action from restoring purged records (D84
     * requires the message to name the shape it found).
     */
    @Test
    void anAbsentChangelogOnTheRecheckRefusesAsStateLossNamingTheMissingTopic() {
        ParsleyFailClosedException loss = assertThrows(ParsleyFailClosedException.class,
                () -> ParsleyRuntime.refuseLostOrderingState(APP, NO_RECORDS,
                        Map.of(IN0, STREAMS_STAMPED), Optional::empty),
                "unstamped offsets with no changelog topic behind them are committed state"
                        + " lost and must refuse terminally");
        assertEquals(ParsleyFailClosedException.Reason.ORDERING_STATE_LOST, loss.reason(),
                "the refusal must carry ORDERING_STATE_LOST for the missing-topic shape too");
        assertTrue(loss.getMessage().contains("this process's ordering-store changelog does not exist"),
                "the diagnosis must name the missing-topic shape, not the emptied one: "
                        + loss.getMessage());
    }

    /**
     * Catches the provenance naming regressing: an empty-metadata offset was committed
     * outside parsley — Kafka Streams stamps every commit, the bootstrap stamps its own —
     * and the diagnosis must attribute it to external tooling so the operator looks at
     * the right actor (D76's diagnosis, shape-named per D84).
     */
    @Test
    void anUnstampedOffsetIsAttributedToExternalTooling() {
        ParsleyFailClosedException loss = assertThrows(ParsleyFailClosedException.class,
                () -> ParsleyRuntime.refuseLostOrderingState(APP, NO_RECORDS,
                        Map.of(IN0, new OffsetAndMetadata(5)), Optional::empty),
                "an offset with empty metadata over a recordless partition is the same loss"
                        + " shape, differently provenanced");
        assertTrue(loss.getMessage().contains("committed outside parsley"),
                "empty metadata means neither Streams nor the bootstrap wrote the offset; the"
                        + " diagnosis must say external tooling: " + loss.getMessage());
    }

    /**
     * Catches the bootstrap-stamp exemption being dropped (the refusal over-reaching): a
     * first-start bootstrap that crashed after committing initial positions leaves
     * offsets with no records anywhere, but every one carries the bootstrap's own stamp —
     * recovery from that crash must start, not refuse, and must not even pay the recheck
     * (D76/D84; the end-to-end counterpart is
     * {@code BootstrapIntegrationTest#bootstrapCommittedOffsetsWithoutAChangelogStillStart}).
     */
    @Test
    void bootstrapStampedOffsetsNeverTriggerTheRecheckOrTheRefusal() {
        AtomicInteger rechecks = new AtomicInteger();

        assertDoesNotThrow(() -> ParsleyRuntime.refuseLostOrderingState(APP, NO_RECORDS,
                        Map.of(IN0, new OffsetAndMetadata(0, ParsleyRuntime.BOOTSTRAP_OFFSET_STAMP)),
                        () -> {
                            rechecks.incrementAndGet();
                            return Optional.empty();
                        }),
                "bootstrap-stamped offsets over an empty changelog are first-start crash"
                        + " recovery and must start, not refuse");
        assertEquals(0, rechecks.get(),
                "the bootstrap stamp settles the question before any second look; recovery must"
                        + " not pay the recheck's describes");
    }

    /**
     * Catches the records-present skip being dropped: a partition whose records were in
     * the first read's view is healthy prior state, and the scan must pass it without
     * invoking the recheck — re-reading the changelog once per healthy partition would
     * tax every restart with describes it does not need (D88 keys the loss per partition).
     */
    @Test
    void aPartitionWhoseRecordsWereInTheFirstViewPassesWithoutRecheck() {
        AtomicInteger rechecks = new AtomicInteger();

        assertDoesNotThrow(() -> ParsleyRuntime.refuseLostOrderingState(APP, viewWithRecordsIn(0),
                        Map.of(IN0, STREAMS_STAMPED), () -> {
                            rechecks.incrementAndGet();
                            return Optional.of(viewWithRecordsIn(0));
                        }),
                "records in the first view are the prior state the offsets promise; the scan"
                        + " must pass the partition");
        assertEquals(0, rechecks.get(),
                "a healthy partition must not pay a recheck; the second look is for the"
                        + " contradiction only");
    }
}
