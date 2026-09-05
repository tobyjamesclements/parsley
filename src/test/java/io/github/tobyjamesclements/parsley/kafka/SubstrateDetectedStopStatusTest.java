package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that a stop the substrate detects, but that recurs identically on restart,
 * reaches {@code status()} with its reason (D109, Operational 1). The main consumer's
 * OffsetOutOfRangeException under auto.offset.reset=none is the consumer-level half of
 * Safety 8; a held message's persisted form exceeding the changelog's record limit is a
 * substrate configuration the guarantee cannot survive. Both used to reach status() as
 * bare client exceptions, which a supervisor keyed on refusalReason reads as transient.
 * A partition-shape change or a missing committed position is resolved by a restart, so
 * those stay transient.
 */
class SubstrateDetectedStopStatusTest {

    /** The consumer-level Safety 8 stop carries POSITIONS_DISCARDED_UNREAD. */
    @Test
    void offsetOutOfRangeStopCarriesThePositionsDiscardedUnreadReasonIntoStatus() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        Throwable streamsWrapped = new StreamsException("stream thread died",
                new KafkaException("client failed",
                        new OffsetOutOfRangeException(Map.of(new TopicPartition("orders", 0), 5L))));

        runtime.recordFailure("p", streamsWrapped);

        ParsleyFailClosedException refusal = ParsleyFailClosedException.findIn(runtime.recordedFailure("p"));
        assertNotNull(refusal, "a Safety 8 stop detected by the consumer must reach status() as a refusal,"
                + " not as an undiagnosed transient (Operational 1)");
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, refusal.reason(),
                "the consumer's out-of-range stop is the engine's discarded-unread refusal");
    }

    /** A record beyond the substrate's size limit carries SUBSTRATE_MISCONFIGURED. */
    @Test
    void recordTooLargeStopCarriesTheSubstrateMisconfiguredReasonIntoStatus() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        runtime.recordFailure("p", new StreamsException("stream thread died",
                new org.apache.kafka.common.errors.RecordTooLargeException("too large")));
        ParsleyFailClosedException refusal = ParsleyFailClosedException.findIn(runtime.recordedFailure("p"));
        assertNotNull(refusal, "a size limit the changelog cannot take is a substrate configuration the guarantee"
                + " cannot survive, and recurs identically until it changes");
        assertEquals(ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED, refusal.reason(),
                "a record too large for the changelog is a substrate limit, not a transient");
        assertTrue(refusal.getMessage().contains("max.message.bytes"), refusal.getMessage());
    }

    /**
     * A received topic missing at a rebalance stays transient (D115): a restart either
     * resumes it — the topic merely lagged in a broker's metadata — or refuses with the
     * start path's own diagnosis of what became of it, so the stop itself carries no reason.
     */
    @Test
    void aMissingSourceTopicStaysATransientWithNoRefusalReason() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        StreamsException stop = new StreamsException("stream thread died",
                new org.apache.kafka.streams.errors.MissingSourceTopicException(
                        "One or more source topics were missing during rebalance"));
        runtime.recordFailure("p", stop);
        assertSame(stop, runtime.recordedFailure("p"),
                "the stop is recorded — status() and awaitStopped depend on it — merely without a reason");
        assertNull(ParsleyFailClosedException.findIn(runtime.recordedFailure("p")),
                "a missing source topic is diagnosed by the restart, so it must not read as a deliberate stop");
    }

    /** A stop a restart resolves stays transient: no refusal reason. */
    @Test
    void aPartitionShapeChangeStaysATransientWithNoRefusalReason() {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        runtime.recordFailure("p", new StreamsException("invalid partitions: topic grew while running"));
        assertNull(ParsleyFailClosedException.findIn(runtime.recordedFailure("p")),
                "a partition-shape change is re-resolved by a restart, so it must not read as a deliberate stop");
    }
}
