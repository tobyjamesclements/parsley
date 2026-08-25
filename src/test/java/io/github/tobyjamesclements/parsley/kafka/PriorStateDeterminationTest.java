package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes how the start's prior-state determination ends: with a corroborated answer,
 * or loudly — never with a guess.
 *
 * <p>{@code describeChangelogCorroborated} is the retry loop behind the bootstrap's
 * ordering-changelog describe. One unknown-topic answer is not proof of absence — a
 * describe is served from a single broker's possibly lagging metadata view — so absence is
 * concluded only from three consistent unknown answers (D84). Anything other than an
 * unknown-topic answer proves nothing about absence at all: a generic failure — a timeout,
 * a broker outage — that concluded "absent" would resume a process with prior state as a
 * first start, silently under-expressing every cause its lost state carried, so it must
 * refuse instead. The scripted-answer seam follows the {@code readToEnds} /
 * {@code ScriptedFacts} precedent (D92).
 */
@Timeout(value = 10)
class PriorStateDeterminationTest {
    private static final String APP = "app-shipper";
    /** Scripted answers carry no real broker latency, so the loop's backoff is ~zero. */
    private static final java.time.Duration NO_BACKOFF = java.time.Duration.ZERO;

    private static TopicDescription description(int partitions) {
        Node node = new Node(1, "broker", 9092);
        List<TopicPartitionInfo> infos = new java.util.ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            infos.add(new TopicPartitionInfo(partition, node, List.of(node), List.of(node)));
        }
        return new TopicDescription(ProcessTopology.changelogName(APP, ProcessTopology.ORDERING_STORE),
                false, infos, Set.of(), Uuid.randomUuid());
    }

    /**
     * Catches the generic-failure arm being deleted or falling through to "absent"
     * (SAFETY): a persistent failure that is not an unknown-topic answer — here a broker
     * timeout — carries no evidence of absence, and concluding "absent" from it would
     * resume a process with prior state as a first start (D84). The refusal must come
     * immediately, name the could-not-determine diagnosis, and keep the failure as its
     * cause for the operator.
     */
    @Test
    void aPersistentGenericFailureRefusesRatherThanConcludingAbsent() {
        ExecutionException outage = new ExecutionException(
                new org.apache.kafka.common.errors.TimeoutException("scripted broker outage"));
        AtomicInteger describes = new AtomicInteger();

        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> ParsleyRuntime.describeChangelogCorroborated(APP, () -> {
                    describes.incrementAndGet();
                    throw outage;
                }, NO_BACKOFF),
                "a failure that is not an unknown-topic answer must refuse the start, never"
                        + " conclude the changelog is absent and resume as a first start");
        assertTrue(refusal.getMessage().contains(APP + ": could not determine prior state; refusing to start"),
                "the refusal must carry the could-not-determine diagnosis for the operator: "
                        + refusal.getMessage());
        assertSame(outage, refusal.getCause(),
                "the scripted failure must survive as the refusal's cause; the operator has to"
                        + " see what actually failed");
        assertEquals(1, describes.get(),
                "a generic failure proves nothing more on retry; the refusal must be immediate,"
                        + " not burn the corroboration budget first");
    }

    /**
     * Catches the corroboration being weakened to a single answer — the exact
     * single-answer trust D84 removed: absence must be concluded only after three
     * consistent unknown-topic answers, so one stale broker view cannot misdiagnose a
     * healthy sibling's state as ORDERING_STATE_LOST, whose printed remedy deletes
     * offsets.
     */
    @Test
    void absenceIsConcludedOnlyFromThreeConsistentUnknownAnswers() {
        AtomicInteger describes = new AtomicInteger();

        var verdict = ParsleyRuntime.describeChangelogCorroborated(APP, () -> {
            describes.incrementAndGet();
            throw new ExecutionException(new UnknownTopicOrPartitionException("scripted unknown"));
        }, NO_BACKOFF);

        assertTrue(verdict.isEmpty(),
                "three consistent unknown answers are the corroborated evidence of absence (D84)");
        assertEquals(3, describes.get(),
                "absence must cost exactly three consistent unknown answers — fewer is the"
                        + " single-answer trust D84 removed, more starves the start budget");
    }

    /**
     * Catches the transient arm breaking: one unknown answer from a lagging broker
     * followed by a successful describe must return the description — the changelog
     * exists, prior state is real — not conclude absence from the first stale answer
     * (D84's misdiagnosis shape) and not refuse a start that just corroborated itself.
     */
    @Test
    void aTransientUnknownFollowedBySuccessReturnsTheDescription() {
        TopicDescription real = description(2);
        AtomicInteger describes = new AtomicInteger();

        var verdict = ParsleyRuntime.describeChangelogCorroborated(APP, () -> {
            if (describes.incrementAndGet() == 1) {
                throw new ExecutionException(new UnknownTopicOrPartitionException("scripted lagging view"));
            }
            return real;
        }, NO_BACKOFF);

        assertSame(real, verdict.orElseThrow(() -> new AssertionError(
                        "a describe that succeeded within the corroboration window must report the"
                                + " changelog present, not absent")),
                "the successful describe's description must be the verdict, untouched");
        assertEquals(2, describes.get(),
                "success must end the loop immediately; further describes would tax every start"
                        + " behind one slow answer");
    }

    /**
     * Catches the interrupt diagnosis being dropped or the interrupt being swallowed: a
     * thread interrupted between corroboration attempts must refuse with the
     * interrupted-specific message — not the generic could-not-determine shape, and never
     * "absent" — and must leave the interrupt flag set so the caller shutting the runtime
     * down still sees its own signal.
     */
    @Test
    void interruptionBetweenAttemptsRefusesAndPreservesTheInterrupt() {
        try {
            Thread.currentThread().interrupt();
            IllegalStateException refusal = assertThrows(IllegalStateException.class,
                    () -> ParsleyRuntime.describeChangelogCorroborated(APP, () -> {
                        throw new ExecutionException(new UnknownTopicOrPartitionException("scripted unknown"));
                    }, NO_BACKOFF),
                    "an interrupted determination must refuse, not conclude absence from the"
                            + " answers it never finished corroborating");
            assertTrue(refusal.getMessage().contains(
                            APP + ": interrupted while determining prior state; refusing to start"),
                    "the refusal must name the interruption, not the generic determination failure: "
                            + refusal.getMessage());
            assertTrue(Thread.currentThread().isInterrupted(),
                    "the interrupt flag must be restored; swallowing it hides the shutdown signal"
                            + " from the caller");
        } finally {
            // Clear the flag so it cannot leak into whatever test the runner schedules next.
            Thread.interrupted();
        }
    }
}
