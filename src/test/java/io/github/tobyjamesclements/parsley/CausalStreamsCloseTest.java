package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Tests for {@link CausalStreams#awaitDrain}: the graceful-shutdown drain wait is unbounded only
 * while draining can actually progress. An instance whose streams died in {@code ERROR} (every task
 * closed and unregistered, so {@link ParsleyQuiesce#isSafeToClose} — which requires at least one
 * registered, drained task — can never become true) must not hang {@code close()} forever: held
 * records are changelog-backed and survive to the next start regardless.
 */
class CausalStreamsCloseTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(5);

    /**
     * A dead streams instance (ERROR, all tasks closed and unregistered) ends the drain wait instead
     * of hanging forever on a quiesce that can never report safe-to-close.
     */
    @Test
    void drainWaitEndsWhenTheStreamsInstanceIsDead() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        // No task registered — exactly the state after every task of a dead instance unregistered on
        // close; isSafeToClose() is permanently false.

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.ERROR),
                "awaitDrain must return promptly when the streams instance is in ERROR — no task can "
                        + "ever drain again, so waiting on isSafeToClose() would hang forever");
    }

    /**
     * A healthy, drained instance ends the wait through the ordinary safe-to-close path — the dead-
     * instance escape never fires while the instance is RUNNING.
     */
    @Test
    void drainWaitEndsThroughSafeToCloseOnAHealthyInstance() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");
        quiesce.setDrained("0_0", true);

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, () -> KafkaStreams.State.RUNNING),
                "awaitDrain must return once every registered task reports drained");
    }

    /**
     * A running instance whose task drains while the wait is in progress ends the wait via
     * safe-to-close: the wait genuinely polls rather than deciding once up front.
     */
    @Test
    void drainWaitPollsUntilARunningInstanceDrains() {
        ParsleyQuiesce quiesce = new ParsleyQuiesce();
        quiesce.requestQuiesce();
        quiesce.register("0_0");

        AtomicReference<KafkaStreams.State> state = new AtomicReference<>(KafkaStreams.State.RUNNING);
        Thread drainer = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            quiesce.setDrained("0_0", true);
        });
        drainer.start();

        assertTimeoutPreemptively(TEST_TIMEOUT,
                () -> CausalStreams.awaitDrain(quiesce, state::get),
                "awaitDrain must keep polling a RUNNING instance and return once its task drains");
    }
}
