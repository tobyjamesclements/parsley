package io.github.tobyjamesclements.parsley.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes the wait an application blocks on (D111): {@code awaitStopped} returns when
 * a process fails, whatever the failure, or when the runtime closes, and not before. The
 * documented shape {@code try (Parsley p = Parsley.start(...)) { p.awaitStopped(); }} is
 * what this pins; without the wait the same block closed every process as soon as it had
 * started.
 */
class AwaitStoppedTest {
    /** Nothing has stopped: the bounded wait elapses and reports so. */
    @Test
    void theWaitElapsesWhileNothingHasStopped() throws Exception {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        assertFalse(runtime.awaitStopped(Duration.ofMillis(50)), "no process has stopped and the runtime is open");
    }

    /** A recorded failure ends the wait. */
    @Test
    void aProcessFailureEndsTheWait() throws Exception {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        Thread waiter = new Thread(() -> {
            try {
                runtime.awaitStopped();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        runtime.recordFailure("p", new IllegalStateException("stream thread died"));
        waiter.join(5_000);
        assertFalse(waiter.isAlive(), "the wait must end once a process has stopped");
    }

    /** Closing the runtime ends the wait. */
    @Test
    void closingTheRuntimeEndsTheWait() throws Exception {
        ParsleyRuntime runtime = new ParsleyRuntime(null);
        runtime.close();
        assertTrue(runtime.awaitStopped(Duration.ofMillis(50)), "a closed runtime has nothing left to wait for");
    }
}
