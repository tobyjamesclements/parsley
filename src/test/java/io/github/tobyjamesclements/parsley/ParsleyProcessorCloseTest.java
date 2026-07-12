package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests {@link ParsleyProcessor#close()} on a task whose {@link ParsleyProcessor#init} did not finish —
 * the clean-shutdown case where the unbounded topology-epoch join wait is interrupted mid-join (see
 * {@link ParsleyCoordination#awaitJoinCommit}). Kafka Streams still calls {@code close()} on a task whose
 * {@code init()} threw, so {@code close()} must tear down only what {@code init()} actually set up rather
 * than dereference the still-null wiring and mask the real failure with a spurious NPE.
 */
class ParsleyProcessorCloseTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final ParsleyTopicAdmin ADMIN = TestTopicAdmin.of(Map.of("t1", T1_ID));

    /**
     * When {@code init()} is interrupted while blocked in the topology-epoch join wait — before it wires
     * the metrics or initialises the user delegate — a subsequent {@code close()} must run cleanly: it
     * must not NPE on the null {@code wiredMetrics}, and it must not call {@code close()} on a delegate it
     * never initialised. Both would bury the genuine {@link IllegalStateException} the interrupt raised.
     *
     * Drives {@code init()} on a background thread against a coordination runtime that never bootstraps
     * (so the join wait blocks), interrupts it, then asserts {@code init()} failed with the interrupt's
     * exception, {@code close()} throws nothing, and the un-initialised delegate is neither initialised
     * nor closed.
     */
    @Test
    void closeAfterAnInterruptedInitRunsCleanAndDoesNotTouchTheUninitialisedDelegate() throws InterruptedException {
        TestKeyValueStore<String, byte[]> frontierStore =
                new TestKeyValueStore<String, byte[]>(Comparator.naturalOrder(), "frontier");
        TestKeyValueStore<Long, byte[]> bufferStore =
                new TestKeyValueStore<Long, byte[]>(Comparator.naturalOrder(), "buffer");
        TestKeyValueStore<byte[], byte[]> candidateIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "candidate-index");
        TestKeyValueStore<byte[], byte[]> forwardedIndexStore =
                new TestKeyValueStore<byte[], byte[]>(Arrays::compareUnsigned, "forwarded-index");

        AtomicBoolean delegateInitialised = new AtomicBoolean(false);
        AtomicBoolean delegateClosed = new AtomicBoolean(false);
        Processor<String, String, String, String> delegate = new Processor<>() {
            @Override public void init(ProcessorContext<String, String> context) { delegateInitialised.set(true); }
            @Override public void process(Record<String, String> record) {}
            @Override public void close() { delegateClosed.set(true); }
        };
        ParsleySerializer<String, String> serializer =
                new ParsleySerializer<>(new ParsleyResolver<>(t -> Serdes.String(), t -> Serdes.String()));

        // A coordination runtime that is never started (and never folds the log), so it never reports
        // bootstrapped — awaitJoinCommit blocks in its awaitBootstrap loop until the thread is interrupted.
        ParsleyEpochRuntime runtime =
                new ParsleyEpochRuntime(new InMemoryEpochTransport(new InMemoryEpochTransport.SharedLog()));
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("t1"), Set.of(), Set.of(), List.of(),
                configs -> ADMIN, ParsleyConfig.from(new Properties()), null,
                ParsleyEpochSnapshotPublisher.NOOP, ParsleyCoordination.forRuntime(runtime));

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);
        context.addStateStore(new ParsleyCommittedCompleteness("frontier-commit-hook"));

        AtomicReference<Throwable> initError = new AtomicReference<>();
        Thread initThread = new Thread(() -> {
            try {
                processor.init(context);
            } catch (Throwable t) {
                initError.set(t);
            }
        }, "test-init");
        initThread.start();

        // Wait until init() is parked in the coordination poll sleep (blocked in awaitBootstrap), then
        // interrupt it — the clean-shutdown signal that unwinds the join wait.
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline && initThread.getState() != Thread.State.TIMED_WAITING) {
            Thread.sleep(5L);
        }
        initThread.interrupt();
        initThread.join(5_000L);

        assertFalse(initThread.isAlive(), "the interrupted init must unwind, not hang");
        assertNotNull(initError.get(), "the interrupted join wait must fail init rather than return");
        assertInstanceOf(IllegalStateException.class, initError.get(),
                "an interrupted join wait surfaces as an IllegalStateException (see ParsleyCoordination)");
        assertFalse(delegateInitialised.get(),
                "init was interrupted before it reached delegate.init(), so the delegate is un-initialised");

        // The state that makes B5 a bug: close() must tear down only what init() set up.
        assertDoesNotThrow(processor::close,
                "close() after a failed init must not NPE on the never-wired metrics — that would mask "
                        + "the real init failure");
        assertFalse(delegateClosed.get(),
                "close() must not close a delegate that was never initialised");
    }
}
