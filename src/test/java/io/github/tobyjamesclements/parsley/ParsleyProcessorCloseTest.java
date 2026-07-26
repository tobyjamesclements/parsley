package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests {@link ParsleyProcessor#close()} on a task whose {@link ParsleyProcessor#init} did not finish.
 * Kafka Streams still calls {@code close()} on a task whose {@code init()} threw, so {@code close()}
 * must tear down only what {@code init()} actually set up rather than dereference the still-null
 * wiring and mask the real failure with a spurious NPE.
 */
class ParsleyProcessorCloseTest {

    /**
     * When {@code init()} fails early — here, topic-UUID resolution throws before init wires the
     * metrics or initialises the user delegate — a subsequent {@code close()} must run cleanly: it
     * must not NPE on the null {@code wiredMetrics}, and it must not call {@code close()} on a
     * delegate it never initialised. Both would bury the genuine failure {@code init()} raised.
     *
     * Runs {@code init()} against a topic admin that always throws, asserts {@code init()} failed
     * with the resolution error, {@code close()} throws nothing, and the un-initialised delegate is
     * neither initialised nor closed.
     */
    @Test
    void closeAfterAFailedInitRunsCleanAndDoesNotTouchTheUninitialisedDelegate() {
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

        // A topic admin that always fails, so init() throws inside resolveTopicUuids — before the
        // metrics are wired and before delegate.init().
        ParsleyProcessor<String, String, String, String> processor = new ParsleyProcessor<>(
                delegate, serializer,
                "frontier", "buffer", "candidate-index", "forwarded-index",
                Set.of("c1"), Set.of(), List.of(),
                configs -> { throw new IllegalStateException("broker unreachable (test)"); },
                null);

        MockProcessorContext<String, String> context = new MockProcessorContext<>();
        context.setCurrentSystemTimeMs(1L);
        context.addStateStore(frontierStore);
        context.addStateStore(bufferStore);
        context.addStateStore(candidateIndexStore);
        context.addStateStore(forwardedIndexStore);

        AtomicReference<Throwable> initError = new AtomicReference<>();
        try {
            processor.init(context);
        } catch (Throwable t) {
            initError.set(t);
        }

        assertNotNull(initError.get(), "the failed topic resolution must fail init rather than return");
        assertInstanceOf(IllegalStateException.class, initError.get(),
                "a failed topic resolution surfaces as an IllegalStateException");
        assertFalse(delegateInitialised.get(),
                "init failed before it reached delegate.init(), so the delegate is un-initialised");

        // The state that makes this a hazard: close() must tear down only what init() set up.
        assertDoesNotThrow(processor::close,
                "close() after a failed init must not NPE on the never-wired metrics — that would mask "
                        + "the real init failure");
        assertFalse(delegateClosed.get(),
                "close() must not close a delegate that was never initialised");
    }
}
