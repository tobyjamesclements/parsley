package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The production assembly path: {@link Parsley#streams} builds a {@link CausalStreams} whose
 * underlying runtime is constructed but never started, so no broker is contacted. The stage
 * declares no sinks, because sink partition counts are the one thing assembly resolves
 * against the cluster.
 */
class ParsleyStreamsTest {

    private static final Topic<String, String> T1 = Topic.of("t1", Codec.utf8(), Codec.utf8());

    @TempDir
    Path stateDir;

    private Parsley parsley() {
        return Parsley.of(Stage.named("edge").on(T1, m -> List.of()).build());
    }

    private Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parsley-assembly");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9099");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /**
     * Live admin-client threads. A {@code KafkaAdminClient} starts one in its constructor and
     * joins it in {@code close()}, so this counts open admin clients — the only handle a test
     * has on a client the runtime deliberately does not expose. An assembled runtime accounts
     * for two: the one Parsley opens to resolve topic identity and owns, and the one Kafka
     * Streams builds for itself through the client supplier.
     */
    private static long adminClientThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(t -> t.getName().startsWith("kafka-admin-client-thread"))
                .count();
    }

    /** Assembly yields an unstarted runtime in CREATED state with live client metrics. */
    @Test
    void assemblesUnstartedRuntime() {
        try (CausalStreams streams = parsley().streams(props())) {
            assertNotNull(streams, "assembly must yield a runtime handle");
            assertEquals(KafkaStreams.State.CREATED, streams.state(),
                    "the runtime must be constructed but not started");
            assertFalse(streams.metrics().isEmpty(),
                    "client metrics must be live before start");
        }
    }

    /** The EOS requirement: explicit exactly_once_v2 is accepted, anything else rejected. */
    @Test
    void rejectsConflictingProcessingGuarantee() {
        Properties conflicting = props();
        conflicting.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        assertThrows(IllegalArgumentException.class, () -> parsley().streams(conflicting),
                "a weaker guarantee must be rejected, not silently upgraded");

        Properties explicit = props();
        explicit.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        try (CausalStreams streams = parsley().streams(explicit)) {
            assertEquals(KafkaStreams.State.CREATED, streams.state(),
                    "an explicit exactly_once_v2 must be accepted");
        }
    }

    /** Close stops the runtime; the bounded form reports completion. */
    @Test
    void closeStopsTheRuntime() {
        CausalStreams streams = parsley().streams(props());
        assertTrue(streams.close(Duration.ofSeconds(30)),
                "closing an unstarted runtime must complete within the bound");
        assertEquals(KafkaStreams.State.NOT_RUNNING, streams.state(),
                "a closed runtime must report NOT_RUNNING");
    }

    /**
     * The runtime owns the admin client assembly opened for it, and closing the runtime closes
     * it. The surface is an allowlist with no accessor for it, so nothing else ever can: left
     * open it holds a broker connection and a thread for the life of the JVM, and an
     * application that creates a runtime per test or per tenant leaks one of each every time.
     */
    @Test
    void closingTheRuntimeClosesTheAdminClientItOwns() {
        long before = adminClientThreads();
        CausalStreams streams = parsley().streams(props());
        assertTrue(adminClientThreads() > before,
                "assembly must open the admin client the runtime will own");

        streams.close();

        assertEquals(before, adminClientThreads(),
                "closing the runtime must close it too; nothing else holds a reference");
    }

    /** The bounded close carries the same duty, whichever way its timeout goes. */
    @Test
    void theBoundedCloseAlsoClosesTheAdminClient() {
        long before = adminClientThreads();
        CausalStreams streams = parsley().streams(props());

        assertTrue(streams.close(Duration.ofSeconds(30)),
                "closing an unstarted runtime must complete within the bound");

        assertEquals(before, adminClientThreads(),
                "the admin client must be closed even on the bounded path, which can return"
                        + " before the runtime has fully stopped");
    }

    /**
     * An assembly that fails after opening its admin client closes it on the way out. The
     * caller never receives a handle, so there is nobody left who could.
     */
    @Test
    void aFailedAssemblyClosesTheAdminClientItOpened() {
        Properties missingApplicationId = props();
        missingApplicationId.remove(StreamsConfig.APPLICATION_ID_CONFIG);
        long before = adminClientThreads();

        assertThrows(RuntimeException.class, () -> parsley().streams(missingApplicationId),
                "an application id is required, and the failure must surface to the caller");

        assertEquals(before, adminClientThreads(),
                "the admin client opened before the failure must not be left behind");
    }

    /** Listener registration is delegated for real: after close it must fail like Streams. */
    @Test
    void listenerRegistrationReachesTheRuntime() {
        CausalStreams streams = parsley().streams(props());
        streams.setStateListener((newState, oldState) -> {});
        streams.setUncaughtExceptionHandler(e ->
                org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        streams.setGlobalStateRestoreListener(null);
        streams.close();

        assertThrows(IllegalStateException.class,
                () -> streams.setStateListener((newState, oldState) -> {}),
                "a state listener must be rejected once the runtime has left CREATED");
        assertThrows(IllegalStateException.class,
                () -> streams.setUncaughtExceptionHandler(e ->
                        org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                                .StreamThreadExceptionResponse.SHUTDOWN_CLIENT),
                "an exception handler must be rejected once the runtime has left CREATED");
        assertThrows(IllegalStateException.class,
                () -> streams.setGlobalStateRestoreListener(null),
                "a restore listener must be rejected once the runtime has left CREATED");
    }

    /** A handler that yields a decision keeps that decision, whatever it is. */
    @Test
    void totalDecisionKeepsTheHandlersDecision() {
        var failure = new RuntimeException("processing failure");
        for (var response : org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                .StreamThreadExceptionResponse.values()) {
            assertEquals(response, CausalStreams.totalDecision(e -> response, failure),
                    "a well-behaved handler's decision must pass through untouched");
        }
    }

    /**
     * A handler that throws resolves to SHUTDOWN_CLIENT. Kafka Streams invokes the handler
     * uncaught on the failing thread, so a propagated handler failure would discard the
     * decision, kill the thread without replacement, and leave the client RUNNING with no
     * threads. The totalized form converts that silent wedge into a loud shutdown.
     */
    @Test
    void totalDecisionShutsDownWhenTheHandlerThrows() {
        assertEquals(
                org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_CLIENT,
                CausalStreams.totalDecision(
                        e -> { throw new IllegalStateException("handler failure"); },
                        new RuntimeException("processing failure")),
                "a throwing handler must resolve to SHUTDOWN_CLIENT, not propagate");
        assertEquals(
                org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_CLIENT,
                CausalStreams.totalDecision(
                        e -> { throw new AssertionError("handler error"); },
                        new RuntimeException("processing failure")),
                "an Error from the handler must also resolve to SHUTDOWN_CLIENT");
    }

    /** A handler that returns null resolves to SHUTDOWN_CLIENT instead of failing the switch. */
    @Test
    void totalDecisionShutsDownWhenTheHandlerReturnsNull() {
        assertEquals(
                org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_CLIENT,
                CausalStreams.totalDecision(e -> null, new RuntimeException("processing failure")),
                "a null decision must resolve to SHUTDOWN_CLIENT");
    }

    /** The thread and lag views delegate to the runtime rather than answering empty. */
    @Test
    void runtimeViewsAnswerBeforeStart() {
        try (CausalStreams streams = parsley().streams(props())) {
            assertFalse(streams.metadataForLocalThreads().isEmpty(),
                    "the constructed runtime's stream threads must be visible");
            assertTrue(streams.allLocalStorePartitionLags().isEmpty(),
                    "no store partitions are assigned before start");
        }
    }
}
