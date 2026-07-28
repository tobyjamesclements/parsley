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
