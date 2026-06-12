package io.parsley.kafka;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class CausalConsumerCreateTest {

    @Test
    void createRejectsDeadLetterPolicy() {
        // The facade has no dead-letter sink parameter; the policy must be rejected
        // before any Kafka connection is attempted.
        BufferingPolicy deadLetter = BufferingPolicy.deadLetter(
                BufferLimit.ofDuration(Duration.ofSeconds(30)), "dead");

        assertThrows(IllegalArgumentException.class, () ->
                CausalConsumer.create(List.of("topic"), deadLetter, Map.of(), Map.of()));
    }

    @Test
    void createWithIgnorePolicyReturnsRunningConsumer(@TempDir Path stateDir) {
        // Kafka clients do not connect eagerly; an unreachable bootstrap suffices.
        // application.id arrives via consumerConfig: StreamsConfig construction fails
        // without it, so BOTH config maps are proven to be merged.
        CausalConsumer<String, String> consumer = CausalConsumer.create(
                List.of("topic"),
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30))),
                Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "create-test-" + UUID.randomUUID()),
                Map.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass(),
                        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass(),
                        StreamsConfig.STATE_DIR_CONFIG, stateDir.toString()));
        assertNotNull(consumer);
        consumer.close();
    }
}
