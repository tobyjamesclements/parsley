package io.parsley;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalConsumersTest {

    /**
     * When the policy uses {@code DEAD_LETTER} for any violation type, {@code build()}
     * requires a dead-letter sink to have been configured — checked eagerly, before any
     * broker/AdminClient interaction, so this is a plain unit test.
     *
     * Asserts that {@code IllegalArgumentException} is thrown when no sink is present.
     */
    @Test
    void buildRejectsDeadLetterPolicyWithoutSink() {
        CausalConsumers.Builder<String, String> b = builderWith(
                CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1)));
        assertThrows(IllegalArgumentException.class, b::build,
                "build() must throw when a dead-letter policy is used without a sink");
    }

    /**
     * A dead-letter sink cannot be configured unless the policy uses {@code DEAD_LETTER}
     * for at least one violation type.
     *
     * Asserts that {@code IllegalArgumentException} is thrown when a sink is provided but
     * the policy does not use dead-letter routing.
     */
    @Test
    void buildRejectsSinkWithoutDeadLetterPolicy() {
        CausalConsumers.Builder<String, String> b = builderWith(
                CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)))
                .deadLetterSink(cr -> {});
        assertThrows(IllegalArgumentException.class, b::build,
                "build() must throw when a sink is configured but the policy does not use dead-letter routing");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static CausalConsumers.Builder<String, String> builderWith(CausalBufferPolicy policy) {
        return CausalConsumers.builder(
                List.of("topic"),
                policy,
                Map.of(),
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "unit-test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }
}
