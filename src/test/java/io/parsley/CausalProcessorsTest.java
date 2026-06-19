package io.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalProcessorsTest {

    private static final ProcessorSupplier<String, String, String, String> USER =
            () -> new Processor<>() {
                @Override
                public void process(Record<String, String> record) {}
            };

    /**
     * {@code CausalProcessors.Builder.build()} requires key and value serdes to be set
     * before the processor supplier can be constructed.
     *
     * Asserts that {@code IllegalStateException} is thrown when {@code serdes()} has not
     * been called.
     */
    @Test
    void buildRequiresSerdes() {
        CausalProcessors.Builder<String, String, String, String> b =
                builderWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)));
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when serdes have not been configured");
    }

    /**
     * When the policy uses {@code DEAD_LETTER} for all violation types, {@code build()}
     * requires a dead-letter sink to have been configured.
     *
     * Asserts that {@code IllegalArgumentException} is thrown when no sink is present.
     */
    @Test
    void buildRejectsDeadLetterPolicyWithoutSink() {
        CausalProcessors.Builder<String, String, String, String> b =
                builderWith(CausalBufferPolicy.deadLetter(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String());
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
        CausalProcessors.Builder<String, String, String, String> b =
                builderWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String())
                        .deadLetterSink(cr -> {});
        assertThrows(IllegalArgumentException.class, b::build,
                "build() must throw when a sink is configured but the policy does not use dead-letter routing");
    }

    /**
     * When the policy configures {@code DEAD_LETTER} for at least one violation type,
     * {@code build()} requires a dead-letter sink even if the other types use different
     * actions.
     *
     * Asserts that {@code IllegalArgumentException} is thrown for a mixed policy with
     * {@code DEAD_LETTER} on MISSING_HEADER and no sink configured.
     */
    @Test
    void buildRejectsAnyDeadLetterActionWithoutSink() {
        CausalProcessors.Builder<String, String, String, String> b =
                builderWith(CausalBufferPolicy.builder()
                        .onMissing(CausalViolationAction.DEAD_LETTER)
                        .onUnresolvable(CausalViolationAction.DROP)
                        .onLimit(CausalViolationAction.DROP)
                        .setLimit(CausalBufferLimit.ofSize(1))
                        .build())
                        .serdes(Serdes.String(), Serdes.String());
        assertThrows(IllegalArgumentException.class, b::build,
                "build() must throw when any action is DEAD_LETTER and no sink is configured");
    }

    /**
     * A fully configured builder with compatible policy and serdes produces a non-null
     * {@code CausalProcessorSupplier} whose {@code get()} method returns a non-null
     * processor instance.
     *
     * Asserts that both the supplier and the processor it creates are non-null.
     */
    @Test
    void buildsAValidSupplier() {
        CausalProcessorSupplier<String, String, String, String> supplier =
                builderWith(CausalBufferPolicy.drop(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String())
                        .build();
        assertNotNull(supplier, "build() must return a non-null supplier");
        assertNotNull(supplier.get(), "supplier.get() must return a non-null processor");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static CausalProcessors.Builder<String, String, String, String> builderWith(CausalBufferPolicy policy) {
        return CausalProcessors.builder(USER, policy);
    }
}
