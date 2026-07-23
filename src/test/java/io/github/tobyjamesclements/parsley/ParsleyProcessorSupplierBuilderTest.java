package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParsleyProcessorSupplierBuilderTest {

    private static final ProcessorSupplier<String, String, String, String> USER =
            () -> new Processor<>() {
                @Override
                public void process(Record<String, String> record) {}
            };

    private static final String TOPOLOGY_VALIDATION = "parsley.topology.validation";

    /**
     * {@code ParsleyProcessorSupplier.Builder.build()} requires a buffer store to be declared via
     * {@code addBufferStore(name)} before the processor supplier can be constructed.
     *
     * Asserts that {@code IllegalStateException} is thrown when no buffer store was declared.
     */
    @Test
    void buildFailsWithoutBufferStore() {
        ParsleyProcessorSupplier.Builder<String, String, String, String> b = ParsleyProcessorSupplier.builder(USER)
                .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()));
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when addBufferStore(name) was not called");
    }

    /**
     * {@code ParsleyProcessorSupplier.Builder.build()} requires at least one {@link ParsleySource} to be
     * registered before the processor supplier can be constructed.
     *
     * Asserts that {@code IllegalStateException} is thrown when no buffer has been added.
     */
    @Test
    void buildRequiresABuffer() {
        ParsleyProcessorSupplier.Builder<String, String, String, String> b =
                builderWith();
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when no ParsleySource has been registered");
    }

    /**
     * A fully configured builder with a buffer store and a registered buffer produces a non-null
     * {@code ParsleyProcessorSupplier} whose {@code get()} method returns a non-null
     * processor instance.
     *
     * Asserts that both the supplier and the processor it creates are non-null.
     */
    @Test
    void buildsAValidSupplier() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                builderWith()
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .build();
        assertNotNull(supplier, "build() must return a non-null supplier");
        assertNotNull(supplier.get(), "supplier.get() must return a non-null processor");
    }

    /**
     * {@code addBufferStore(name)} sets the state-store namespace — the frontier, buffer,
     * candidate-index, and forwarded-index store names are all derived from {@code name}.
     *
     * Asserts the four derived store names appear in {@code stores()}.
     */
    @Test
    void addBufferStoreSetsNamespace() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) ParsleyProcessorSupplier.builder(USER)
                        .addBufferStore("c1")
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .build();

        Set<String> storeNames = supplier.stores().stream()
                .map(StoreBuilder::name)
                .collect(Collectors.toSet());
        assertTrue(storeNames.contains("c1-frontier"), "frontier store must be named from the namespace");
        assertTrue(storeNames.contains("c1-buffer"), "buffer store must be named from the namespace");
        assertTrue(storeNames.contains("c1-candidate-index"),
                "candidate-index store must be named from the namespace");
        assertTrue(storeNames.contains("c1-forwarded-index"),
                "forwarded-index store must be named from the namespace");
    }

    /**
     * {@code withConfig(key, value)} sets a Parsley configuration entry that is threaded into the
     * built supplier's effective {@link ParsleyConfig}.
     *
     * Asserts that setting {@code parsley.topology.validation} to {@code strict} makes the effective
     * config report STRICT, where the default is WARN.
     */
    @Test
    void withConfigKeyValueOverridesDefault() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith()
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .withConfig(TOPOLOGY_VALIDATION, "strict")
                        .build();
        assertEquals(ParsleyConfig.ValidationMode.STRICT, supplier.config().topologyValidation(),
                "withConfig(strict) must thread the value into the effective config");
    }

    /**
     * With no Parsley configuration supplied, the effective config falls back to its defaults, where
     * {@code parsley.topology.validation} is {@code strict}.
     *
     * Asserts the default effective config reports STRICT.
     */
    @Test
    void defaultConfigUsesStrictValidation() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith()
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .build();
        assertEquals(ParsleyConfig.ValidationMode.STRICT, supplier.config().topologyValidation(),
                "the default topology-validation mode is 'strict'");
    }

    /**
     * {@code ParsleyProcessorSupplier.builder(...)} rejects a {@code userSupplier} that is already a
     * {@link ParsleyProcessorSupplier} — decorating an already-decorated supplier would buffer and
     * stamp every record twice, nested, silently corrupting the frontier.
     *
     * Asserts an {@link IllegalArgumentException} naming the double-decoration.
     */
    @Test
    void builderRejectsAnAlreadyDecoratedSupplier() {
        ParsleyProcessorSupplier<String, String, String, String> alreadyDecorated =
                builderWith()
                        .addSource(new ParsleySource<>("c1", Serdes.String(), Serdes.String()))
                        .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ParsleyProcessorSupplier.builder(alreadyDecorated),
                "builder(...) must reject an already-decorated supplier");
        assertTrue(e.getMessage().contains("ParsleyProcessorSupplier"),
                "the message must name the double-decoration: " + e.getMessage());
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyProcessorSupplier.Builder<String, String, String, String> builderWith() {
        return ParsleyProcessorSupplier.builder(USER).addBufferStore("parsley");
    }
}
