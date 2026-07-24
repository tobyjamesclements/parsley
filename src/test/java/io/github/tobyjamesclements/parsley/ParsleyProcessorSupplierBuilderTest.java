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
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.cause;
import static io.github.tobyjamesclements.parsley.ParsleyTestFixtures.message;

class ParsleyProcessorSupplierBuilderTest {

    private static final ProcessorSupplier<String, String, String, String> USER =
            () -> new Processor<>() {
                @Override
                public void process(Record<String, String> record) {}
            };

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
        assertTrue(message(e).contains("ParsleyProcessorSupplier"),
                "the message must name the double-decoration: " + message(e));
    }

    // --- helpers --------------------------------------------------------------------------------

    private static ParsleyProcessorSupplier.Builder<String, String, String, String> builderWith() {
        return ParsleyProcessorSupplier.builder(USER).addBufferStore("parsley");
    }
}
