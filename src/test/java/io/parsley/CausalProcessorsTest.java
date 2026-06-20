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
                builderWith(CausalBufferLimit.ofSize(1));
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when serdes have not been configured");
    }

    /**
     * A fully configured builder with a buffer limit and serdes produces a non-null
     * {@code CausalProcessorSupplier} whose {@code get()} method returns a non-null
     * processor instance.
     *
     * Asserts that both the supplier and the processor it creates are non-null.
     */
    @Test
    void buildsAValidSupplier() {
        CausalProcessorSupplier<String, String, String, String> supplier =
                builderWith(CausalBufferLimit.ofSize(1))
                        .serdes(Serdes.String(), Serdes.String())
                        .build();
        assertNotNull(supplier, "build() must return a non-null supplier");
        assertNotNull(supplier.get(), "supplier.get() must return a non-null processor");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static CausalProcessors.Builder<String, String, String, String> builderWith(CausalBufferLimit limit) {
        return CausalProcessors.builder(USER, limit);
    }
}
