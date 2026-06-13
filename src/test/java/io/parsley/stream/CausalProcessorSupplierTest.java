package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import io.parsley.internal.Attributes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CausalProcessorSupplierTest {

    @Test
    void twoArgFactoryRejectsDeadLetterPolicy() {
        BufferingPolicy.DeadLetter deadLetter =
                (BufferingPolicy.DeadLetter) BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dlq");
        assertThrows(IllegalArgumentException.class,
                () -> CausalProcessorSupplier.create(deadLetter, CausalViolationHandler.noop()));
    }

    @Test
    void getReturnsAProcessorAndRegistersFrontierStore() {
        CausalProcessorSupplier<String, String> supplier = CausalProcessorSupplier.create(
                BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(1))),
                CausalViolationHandler.noop());

        assertNotNull(supplier.get());

        Set<StoreBuilder<?>> stores = supplier.stores();
        assertEquals(1, stores.size());
        assertEquals(Attributes.FRONTIER_STORE, stores.iterator().next().name());
    }

    @Test
    void deadLetterFactoryBuildsSupplier() {
        BufferingPolicy.DeadLetter deadLetter =
                (BufferingPolicy.DeadLetter) BufferingPolicy.deadLetter(BufferLimit.ofSize(1), "dlq");
        CausalProcessorSupplier<String, String> supplier =
                CausalProcessorSupplier.create(deadLetter, CausalViolationHandler.noop(), cr -> {});
        assertNotNull(supplier.get());
    }
}
