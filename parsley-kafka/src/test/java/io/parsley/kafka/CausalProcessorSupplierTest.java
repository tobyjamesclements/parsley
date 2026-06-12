package io.parsley.kafka;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CausalProcessorSupplierTest {

    private static final KafkaVectorClockSerialiser SERIALISER = new KafkaVectorClockSerialiser();
    private static final BufferingPolicy IGNORE =
            BufferingPolicy.ignore(BufferLimit.ofDuration(Duration.ofSeconds(30)));

    @Test
    void threeArgConstructorRejectsDeadLetterPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new CausalProcessorSupplier<>(
                BufferingPolicy.deadLetter(BufferLimit.ofDuration(Duration.ofSeconds(30)), "dead"),
                CausalViolationHandler.noop(), SERIALISER));
    }

    @Test
    void unsupportedLimitsAreRejectedAtConstruction() {
        assertThrows(UnsupportedOperationException.class, () -> new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofBytes(1024)),
                CausalViolationHandler.noop(), SERIALISER));
        assertThrows(UnsupportedOperationException.class, () -> new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.ofFrontierAdvancement(10)),
                CausalViolationHandler.noop(), SERIALISER));
        // Unsupported limits nested in a FirstLimit are also rejected.
        assertThrows(UnsupportedOperationException.class, () -> new CausalProcessorSupplier<>(
                BufferingPolicy.ignore(BufferLimit.first(
                        BufferLimit.ofDuration(Duration.ofSeconds(30)), BufferLimit.ofBytes(1024))),
                CausalViolationHandler.noop(), SERIALISER));
        // The 4-argument DeadLetter constructor validates too.
        assertThrows(UnsupportedOperationException.class, () -> new CausalProcessorSupplier<String, String>(
                new BufferingPolicy.DeadLetter(BufferLimit.ofBytes(1024), "dead"),
                CausalViolationHandler.noop(), SERIALISER, record -> {}));
    }

    @Test
    void getReturnsAFreshProcessorPerCall() {
        var supplier = new CausalProcessorSupplier<String, String>(
                IGNORE, CausalViolationHandler.noop(), SERIALISER);
        assertNotNull(supplier.get());
        assertNotSame(supplier.get(), supplier.get());
    }

    @Test
    void storesRegistersTheFrontierStore() {
        var supplier = new CausalProcessorSupplier<String, String>(
                IGNORE, CausalViolationHandler.noop(), SERIALISER);
        Set<StoreBuilder<?>> stores = supplier.stores();
        assertEquals(1, stores.size());
        assertEquals("parsley-frontier", stores.iterator().next().name());
    }
}
