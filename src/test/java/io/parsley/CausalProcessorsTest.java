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

    private static CausalProcessors.Builder<String, String, String, String> builder(CausalBufferingPolicy policy) {
        return CausalProcessors.builder(USER, policy);
    }

    @Test
    void buildRequiresSerdes() {
        CausalProcessors.Builder<String, String, String, String> b =
                builder(CausalBufferingPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)));
        assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    void deadLetterPolicyRequiresASink() {
        CausalProcessors.Builder<String, String, String, String> b =
                builder(CausalBufferingPolicy.deadLetter(CausalBufferLimit.ofSize(1), "dlq"))
                        .serdes(Serdes.String(), Serdes.String());
        assertThrows(IllegalArgumentException.class, b::build);
    }

    @Test
    void sinkIsIllegalWithoutADeadLetterPolicy() {
        CausalProcessors.Builder<String, String, String, String> b =
                builder(CausalBufferingPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String())
                        .deadLetterSink(cr -> {});
        assertThrows(IllegalArgumentException.class, b::build);
    }

    @Test
    void buildsAValidSupplier() {
        CausalProcessorSupplier<String, String, String, String> supplier =
                builder(CausalBufferingPolicy.forwardUnsafe(CausalBufferLimit.ofSize(1)))
                        .serdes(Serdes.String(), Serdes.String())
                        .build();
        assertNotNull(supplier);
        assertNotNull(supplier.get());
    }
}
