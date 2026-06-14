package io.parsley.stream;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.CausalViolationHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalStreamsTest {

    private CausalProcessorSupplier<String, String> supplier() {
        return CausalProcessorSupplier.create(
                BufferingPolicy.forwardUnsafe(BufferLimit.ofDuration(Duration.ofSeconds(1))),
                CausalViolationHandler.noop());
    }

    @Test
    void processInsertsTheCausalProcessorIntoTheTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        CausalStreams.process(
                builder.stream(List.of("prices"), Consumed.with(Serdes.String(), Serdes.String())),
                supplier());
        Topology topology = builder.build();
        assertNotNull(topology.describe());
    }

    @Test
    void processWithNamedLabelsTheProcessorNode() {
        StreamsBuilder builder = new StreamsBuilder();
        CausalStreams.process(
                builder.stream(List.of("prices"), Consumed.with(Serdes.String(), Serdes.String())),
                supplier(),
                Named.as("causal-orderer"));
        assertTrue(builder.build().describe().toString().contains("causal-orderer"));
    }
}
