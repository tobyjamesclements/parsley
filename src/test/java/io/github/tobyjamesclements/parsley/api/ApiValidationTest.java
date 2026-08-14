package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.util.List;

import io.github.tobyjamesclements.parsley.core.HeaderKV;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Establishes that the declaration surface refuses what would weaken the guarantee.
 *
 * <p>Reserved names, reserved headers and owned configuration keys are rejected at
 * construction, before any broker is contacted.
 */
class ApiValidationTest {
    /** Reserved store names are unconstructible. */
    @Test
    void reservedStoreNamesAreUnconstructible() {
        assertThrows(IllegalArgumentException.class,
                () -> Store.of("__parsley.anything", Serdes.String(), Serdes.String()),
                "application state may never alias ordering state (SPEC Structural 8)");
    }

    /** Reserved headers are unconstructible. */
    @Test
    void reservedHeadersAreUnconstructible() {
        Channel<String, String> channel = Channel.of("t", Serdes.String(), Serdes.String());
        io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException e =
                assertThrows(io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.class,
                        () -> Effects.builder().send(channel, "k", "v",
                                List.of(new HeaderKV("parsley.causes", new byte[0]))).build(),
                        "application headers may never impersonate causal metadata (SPEC Structural 5)");
        assertEquals(io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason.RESERVED_HEADER_USED, e.reason(),
                "the refusal names its condition (SPEC Operational 6) and fails the step through the seam");
    }

    /** Reserved topic names are refused before any broker contact. */
    @Test
    void reservedTopicNamesAreRefusedBeforeAnyBrokerContact() {
        Channel<String, String> internal = Channel.of("x-p-__parsley.ordering-changelog",
                Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("p")
                .receives(internal, (d, s) -> Effects.none())
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> io.github.tobyjamesclements.parsley.api.Parsley.start(
                        ParsleyConfig.builder("unreachable:1", "x").build(), p),
                "a declared topic inside parsley's internal namespace is refused at declaration time (D58)");
    }

    /** Guarantee bearing configuration is unoverridable. */
    @Test
    void guaranteeBearingConfigurationIsUnoverridable() {
        ParsleyConfig.Builder builder = ParsleyConfig.builder("broker:9092", "app");
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("processing.guarantee", "at_least_once"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("consumer.isolation.level", "read_uncommitted"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.auto.offset.reset", "latest"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("group.id", "other"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("processing.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.production.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.deserialization.exception.handler", "continue"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("producer.interceptor.classes", "com.example.HeaderStripper"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.interceptor.classes", "com.example.Interceptor"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.timestamp.extractor", "LogAndSkipOnInvalidTimestamp"));
    }

    /** Processes must receive something. */
    @Test
    void processesMustReceiveSomething() {
        assertThrows(IllegalArgumentException.class, () -> ProcessDefinition.named("p").build(),
                "a process with no received channels can never deliver");
    }
}
