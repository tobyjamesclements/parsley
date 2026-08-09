package io.github.tobyjamesclements.parsley.api;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.util.List;

import io.github.tobyjamesclements.parsley.core.HeaderKV;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The public API must offer no operation whose documented use can violate a safety criterion (SPEC Structural 9):
 * reserved names are unconstructible and owned configuration is unoverridable, at build time, with attributable
 * errors.
 */
class ApiValidationTest {

    @Test
    void reservedStoreNamesAreUnconstructible() {
        assertThrows(IllegalArgumentException.class,
                () -> StoreDef.of("__parsley.anything", Serdes.String(), Serdes.String()),
                "application state may never alias ordering state (SPEC Structural 8)");
    }

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
        // Continue-style exception handlers would convert failing closed into dropping and committing
        // (SPEC Safety 3/7, Structural 19): equally unoverridable, under any prefix.
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("processing.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.production.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.deserialization.exception.handler", "continue"));
        // Client interceptors mutate records on the wire by documented design — one that strips the causes header
        // makes every emission read cause-free downstream (SPEC Structural 9 via Safety 1/3/4/7) — and a
        // log-and-skip timestamp extractor is a documented silent drop. Both owned, under any prefix (D51).
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("producer.interceptor.classes", "com.example.HeaderStripper"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.interceptor.classes", "com.example.Interceptor"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.timestamp.extractor", "LogAndSkipOnInvalidTimestamp"));
    }

    @Test
    void processesMustReceiveSomething() {
        assertThrows(IllegalArgumentException.class, () -> ProcessDefinition.named("p").build(),
                "a process with no received channels can never deliver");
    }
}
