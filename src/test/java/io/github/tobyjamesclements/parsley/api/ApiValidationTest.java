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

    /** Null serdes are refused at declaration, not at first use on the stream thread. */
    @Test
    void nullSerdesAreRefusedAtDeclaration() {
        assertThrows(IllegalArgumentException.class, () -> Channel.of("t", null, Serdes.String()),
                "a null key serde would otherwise surface as an NPE on the stream thread");
        assertThrows(IllegalArgumentException.class, () -> Channel.of("t", Serdes.String(), null),
                "a null value serde would otherwise surface as an NPE on the stream thread");
        assertThrows(IllegalArgumentException.class, () -> Store.of("s", null, Serdes.String()),
                "a null store key serde would otherwise surface at the first state access");
        assertThrows(IllegalArgumentException.class, () -> Store.of("s", Serdes.String(), null),
                "a null store value serde would otherwise surface at the first state access");
    }

    /** A null starting position is refused rather than silently meaning LATEST. */
    @Test
    void nullStartingPositionIsRefusedNotDefaulted() {
        Channel<String, String> channel = Channel.of("t", Serdes.String(), Serdes.String());
        assertThrows(IllegalArgumentException.class, () -> channel.startingAt(null),
                "null compared unequal to EARLIEST at commit time, which would silently skip"
                        + " every retained message");
    }

    /** Names that feed Kafka topic names must satisfy Kafka's topic-name rules. */
    @Test
    void namesFeedingKafkaTopicsAreValidatedAsTopicNames() {
        assertThrows(IllegalArgumentException.class,
                () -> Channel.of("has space", Serdes.String(), Serdes.String()),
                "an invalid topic name should fail at declaration, not at topic resolution");
        assertThrows(IllegalArgumentException.class,
                () -> Channel.of("a".repeat(250), Serdes.String(), Serdes.String()),
                "a topic name beyond Kafka's 249-character limit is unusable");
        assertThrows(IllegalArgumentException.class,
                () -> Store.of("has space", Serdes.String(), Serdes.String()),
                "a store name becomes its changelog topic name and must satisfy the same rules");
        assertThrows(IllegalArgumentException.class,
                () -> Store.of("..", Serdes.String(), Serdes.String()),
                "'.' and '..' would resolve the store's local directory outside its task directory");
        assertThrows(IllegalArgumentException.class,
                () -> ParsleyConfig.builder("broker:9092", "has space"),
                "the prefix becomes part of every changelog topic name and must satisfy the same"
                        + " rules, matching the validation process names already get");
    }

    /** A send topic declared through two different channel instances is refused. */
    @Test
    void sendTopicDeclaredThroughTwoInstancesIsRefused() {
        Channel<String, String> in = Channel.of("in", Serdes.String(), Serdes.String());
        Channel<String, String> declared = Channel.of("out", Serdes.String(), Serdes.String());
        Channel<String, String> lookAlike = Channel.of("out", Serdes.String(), Serdes.String());
        ProcessDefinition.Builder builder = ProcessDefinition.named("p")
                .receives(in, (d, s) -> Effects.none())
                .sends(declared);
        assertThrows(IllegalArgumentException.class, () -> builder.sends(lookAlike),
                "a silently dropped duplicate would surface as a fail-closed refusal at first"
                        + " emission through the dropped instance");
        builder.sends(declared).build();
    }

    /** Overlong changelog names are refused before any broker contact. */
    @Test
    void overlongChangelogNamesAreRefusedBeforeAnyBrokerContact() {
        Channel<String, String> in = Channel.of("t", Serdes.String(), Serdes.String());
        ProcessDefinition p = ProcessDefinition.named("p")
                .receives(in, (d, s) -> Effects.none())
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("unreachable:1", "a".repeat(240)).build(), p),
                "each component passes its own check, but the composed changelog topic name"
                        + " exceeds Kafka's 249-character limit and would fail inside Streams"
                        + " internal-topic creation");
    }

    /** Null effect targets are refused when the effect is declared. */
    @Test
    void nullEffectTargetsAreRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(null, "k", "v"),
                "a null channel would otherwise fail at commit time inside the step");
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(Channel.of("t", Serdes.String(), Serdes.String()),
                        "k", "v", null),
                "null headers would otherwise fail as a bare NPE in the copy");
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().put(null, "k", "v"),
                "a null store would otherwise fail at commit time inside the step");
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().delete(null, "k"),
                "a null store would otherwise fail at commit time inside the step");
        assertThrows(IllegalArgumentException.class,
                () -> ParsleyConfig.builder("broker:9092", "p").streamsProperty(null, "v"),
                "a null property key would otherwise NPE inside the deny-list check");
    }
}
