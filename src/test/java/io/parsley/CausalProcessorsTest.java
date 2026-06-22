package io.parsley;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalProcessorsTest {

    private static final ProcessorSupplier<String, String, String, String> USER =
            () -> new Processor<>() {
                @Override
                public void process(Record<String, String> record) {}
            };

    private static final String FAILURE_POLICY = "parsley.buffer.deserialization.failure.policy";

    /**
     * {@code CausalProcessors.Builder.build()} requires a buffer store to be declared via
     * {@code addBufferStore(name, limit)} before the processor supplier can be constructed.
     *
     * Asserts that {@code IllegalStateException} is thrown when no buffer store was declared.
     */
    @Test
    void buildFailsWithoutBufferStore() {
        CausalProcessors.Builder<String, String, String, String> b = CausalProcessors.builder(USER)
                .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()));
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when addBufferStore(name, limit) was not called");
    }

    /**
     * {@code CausalProcessors.Builder.build()} requires at least one {@link CausalBuffer} to be
     * registered before the processor supplier can be constructed.
     *
     * Asserts that {@code IllegalStateException} is thrown when no buffer has been added.
     */
    @Test
    void buildRequiresABuffer() {
        CausalProcessors.Builder<String, String, String, String> b =
                builderWith(CausalBufferLimit.ofSize(1));
        assertThrows(IllegalStateException.class, b::build,
                "build() must throw when no CausalBuffer has been registered");
    }

    /**
     * A fully configured builder with a buffer store and a registered buffer produces a non-null
     * {@code CausalProcessorSupplier} whose {@code get()} method returns a non-null
     * processor instance.
     *
     * Asserts that both the supplier and the processor it creates are non-null.
     */
    @Test
    void buildsAValidSupplier() {
        CausalProcessorSupplier<String, String, String, String> supplier =
                builderWith(CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .build();
        assertNotNull(supplier, "build() must return a non-null supplier");
        assertNotNull(supplier.get(), "supplier.get() must return a non-null processor");
    }

    /**
     * {@code addBufferStore(name, limit)} sets the state-store namespace — the frontier, buffer, and
     * position-index store names are all derived from {@code name} — and carries the eviction limit
     * through to the built supplier.
     *
     * Asserts the three derived store names appear in {@code stores()} and the supplier reports the
     * limit it was given.
     */
    @Test
    void addBufferStoreSetsNamespaceAndLimit() {
        CausalBufferLimit limit = CausalBufferLimit.ofSize(7);
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) CausalProcessors.builder(USER)
                        .addBufferStore("t1", limit)
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .build();

        Set<String> storeNames = supplier.stores().stream()
                .map(StoreBuilder::name)
                .collect(Collectors.toSet());
        assertTrue(storeNames.contains("t1-frontier"), "frontier store must be named from the namespace");
        assertTrue(storeNames.contains("t1-buffer"), "buffer store must be named from the namespace");
        assertTrue(storeNames.contains("t1-position-index"),
                "position-index store must be named from the namespace");
        assertEquals(limit, supplier.limit(), "the supplier must carry the eviction limit it was given");
    }

    /**
     * {@code withConfig(key, value)} sets a Parsley configuration entry that overrides the default
     * deserialization-failure policy ({@code fail}).
     *
     * Asserts that setting the policy to {@code continue} makes the effective config skip on decode
     * failure, where the default would not.
     */
    @Test
    void withConfigKeyValueOverridesDefault() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith(CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .withConfig(FAILURE_POLICY, "continue")
                        .build();
        assertTrue(supplier.config().skipOnDecodeFailure(),
                "withConfig(continue) must make the effective config skip on decode failure");
    }

    /**
     * With no Parsley configuration supplied, the effective config falls back to its defaults, where
     * the deserialization-failure policy is {@code fail} (do not skip).
     *
     * Asserts the default effective config does not skip on decode failure.
     */
    @Test
    void defaultConfigDoesNotSkipOnDecodeFailure() {
        ParsleyProcessorSupplier<String, String, String, String> supplier =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith(CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .build();
        assertFalse(supplier.config().skipOnDecodeFailure(),
                "the default policy is 'fail', which must not skip on decode failure");
    }

    /**
     * The {@code withConfigs(Map)} and {@code withConfig(Properties)} overloads both feed the
     * effective Parsley configuration, exactly as {@code withConfig(key, value)} does.
     *
     * Asserts the {@code continue} policy lands via both the map and the properties overload.
     */
    @Test
    void withConfigsMapAndPropertiesApplied() {
        ParsleyProcessorSupplier<String, String, String, String> fromMap =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith(CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t1", Serdes.String(), Serdes.String()))
                        .withConfigs(Map.of(FAILURE_POLICY, "continue"))
                        .build();
        assertTrue(fromMap.config().skipOnDecodeFailure(),
                "withConfigs(Map) must apply the supplied policy");

        Properties props = new Properties();
        props.setProperty(FAILURE_POLICY, "continue");
        ParsleyProcessorSupplier<String, String, String, String> fromProps =
                (ParsleyProcessorSupplier<String, String, String, String>) builderWith(CausalBufferLimit.ofSize(1))
                        .addBuffer(CausalBuffer.of("t2", Serdes.String(), Serdes.String()))
                        .withConfig(props)
                        .build();
        assertTrue(fromProps.config().skipOnDecodeFailure(),
                "withConfig(Properties) must apply the supplied policy");
    }

    // --- helpers --------------------------------------------------------------------------------

    private static CausalProcessors.Builder<String, String, String, String> builderWith(CausalBufferLimit limit) {
        return CausalProcessors.builder(USER).addBufferStore("parsley", limit);
    }
}
