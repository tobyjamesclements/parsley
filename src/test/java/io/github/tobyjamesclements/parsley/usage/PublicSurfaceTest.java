package io.github.tobyjamesclements.parsley.usage;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import io.github.tobyjamesclements.parsley.Channel;
import io.github.tobyjamesclements.parsley.ChannelId;
import io.github.tobyjamesclements.parsley.FailClosedException;
import io.github.tobyjamesclements.parsley.OrderingStateInspector;
import io.github.tobyjamesclements.parsley.Parsley;
import io.github.tobyjamesclements.parsley.ParsleyConfig;
import io.github.tobyjamesclements.parsley.Process;
import io.github.tobyjamesclements.parsley.ProcessStatus;
import io.github.tobyjamesclements.parsley.Store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reaches the public members that no other test outside the library's package reaches.
 *
 * <p>Every test but these lives in the library's own package, where package-private is
 * indistinguishable from public, so a member that quietly lost {@code public} would compile
 * and pass everywhere else. This file and {@link HandlerSeamTest} are the compile-time proof,
 * and between them they name every public type. There is no module declaration and no
 * javadoc gate to catch it otherwise.
 *
 * <p>{@link ApiValidationTest} covers the factories and their refusals. This covers what is
 * left: the accessors, the operator diagnosis pair, and the lifecycle members whose bodies
 * need a broker, which are proved reachable by method reference rather than by call.
 */
class PublicSurfaceTest {

    private static final Channel<String, String> ORDERS =
            Channel.of("orders", Serdes.String(), Serdes.String());
    private static final Store<String, String> INVENTORY =
            Store.of("inventory", Serdes.String(), Serdes.String());

    /** A channel reports the topic, serdes and starting position it was declared with. */
    @Test
    void aChannelReportsItsDeclaration() {
        assertEquals("orders", ORDERS.topic(), "the topic is the one declared");
        assertNotNull(ORDERS.keySerde(), "the key serde is the one declared");
        assertNotNull(ORDERS.valueSerde(), "the value serde is the one declared");
        assertEquals(Channel.InitialPosition.EARLIEST, ORDERS.initialPosition(),
                "a channel starts at the earliest retained message unless told otherwise");

        Channel<String, String> latest = ORDERS.startingAt(Channel.InitialPosition.LATEST);
        assertEquals(Channel.InitialPosition.LATEST, latest.initialPosition(), "startingAt sets the position");
        assertEquals(Channel.InitialPosition.EARLIEST, ORDERS.initialPosition(),
                "and leaves the channel it was called on unchanged");
    }

    /** A store reports the name and serdes it was declared with. */
    @Test
    void aStoreReportsItsDeclaration() {
        assertEquals("inventory", INVENTORY.name(), "the name is the one declared");
        assertNotNull(INVENTORY.keySerde(), "the key serde is the one declared");
        assertNotNull(INVENTORY.valueSerde(), "the value serde is the one declared");
    }

    /** A process reports the channels and stores it declared, and looks each up by name. */
    @Test
    void aProcessReportsWhatItDeclared() {
        Channel<String, String> shipments = Channel.of("shipments", Serdes.String(), Serdes.String());
        Process shipper = Process.named("shipper")
                .receives(ORDERS, (delivery, state) -> null)
                .sends(shipments)
                .stores(INVENTORY)
                .build();

        assertEquals("shipper", shipper.name(), "the process name is the one declared");
        assertEquals(1, shipper.inputs().size(), "one received channel was declared");
        assertSame(ORDERS, shipper.input("orders").channel(), "and is found by its topic");
        assertEquals(1, shipper.outputs().size(), "one send channel was declared");
        assertSame(shipments, shipper.output("shipments"), "and is found by its topic");
        assertEquals(1, shipper.stores().size(), "one store was declared");
        assertSame(INVENTORY, shipper.store("inventory"), "and is found by its name");
        assertNull(shipper.input("absent"), "an undeclared topic is not found");
    }

    /** A configuration reports the connection, identity and tuning it was built with. */
    @Test
    void aConfigurationReportsWhatItWasBuiltWith() {
        ParsleyConfig config = ParsleyConfig.builder("localhost:9092", "app")
                .stateDir("/tmp/parsley-state")
                .metadataBudgetBytes(4096)
                .streamsProperty("num.stream.threads", 2)
                .build();

        assertEquals("localhost:9092", config.bootstrapServers(), "the bootstrap servers are the ones given");
        assertEquals("app", config.applicationIdPrefix(), "the application id prefix is the one given");
        assertEquals("/tmp/parsley-state", config.stateDir(), "the state directory is the one given");
        assertEquals(4096, config.metadataBudgetBytes(), "the metadata budget is the one given");
        assertEquals(2, config.streamsProperties().get("num.stream.threads"),
                "a property parsley does not own is carried through");
    }

    /**
     * A channel identity round-trips through its wire encoding, and orders by identity
     * then partition.
     */
    @Test
    void aChannelIdentityRoundTripsAndOrders() {
        UUID topic = new UUID(1L, 2L);
        ChannelId channel = new ChannelId(topic, 3);

        assertEquals(topic, channel.topicId(), "the topic identity is the one given");
        assertEquals(3, channel.partition(), "the partition is the one given");

        byte[] encoded = channel.toBytes();
        assertEquals(ChannelId.ENCODED_LENGTH, encoded.length, "an encoded channel is a fixed width");
        assertEquals(channel, ChannelId.readFrom(ByteBuffer.wrap(encoded)), "and decodes back to itself");

        ByteBuffer buffer = ByteBuffer.allocate(ChannelId.ENCODED_LENGTH);
        channel.writeTo(buffer);
        assertEquals(channel, ChannelId.readFrom(buffer.flip()), "writeTo and readFrom agree");

        assertTrue(channel.compareTo(new ChannelId(topic, 4)) < 0, "a lower partition sorts first");
    }

    /** The inspector answers over ordering state, and reads nothing from an empty changelog. */
    @Test
    void theInspectorReadsOrderingStateWithoutAnEngine() {
        Map<byte[], byte[]> empty = Map.of();

        assertEquals(Set.of(), OrderingStateInspector.heldChannels(empty), "nothing is held in empty state");
        assertEquals(Map.of(), OrderingStateInspector.coveredPositions(empty), "nothing is covered");
        assertEquals(Map.of(), OrderingStateInspector.nameBindings(empty), "no name is bound");
        assertEquals(List.of(), OrderingStateInspector.identityChangedTopics(empty, Map.of("orders", UUID.randomUUID())),
                "an unbound name cannot have changed identity");
        assertTrue(!OrderingStateInspector.isHeldKey(new byte[0]), "an empty key is not a held message's");
        assertTrue(OrderingStateInspector.isFedToEnd(Long.MAX_VALUE), "the sentinel reads as fed to the end");
        assertTrue(!OrderingStateInspector.isFedToEnd(7L), "an ordinary position does not");
    }

    /** A refusal names its reason, and is found inside a chain the host wrapped it in. */
    @Test
    void aRefusalIsFoundInsideAWrappedFailure() {
        FailClosedException refusal = new FailClosedException(
                FailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, "orders was recreated");

        assertEquals(FailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, refusal.reason(), "it names the condition");
        assertTrue(refusal.getMessage().contains("CHANNEL_IDENTITY_CHANGED"), "and the message is prefixed by it");
        assertSame(refusal, FailClosedException.findIn(new RuntimeException("host wrapper", refusal)),
                "and it is found through a wrapping failure, which is how the host delivers it");
        assertNull(FailClosedException.findIn(new RuntimeException("unrelated")),
                "a failure carrying no refusal yields none");
    }

    /** The namespace parsley reserves is public, because every declared name must avoid it. */
    @Test
    void theReservedNamespaceIsReadable() {
        assertEquals("__parsley.", Parsley.RESERVED_PREFIX, "the reserved prefix is part of the naming contract");
    }

    /**
     * The lifecycle members are reachable from outside the package. Their bodies need a
     * broker, so this binds each as an unbound method reference: the assertions are that the
     * references resolve, which is a compile-time fact, not a runtime one.
     */
    @Test
    void theLifecycleMembersAreReachable() {
        Function<Parsley, Boolean> healthy = Parsley::healthy;
        Function<Parsley, Map<String, ProcessStatus>> status = Parsley::status;
        Consumes<Parsley> close = Parsley::close;
        Consumes<Parsley> awaitStopped = Parsley::awaitStopped;

        assertNotNull(healthy, "healthy is reachable");
        assertNotNull(status, "status is reachable");
        assertNotNull(close, "close is reachable");
        assertNotNull(awaitStopped, "awaitStopped is reachable");
    }

    /** A consumer whose method may throw, so a checked-throwing member can be referenced. */
    @FunctionalInterface
    private interface Consumes<T> {
        void accept(T value) throws Exception;
    }
}
