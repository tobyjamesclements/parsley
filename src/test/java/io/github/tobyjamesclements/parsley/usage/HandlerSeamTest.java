package io.github.tobyjamesclements.parsley.usage;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.github.tobyjamesclements.parsley.Channel;
import io.github.tobyjamesclements.parsley.Delivery;
import io.github.tobyjamesclements.parsley.Effects;
import io.github.tobyjamesclements.parsley.FailClosedException;
import io.github.tobyjamesclements.parsley.Handler;
import io.github.tobyjamesclements.parsley.Header;
import io.github.tobyjamesclements.parsley.ProcessStatus;
import io.github.tobyjamesclements.parsley.State;
import io.github.tobyjamesclements.parsley.Store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives a {@link Handler} the way an application's own unit test would, from outside the
 * package the library lives in.
 *
 * <p>Every type here is reached through a public import, so this file stops compiling if a
 * member of the seam loses {@code public}. Nothing else covers that: the rest of the suite
 * sits in the library's package, where package-private is indistinguishable from public, and
 * there is no module declaration and no javadoc gate to catch it. The seam is
 * {@link Delivery} in, {@link State} beside it, {@link Effects} out.
 */
class HandlerSeamTest {

    private static final Channel<String, String> ORDERS =
            Channel.of("orders", Serdes.String(), Serdes.String());
    private static final Channel<String, String> SHIPMENTS =
            Channel.of("shipments", Serdes.String(), Serdes.String());
    private static final Store<String, String> INVENTORY =
            Store.of("inventory", Serdes.String(), Serdes.String());

    /** A handler reads state, writes state and sends, and every effect arrives in declaration order. */
    @Test
    void aHandlerReadsStateAndReturnsItsEffects() {
        Handler<String, String> shipper = (delivery, state) -> {
            String remaining = state.get(INVENTORY, delivery.key());
            return Effects.builder()
                    .put(INVENTORY, delivery.key(), remaining == null ? "0" : remaining)
                    .send(SHIPMENTS, delivery.key(), "ship " + delivery.value())
                    .build();
        };

        Effects effects = shipper.handle(delivery("sku-1", "2 units"), stateHolding(INVENTORY, "sku-1", "7"));

        assertEquals(1, effects.writes().size(), "the handler declared one state write");
        Effects.Write<?, ?> write = effects.writes().get(0);
        assertSame(INVENTORY, write.store(), "the write names the declared store instance");
        assertEquals("sku-1", write.key(), "the write is keyed by the delivered key");
        assertEquals("7", write.value(), "the write carries what the read returned");

        assertEquals(1, effects.sends().size(), "the handler declared one send");
        Effects.Send<?, ?> send = effects.sends().get(0);
        assertSame(SHIPMENTS, send.channel(), "the send names the declared channel instance");
        assertEquals("ship 2 units", send.value(), "the send carries what the handler computed");
        assertTrue(send.timestamp().isEmpty(), "a send with no timestamp of its own inherits the delivery's");
    }

    /** A handler that changes nothing returns the empty effects rather than null. */
    @Test
    void anObservingHandlerReturnsNone() {
        Handler<String, String> observer = (delivery, state) -> Effects.none();

        Effects effects = observer.handle(delivery("sku-1", "2 units"), stateHolding(INVENTORY, "other", "1"));

        assertTrue(effects.writes().isEmpty(), "none writes nothing");
        assertTrue(effects.sends().isEmpty(), "none sends nothing");
    }

    /** A key absent from the store reads as null, which is the handler's signal to initialise it. */
    @Test
    void anAbsentKeyReadsAsNull() {
        Handler<String, String> reader = (delivery, state) ->
                Effects.builder().put(INVENTORY, delivery.key(), String.valueOf(state.get(INVENTORY, "absent"))).build();

        Effects effects = reader.handle(delivery("sku-1", "2 units"), stateHolding(INVENTORY, "present", "1"));

        assertEquals("null", effects.writes().get(0).value(), "the absent key read as null");
    }

    /** Reserved transport headers never reach application logic, whatever arrived on the wire. */
    @Test
    void reservedHeadersAreInvisibleToTheHandler() {
        Delivery<String, String> delivery = Delivery.of(ORDERS, 0, 12L, 1_000L, "sku-1", "2 units",
                List.of(new Header("parsley.causes", new byte[]{1}), new Header("trace-id", new byte[]{2})));

        assertEquals(1, delivery.headers().size(), "only the application's own header survives");
        assertEquals("trace-id", delivery.headers().get(0).key(), "and it is the one the application sent");
    }

    /** A delivery carries the coordinates of the record it came from. */
    @Test
    void aDeliveryCarriesItsCoordinates() {
        Delivery<String, String> delivery = delivery("sku-1", "2 units");

        assertSame(ORDERS, delivery.channel(), "the delivery names the channel it arrived on");
        assertEquals(0, delivery.partition(), "the partition it arrived on");
        assertEquals(12L, delivery.position(), "the offset within that partition");
        assertEquals(1_000L, delivery.timestamp(), "and the record timestamp");
    }

    /** A running process reports no refusal, and a refused one reports the reason an operator acts on. */
    @Test
    void statusDistinguishesARefusalFromAHealthyProcess() {
        ProcessStatus running = new ProcessStatus(
                "shipper", ProcessStatus.Lifecycle.RUNNING, Optional.empty(), Optional.empty());
        assertFalse(running.refused(), "a running process has not refused");
        assertTrue(running.refusalReason().isEmpty(), "and carries no reason");

        ProcessStatus refused = new ProcessStatus("shipper", ProcessStatus.Lifecycle.STOPPED,
                Optional.of(FailClosedException.Reason.CHANNEL_IDENTITY_CHANGED),
                Optional.of("orders was recreated"));
        assertTrue(refused.refused(), "a stopped process with a reason has refused");
        assertEquals(FailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, refused.refusalReason().orElseThrow(),
                "and names the condition");
        assertEquals(ProcessStatus.Lifecycle.STOPPED, refused.lifecycle(), "the lifecycle reads stopped");
    }

    /** A delivery of the fixture record, with no headers. */
    private static Delivery<String, String> delivery(String key, String value) {
        return Delivery.of(ORDERS, 0, 12L, 1_000L, key, value, List.of());
    }

    /** A read view over one store holding one entry, standing in for the runtime's own. */
    private static State stateHolding(Store<String, String> store, String key, String value) {
        Map<String, String> entries = new HashMap<>();
        entries.put(key, value);
        return new State() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> V get(Store<K, V> requested, K requestedKey) {
                assertSame(store, requested, "the handler read the store it declared");
                return (V) entries.get(requestedKey);
            }
        };
    }

    /** A null value is refused by put, which directs the caller to delete instead. */
    @Test
    void putRefusesANullValue() {
        Effects.Builder builder = Effects.builder();
        try {
            builder.put(INVENTORY, "sku-1", null);
            assertNull("unreachable", "put must refuse a null value");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("delete"),
                    "the refusal names delete as the way to remove a key");
        }
    }
}
