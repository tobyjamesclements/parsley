package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The functional core tested as plain functions: handlers and folds return emission values
 * with equality, so logic is verified with {@code assertEquals} — no driver, no runtime, no
 * Kafka. This is the testing story the API is shaped for; the adapter plumbing has its own
 * tests under {@code TopologyTestDriver}.
 */
class LogicTest {

    private static final Topic<String, String> OUT = Topic.of("out", Codec.utf8(), Codec.utf8());

    /** A handler is a pure function; its emissions compare by value. */
    @Test
    void handlerIsTestableByEquality() {
        Handler<String, String> enrich = m -> List.of(OUT.send(m.key(), m.value() + "!"));

        assertEquals(List.of(OUT.send("k", "v!")),
                enrich.handle(Message.of("in", "k", "v")),
                "the handler's emissions must equal the expected emission values");
    }

    /** A fold is a pure step function; state and emissions compare by value together. */
    @Test
    void foldIsTestableByEquality() {
        Fold<Long, String, String> counter =
                (n, m) -> Step.of(n + 1, OUT.send(m.key(), "n=" + (n + 1)));

        assertEquals(Step.of(1L, OUT.send("k", "n=1")),
                counter.apply(0L, Message.of("in", "k", "v")),
                "one application must advance the state and emit the running count");
        assertEquals(Step.of(42L, OUT.send("k", "n=42")),
                counter.apply(41L, Message.of("in", "k", "v")),
                "the fold must be a function of the state argument alone");
    }

    /** Emission equality is by topic name, key, value, and timestamp. */
    @Test
    void emissionEqualityIsStructural() {
        assertEquals(OUT.send("k", "v"), OUT.send("k", "v"),
                "equal payloads to the same topic must be equal");
        assertEquals(OUT.send("k", "v", 5L), OUT.send("k", "v", 5L),
                "explicit timestamps participate in equality");
    }

    /** Emissions differing in topic, key, value, or timestamp are unequal. */
    @Test
    void emissionInequalityIsStructural() {
        Topic<String, String> other = Topic.of("other", Codec.utf8(), Codec.utf8());
        Emission e = OUT.send("k", "v");
        assertNotEquals(e, other.send("k", "v"), "a different topic must break equality");
        assertNotEquals(e, OUT.send("x", "v"), "a different key must break equality");
        assertNotEquals(e, OUT.send("k", "x"), "a different value must break equality");
        assertNotEquals(e, OUT.send("k", "v", 5L), "a different timestamp must break equality");
        assertNotEquals(e, "not an emission", "a non-emission must never be equal");
        assertEquals(Objects.hash("out", "k", "v", -1L), e.hashCode(),
                "the hash must combine topic name, key, value, and timestamp");
    }

    /** An emission carries its payload; the default timestamp is the inherit sentinel. */
    @Test
    void emissionCarriesPayloadAndTimestamp() {
        Emission inherit = OUT.send("k", "v");
        assertEquals("k", inherit.key(), "the key must be carried");
        assertEquals("v", inherit.value(), "the value must be carried");
        assertEquals(Emission.INHERIT_TIMESTAMP, inherit.timestamp(),
                "the two-argument send must inherit the handled message's timestamp");

        assertEquals(5L, OUT.send("k", "v", 5L).timestamp(),
                "an explicit timestamp must be carried");
    }

    /** The emission text form shows the topic and payload, and the timestamp only when set. */
    @Test
    void emissionTextFormMarksExplicitTimestamps() {
        assertEquals("out<-(k, v)", OUT.send("k", "v").toString(),
                "an inherited timestamp must not appear in the text form");
        assertEquals("out<-(k, v, @5)", OUT.send("k", "v", 5L).toString(),
                "an explicit timestamp must appear in the text form");
    }

    /** Topic names follow Kafka's legal character set; timestamps must be non-negative. */
    @Test
    void topicValidatesNameAndTimestamp() {
        assertEquals("legal.Name_1-x", Topic.of("legal.Name_1-x", Codec.utf8(), Codec.utf8()).toString(),
                "legal Kafka topic names must be accepted, and the text form is the name");
        assertThrows(IllegalArgumentException.class,
                () -> Topic.of("bad name!", Codec.utf8(), Codec.utf8()),
                "an illegal topic name must be rejected at declaration");

        assertEquals(0L, OUT.send("k", "v", 0L).timestamp(), "timestamp zero is legal");
        assertThrows(IllegalArgumentException.class, () -> OUT.send("k", "v", -2L),
                "a negative timestamp must be rejected at the construction site");
    }

    /** A recovering handler is transparent while the wrapped handler returns. */
    @Test
    void recoveringHandlerPassesSuccessThrough() {
        Handler<String, String> enrich = Handler.recovering(
                m -> List.of(OUT.send(m.key(), m.value() + "!")),
                (m, e) -> List.of(OUT.send(m.key(), "error")));

        assertEquals(List.of(OUT.send("k", "v!")),
                enrich.handle(Message.of("in", "k", "v")),
                "a returning handler's emissions must pass through the decorator untouched");
    }

    /** A recovering handler converts a RuntimeException into the recovery emissions. */
    @Test
    void recoveringHandlerRoutesFailureToTheRecovery() {
        Handler<String, String> failing = Handler.recovering(
                m -> { throw new IllegalArgumentException("unmappable: " + m.key()); },
                (m, e) -> List.of(OUT.send(m.key(), e.getMessage())));

        assertEquals(List.of(OUT.send("k", "unmappable: k")),
                failing.handle(Message.of("in", "k", "v")),
                "a throwing handler must yield the recovery's emissions for the failure");
    }

    /** A recovering handler lets an Error propagate: only RuntimeException is domain failure. */
    @Test
    void recoveringHandlerPropagatesErrors() {
        Handler<String, String> broken = Handler.recovering(
                m -> { throw new AssertionError("broken runtime"); },
                (m, e) -> List.of());

        assertThrows(AssertionError.class,
                () -> broken.handle(Message.of("in", "k", "v")),
                "an Error must propagate and fail the task, never route to the error sink");
    }

    /** A recovering fold is transparent while the wrapped fold returns. */
    @Test
    void recoveringFoldPassesSuccessThrough() {
        Fold<Long, String, String> counter = Fold.recovering(
                (n, m) -> Step.of(n + 1, OUT.send(m.key(), "n=" + (n + 1))),
                (m, e) -> List.of(OUT.send(m.key(), "error")));

        assertEquals(Step.of(1L, OUT.send("k", "n=1")),
                counter.apply(0L, Message.of("in", "k", "v")),
                "a returning fold's step must pass through the decorator untouched");
    }

    /** A recovering fold keeps the prior state and yields the recovery emissions on failure. */
    @Test
    void recoveringFoldKeepsPriorStateOnFailure() {
        Fold<Long, String, String> failing = Fold.recovering(
                (n, m) -> { throw new IllegalStateException("rejected"); },
                (m, e) -> List.of(OUT.send(m.key(), e.getMessage())));

        assertEquals(Step.of(41L, OUT.send("k", "rejected")),
                failing.apply(41L, Message.of("in", "k", "v")),
                "a throwing fold must keep the prior state and emit the recovery's emissions");
    }

    /** A recovering fold lets an Error propagate: only RuntimeException is domain failure. */
    @Test
    void recoveringFoldPropagatesErrors() {
        Fold<Long, String, String> broken = Fold.recovering(
                (n, m) -> { throw new AssertionError("broken runtime"); },
                (m, e) -> List.of());

        assertThrows(AssertionError.class,
                () -> broken.apply(0L, Message.of("in", "k", "v")),
                "an Error must propagate and fail the task, never route to the error sink");
    }

    /** Tick logic is pure: handlers and folds over ticks compare by value, no runtime. */
    @Test
    void tickLogicIsTestableByEquality() {
        TickHandler heartbeat = tick -> List.of(OUT.send("hb", "t=" + tick.timestamp()));
        assertEquals(List.of(OUT.send("hb", "t=7")), heartbeat.onTick(new Tick(7L)),
                "a tick handler's emissions must equal the expected emission values");

        TickFold<Long> policy = (n, tick) -> Step.of(n + 1, OUT.send("cut", "at=" + tick.timestamp()));
        assertEquals(Step.of(1L, OUT.send("cut", "at=5")), policy.apply(0L, new Tick(5L)),
                "a tick fold must step the tick state and emissions as plain values");
    }

    /** A message for a unit test carries a zero coordinate and the given payload. */
    @Test
    void messageFactoryCarriesPayload() {
        Message<String, String> m = Message.of("t", "k", "v");
        assertEquals("t", m.topic(), "topic must be carried");
        assertEquals("k", m.key(), "key must be carried");
        assertEquals("v", m.value(), "value must be carried");
        assertEquals(0L, m.offset(), "the test factory pins the coordinate at zero");
    }
}
