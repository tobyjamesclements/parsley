package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
