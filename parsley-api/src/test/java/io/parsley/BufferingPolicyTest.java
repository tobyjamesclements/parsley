package io.parsley;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BufferingPolicyTest {

    private static final BufferLimit LIMIT = BufferLimit.ofDuration(Duration.ofSeconds(30));

    @Test
    void ignoreFactoryReturnsIgnoreWithLimit() {
        assertEquals(new BufferingPolicy.Ignore(LIMIT), BufferingPolicy.ignore(LIMIT));
    }

    @Test
    void dropFactoryReturnsDropWithLimit() {
        assertEquals(new BufferingPolicy.Drop(LIMIT), BufferingPolicy.drop(LIMIT));
    }

    @Test
    void deadLetterFactoryReturnsDeadLetterWithLimitAndDestination() {
        assertEquals(new BufferingPolicy.DeadLetter(LIMIT, "dead"),
                BufferingPolicy.deadLetter(LIMIT, "dead"));
    }
}
