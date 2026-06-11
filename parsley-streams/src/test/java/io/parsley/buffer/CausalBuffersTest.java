package io.parsley.buffer;

import io.parsley.BufferLimit;
import io.parsley.BufferingPolicy;
import io.parsley.serialisation.DefaultVectorClockSerialiser;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class CausalBuffersTest {

    private static final DefaultVectorClockSerialiser SERIALISER = new DefaultVectorClockSerialiser();
    private static final BufferLimit LIMIT = BufferLimit.ofDuration(Duration.ofSeconds(30));

    @Test
    void createIgnoreReturnsBuffer() {
        assertNotNull(CausalBuffers.create(BufferingPolicy.ignore(LIMIT), SERIALISER));
    }

    @Test
    void createDropReturnsBuffer() {
        assertNotNull(CausalBuffers.create(BufferingPolicy.drop(LIMIT), SERIALISER));
    }

    @Test
    void createDeadLetterWithTwoArgThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> CausalBuffers.create(BufferingPolicy.deadLetter(LIMIT, "dead-topic"), SERIALISER));
    }

    @Test
    void createDeadLetterWithSinkReturnsBuffer() {
        var policy = new BufferingPolicy.DeadLetter(LIMIT, "dead-topic");
        assertNotNull(CausalBuffers.create(policy, SERIALISER, r -> {}));
    }
}
