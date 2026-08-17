package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the one-rule exception taxonomy D73 records: a null component is refused with
 * {@code IllegalArgumentException} across {@code api/} and {@code core/} alike. These two
 * records previously threw {@code NullPointerException} while their siblings ({@code Causes},
 * every {@code api/} site) threw {@code IllegalArgumentException} for the identical mistake.
 */
class NullComponentRefusalTest {

    /** A null header key is refused with the taxonomy's exception, not an NPE. */
    @Test
    void nullHeaderKeyIsRefusedAsAnIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> new HeaderKV(null, new byte[0]),
                "one rule for null components: HeaderKV must refuse like Causes and the api/"
                        + " surface do, not with its own NullPointerException");
    }

    /** A null channel topic identity is refused with the taxonomy's exception, not an NPE. */
    @Test
    void nullTopicIdIsRefusedAsAnIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> new ChannelId(null, 0),
                "one rule for null components: ChannelId's null refusal must match its own"
                        + " negative-partition refusal and the rest of the surface");
    }

    /** A negative partition stays refused alongside the null rule. */
    @Test
    void negativePartitionStaysRefused() {
        assertThrows(IllegalArgumentException.class, () -> new ChannelId(new UUID(1, 1), -1),
                "the taxonomy change must not loosen the existing range check");
    }
}
