package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A channel's wire encoding is fixed-width and round-trips. */
class ChannelTest {
    /** A channel encodes to a fixed width and decodes back to itself, by either route. */
    @Test
    void wireEncodingRoundTrips() {
        Channel channel = new Channel(new UUID(1L, 2L), 3);

        byte[] encoded = channel.toBytes();
        assertEquals(Channel.ENCODED_LENGTH, encoded.length, "an encoded channel is a fixed width");
        assertEquals(channel, Channel.readFrom(ByteBuffer.wrap(encoded)), "and decodes back to itself");

        ByteBuffer buffer = ByteBuffer.allocate(Channel.ENCODED_LENGTH);
        channel.writeTo(buffer);
        assertEquals(channel, Channel.readFrom(buffer.flip()), "writeTo and readFrom agree");
    }
}
