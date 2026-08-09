package io.github.tobyjamesclements.parsley.core;

import java.util.List;

/**
 * A message as the host fed it to the engine, before its causes were read.
 *
 * @param channel   the channel it arrived on
 * @param position  its offset within that channel
 * @param timestamp its message timestamp
 * @param key       its key bytes
 * @param value     its value bytes
 * @param headers   its headers, reserved entries included
 * @see DeliverableMessage
 */
public record ReceivedMessage(
        ChannelId channel,
        long position,
        long timestamp,
        byte[] key,
        byte[] value,
        List<HeaderKV> headers) {
    /**
     * Validates the position and copies the headers.
     *
     * @throws IllegalArgumentException if {@code position} is negative
     */
    public ReceivedMessage {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative: " + position);
        }
        headers = List.copyOf(headers);
    }
}
