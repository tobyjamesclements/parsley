package io.github.tobyjamesclements.parsley.core;

import java.util.List;

/**
 * A message the engine has established as deliverable, with its causes decoded.
 *
 * @param channel   the channel it arrived on
 * @param position  its offset within that channel
 * @param timestamp its message timestamp
 * @param key       its key bytes
 * @param value     its value bytes
 * @param headers   its headers, reserved entries included
 * @param causes    the frontier its sender expressed
 * @see ReceivedMessage
 */
public record DeliverableMessage(
        ChannelId channel,
        long position,
        long timestamp,
        byte[] key,
        byte[] value,
        List<HeaderKV> headers,
        Causes causes) {
}
