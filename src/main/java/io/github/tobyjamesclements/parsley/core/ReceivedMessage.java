package io.github.tobyjamesclements.parsley.core;

import java.util.List;

/**
 * A message as received from a channel: raw key and value bytes exactly as they sit in the record (SPEC Safety 4),
 * all headers, and the record's position and timestamp. The engine extracts and validates causal metadata itself.
 */
public record ReceivedMessage(
        ChannelId channel,
        long position,
        long timestamp,
        byte[] key,
        byte[] value,
        List<HeaderKV> headers) {

    public ReceivedMessage {
        if (position < 0) {
            throw new IllegalArgumentException("position must be non-negative: " + position);
        }
        headers = List.copyOf(headers);
    }
}
