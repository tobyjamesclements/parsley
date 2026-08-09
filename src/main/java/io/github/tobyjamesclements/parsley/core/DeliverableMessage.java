package io.github.tobyjamesclements.parsley.core;

import java.util.List;

/** A held message the decision unit has judged deliverable, complete with its body, ready to hand to application logic. */
public record DeliverableMessage(
        ChannelId channel,
        long position,
        long timestamp,
        byte[] key,
        byte[] value,
        List<HeaderKV> headers,
        Causes causes) {
}
