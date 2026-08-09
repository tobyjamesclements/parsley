package io.github.tobyjamesclements.parsley.core;

import java.util.Set;

/**
 * Deliberate faults, each disabling one guarantee.
 *
 * <p>This exists so the suite can demonstrate that it catches every violation class, rather
 * than asserting that it would. Package-private on purpose: the public API offers no way to
 * construct an engine with a mode enabled.
 *
 * @param modes the faults to enable
 */
record Sabotage(Set<Mode> modes) {

    /** One disabled guarantee. */
    enum Mode {
        /** Deliver regardless of the causes a message expressed. */
        IGNORE_CAUSES,

        /** Offer every held message to the decision rather than the channel head. */
        NO_FIFO,

        /** Deliver a re-fed message that was already delivered. */
        REDELIVER_REFEEDS,

        /** Treat metadata that cannot be decoded as metadata that was absent. */
        UNDECODABLE_AS_ABSENT,

        /** Skip merging causes carried by a message that was received and not delivered. */
        SKIP_RECEIPT_MERGE,

        /** Hold messages in memory without persisting them. */
        DROP_HELD,

        /** Treat positions discarded by retention as positions never carried. */
        IGNORE_TRUNCATION,

        /** Start even where a removed channel still holds undelivered messages. */
        IGNORE_REMOVED_CHANNELS,

        /** Discard the message at position 3 of any channel as a duplicate. */
        SILENT_DROP,

        /** Stamp emissions with assigned positions that are not causes. */
        OVEREXPRESS,

        /** Ignore a received channel whose topic was recreated under its name. */
        IGNORE_RECREATION,

        /** Settle a dead channel while messages from it remain held. */
        DELIVER_PAST_DEAD_HOLDS
    }

    /** No faults enabled. */
    static final Sabotage NONE = new Sabotage(Set.of());

    /**
     * @param mode the fault to test for
     * @return {@code true} when {@code mode} is enabled
     */
    boolean has(Mode mode) {
        return modes.contains(mode);
    }
}
