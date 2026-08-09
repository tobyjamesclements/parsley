package io.github.tobyjamesclements.parsley.core;

import java.util.Set;

/**
 * Test-only fault injection: each mode disables one guarantee so the test suite can prove it would catch the
 * resulting violation (see EVIDENCE.md). Package-private on purpose — the public API offers no way to construct an
 * engine with any of these enabled (SPEC Structural 9).
 */
record Sabotage(Set<Mode> modes) {

    enum Mode {
        /** Deliver regardless of expressed causes (breaks SPEC Safety 1). */
        IGNORE_CAUSES,
        /** Offer every held message to the decision, not just the channel head (breaks SPEC Safety 3). */
        NO_FIFO,
        /** Do not drop re-fed, already-delivered messages (breaks SPEC Safety 2). */
        REDELIVER_REFEEDS,
        /** Treat undecodable causal metadata as absent (breaks SPEC Safety 7). */
        UNDECODABLE_AS_ABSENT,
        /** Do not merge causes from received-but-undelivered metadata into the frontier (breaks SPEC Structural 15). */
        SKIP_RECEIPT_MERGE,
        /** Do not persist held messages (breaks SPEC Liveness 5). */
        DROP_HELD,
        /** Treat positions discarded below the earliest retained position as never-carried (breaks SPEC Safety 8). */
        IGNORE_TRUNCATION,
        /** Start an execution even when a removed channel still holds undelivered messages (breaks SPEC Structural 16). */
        IGNORE_REMOVED_CHANNELS,
        /** Silently discard the message received at position 3 of any channel as a duplicate (breaks SPEC Liveness 1). */
        SILENT_DROP,
        /** Merge fedUpTo — assigned positions that are not causes — into every emission stamp (over-expression). */
        OVEREXPRESS,
        /** Ignore the fact that a received channel's topic was recreated under its name (breaks SPEC Assumption 2). */
        IGNORE_RECREATION,
        /** Settle a dead received channel even while messages from it remain held undelivered (breaks SPEC Safety 9). */
        DELIVER_PAST_DEAD_HOLDS
    }

    static final Sabotage NONE = new Sabotage(Set.of());

    boolean has(Mode mode) {
        return modes.contains(mode);
    }
}
