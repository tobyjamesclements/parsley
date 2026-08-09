package io.github.tobyjamesclements.parsley.core;

/**
 * Thrown to fail closed (SPEC Terminology): the process stops delivering rather than proceed with a weakened
 * guarantee. The adapter lets this propagate out of the processor, failing the step — the transaction aborts, nothing
 * is delivered past the failure, and the process re-fails on restart until an operator intervenes.
 */
public final class ParsleyFailClosedException extends RuntimeException {

    public enum Reason {
        /** Causal metadata present but undecodable (SPEC Safety 7). */
        UNDECODABLE_METADATA,
        /** Read position at or resuming below the channel's earliest retained position (SPEC Safety 8). */
        POSITIONS_DISCARDED_UNREAD,
        /** The host fed a position already covered as fed-or-never-arriving within this execution (Host obligation 1/2 breach). */
        OUT_OF_ORDER_FEED,
        /** Execution declared without a channel on which received messages remain undelivered (SPEC Structural 16). */
        CHANNEL_REMOVED_WITH_HELD_MESSAGES,
        /** A declared topic's identity changed: the topic was deleted and recreated under the same name, so the
         * group's read positions for that name belong to a dead channel (SPEC Assumption 2, Safety 8). */
        CHANNEL_IDENTITY_CHANGED,
        /** A received channel's topic no longer exists while this process still retains received-but-undelivered
         * messages from it. Senders that delivered from that channel may already have discarded its causes from
         * their metadata (SPEC Structural 13 permits that), so arriving effects can no longer be ordered against
         * the held messages: their place in causal order cannot be preserved, and the process must stop rather
         * than deliver past them (SPEC Safety 9; the deletion also breached Assumption 17's hygiene). */
        CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES,
        /** The declaration's task width no longer matches the width the ordering state was built for; the
         * ordering store's changelog cannot change partition count, so the execution is refused rather than left
         * to die in the host's internal-topic validation with a state-destroying remedy (SPEC Structural 16). */
        TASK_WIDTH_CHANGED,
        /** Persisted ordering state has a format this build does not understand. */
        UNKNOWN_ORDERING_STATE_FORMAT,
        /** Application logic emitted to a channel outside the declared send set (SPEC Structural 19). */
        EMISSION_TO_UNDECLARED_CHANNEL,
        /** Application attached a header using the reserved parsley prefix (SPEC Structural 5). */
        RESERVED_HEADER_USED,
        /** The application's own codec could not decode a delivered payload; delivering past it would break SPEC Safety 3. */
        APPLICATION_PAYLOAD_UNDECODABLE,
        /** The host or substrate configuration violates SPEC Substrate requirements (e.g. EOS override). */
        SUBSTRATE_MISCONFIGURED,
        /** Causal metadata reached the configured budget on receipt or emission: the process stops with its own
         * diagnosis rather than running into the substrate's record-size wall, where the failure would be a
         * permanent, unattributed production error (SPEC Operational 4). */
        METADATA_BUDGET_EXCEEDED
    }

    private final Reason reason;

    public ParsleyFailClosedException(Reason reason, String message) {
        super(reason + ": " + message);
        this.reason = reason;
    }

    public ParsleyFailClosedException(Reason reason, String message, Throwable cause) {
        super(reason + ": " + message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    /**
     * The outermost {@code ParsleyFailClosedException} in a failure's cause chain, or null. The walk is
     * depth-capped: cause chains can legally be cyclic ({@code initCause} rejects only self-reference), and
     * callers run this inside failure handlers where an unbounded walk would wedge them.
     */
    public static ParsleyFailClosedException findIn(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (cause instanceof ParsleyFailClosedException parsley) {
                return parsley;
            }
        }
        return null;
    }
}
