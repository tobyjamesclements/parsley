package io.github.tobyjamesclements.parsley;

/**
 * Thrown when an {@code epoch-events} record cannot be deserialised into a {@link ParsleyEpochEvent} this
 * binary understands — a corrupt record, a truncated body, or (the common cause during an upgrade) a
 * record written in the pre-genesis-cohort wire format ({@link ParsleyEpochEvent#TAG_JOIN}/{@link
 * ParsleyEpochEvent#TAG_COMMIT}). Unlike a transient transport/broker error, this never heals on its own:
 * the log and the binary genuinely disagree on the wire format, so the epoch runtime treats it as
 * <strong>fatal</strong> rather than backing off and retrying forever (which would strand every join in a
 * misleading "log not folded to its end" timeout). The remediation for an older log is to reset the
 * epoch-events topic, which is safe before genesis has committed (nothing durable is lost).
 *
 * <p>Deliberately distinct from a broker error so {@link ParsleyEpochRuntime#runOnce} can fail closed on
 * it while still surviving a transient blip.
 */
final class ParsleyIncompatibleEpochLogException extends RuntimeException {

    ParsleyIncompatibleEpochLogException(String message) {
        super(message);
    }

    ParsleyIncompatibleEpochLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
