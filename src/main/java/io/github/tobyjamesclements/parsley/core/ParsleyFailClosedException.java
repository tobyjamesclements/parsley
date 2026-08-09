package io.github.tobyjamesclements.parsley.core;

/**
 * Thrown where the delivery guarantee cannot be upheld.
 *
 * <p>Every throw stops delivery rather than weakening the guarantee. A process that fails
 * closed stays down until an operator intervenes, so this is a diagnosis rather than a
 * condition to retry.
 *
 * @see #reason()
 * @see io.github.tobyjamesclements.parsley.api.ProcessStatus#refusalReason()
 */
public final class ParsleyFailClosedException extends RuntimeException {

    /** Why delivery stopped. */
    public enum Reason {
        /** Causal metadata was present but could not be decoded. */
        UNDECODABLE_METADATA,
        /** Positions were discarded by retention before this process read them. */
        POSITIONS_DISCARDED_UNREAD,
        /** The host fed a channel out of position order. */
        OUT_OF_ORDER_FEED,
        /** A channel left the received set while it still held undelivered messages. */
        CHANNEL_REMOVED_WITH_HELD_MESSAGES,
        /** A topic was recreated, so positions held against the old identity are meaningless. */
        CHANNEL_IDENTITY_CHANGED,
        /** A received topic was deleted while messages from it remained undelivered. */
        CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES,
        /** The task count changed, so ordering state no longer matches its partitioning. */
        TASK_WIDTH_CHANGED,
        /** Stored ordering state carries a format version this build cannot read. */
        UNKNOWN_ORDERING_STATE_FORMAT,
        /** A handler emitted on a channel its process never declared. */
        EMISSION_TO_UNDECLARED_CHANNEL,
        /** An application header used the prefix reserved for causal metadata. */
        RESERVED_HEADER_USED,
        /** An application payload could not be decoded by its declared serde. */
        APPLICATION_PAYLOAD_UNDECODABLE,
        /** The substrate is configured in a way the guarantee cannot survive. */
        SUBSTRATE_MISCONFIGURED,
        /** Causal metadata exceeded the configured budget. */
        METADATA_BUDGET_EXCEEDED
    }

    /** Why delivery stopped. */
    private final Reason reason;

    /**
     * Builds a diagnosis.
     *
     * @param reason  why delivery stopped
     * @param message the diagnosis, prefixed with {@code reason}
     */
    public ParsleyFailClosedException(Reason reason, String message) {
        super(reason + ": " + message);
        this.reason = reason;
    }

    /**
     * Builds a diagnosis wrapping an underlying failure.
     *
     * @param reason  why delivery stopped
     * @param message the diagnosis, prefixed with {@code reason}
     * @param cause   the underlying failure
     */
    public ParsleyFailClosedException(Reason reason, String message, Throwable cause) {
        super(reason + ": " + message, cause);
        this.reason = reason;
    }

    /**
     * Returns why delivery stopped.
     *
     * @return why delivery stopped
     */
    public Reason reason() {
        return reason;
    }

    /**
     * Searches a failure's cause chain for a fail-closed diagnosis.
     *
     * <p>Kafka Streams wraps handler failures, so the diagnosis is rarely the outermost
     * throwable. The search is bounded to guard against a cyclic chain.
     *
     * @param failure the throwable to search, which may be {@code null}
     * @return the first fail-closed exception found, or {@code null} when there is none
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
