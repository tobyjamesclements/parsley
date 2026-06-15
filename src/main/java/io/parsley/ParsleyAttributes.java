package io.parsley;

/**
 * Parsley's wire and state-store protocol names — the single source of truth shared by the producer,
 * stream engine, and consumer. Package-private: an internal implementation detail, not public API.
 */
final class ParsleyAttributes {

    private ParsleyAttributes() {}

    /** Header carrying a record's serialised dependency {@code VectorClock}. */
    static final String VECTOR_CLOCK = "parsley-vector-clock";

    /** Persistent state store holding the consumer's frontier. */
    static final String FRONTIER_STORE = "parsley-frontier";

    /** Key under which the frontier is stored in {@link #FRONTIER_STORE}. */
    static final String FRONTIER_KEY = "f";

    /**
     * Persistent state store holding records a decorating causal processor has buffered while
     * their causal dependencies are unmet, so they survive a restart.
     */
    static final String BUFFER_STORE = "parsley-buffer";
}
