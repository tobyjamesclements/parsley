package io.parsley;

/**
 * Parsley's wire and state-store protocol names — the single source of truth shared by the producer,
 * stream engine, and consumer. Package-private: an internal implementation detail, not public API.
 */
final class ParsleyAttributes {

    private ParsleyAttributes() {}

    /** Header carrying a record's serialised dependency {@code CausalDependencies}. */
    static final String VECTOR_CLOCK = "parsley-vector-clock";

    /** Key under which the frontier is stored in the processor's frontier state store. */
    static final String FRONTIER_KEY = "f";
}
