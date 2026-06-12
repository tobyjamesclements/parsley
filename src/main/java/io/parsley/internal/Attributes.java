package io.parsley.internal;

/**
 * Parsley's wire and state-store protocol names — the single source of truth shared by the
 * producer, stream engine, and consumer.
 *
 * <p><strong>Internal:</strong> not part of the public API. This package is excluded from the
 * generated Javadoc; do not depend on it. The class is {@code public} only so the producer,
 * consumer, and stream packages can share these constants without duplicating them.
 */
public final class Attributes {

    private Attributes() {}

    /** Header carrying a record's serialised dependency {@code VectorClock}. */
    public static final String VECTOR_CLOCK = "parsley-vector-clock";

    /** Persistent state store holding the consumer's frontier. */
    public static final String FRONTIER_STORE = "parsley-frontier";

    /** Key under which the frontier is stored in {@link #FRONTIER_STORE}. */
    public static final String FRONTIER_KEY = "f";

    /** Header stamped on forwarded records carrying the source topic. */
    public static final String SOURCE_TOPIC = "parsley-src-topic";

    /** Header stamped on forwarded records carrying the source partition. */
    public static final String SOURCE_PARTITION = "parsley-src-partition";

    /** Header stamped on forwarded records carrying the source offset. */
    public static final String SOURCE_OFFSET = "parsley-src-offset";
}
