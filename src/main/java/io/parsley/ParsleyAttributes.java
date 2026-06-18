package io.parsley;

/**
 * Parsley's wire and state-store protocol names — the single source of truth shared by the producer,
 * stream engine, and consumer. Package-private: an internal implementation detail, not public API.
 */
final class ParsleyAttributes {

    private ParsleyAttributes() {}

    /** Header carrying a record's serialised {@code CausalDependencies}. */
    static final String CAUSAL_DEPENDENCIES = "parsley-causal-dependencies";

    /** Key under which the frontier is stored in the processor's frontier state store. */
    static final String FRONTIER_KEY = "f";

    /** Internal header: the source topic of a record routed through the outbox path. */
    static final String SRC_TOPIC     = "_parsley_src_topic";
    /** Internal header: the Kafka topic UUID (16 bytes: MSB then LSB) of the source topic. */
    static final String SRC_TOPIC_ID  = "_parsley_src_topic_id";
    /** Internal header: the source partition (4-byte big-endian int) of a record routed through the outbox path. */
    static final String SRC_PARTITION = "_parsley_src_partition";
    /** Internal header: the source offset (8-byte big-endian long) of a record routed through the outbox path. */
    static final String SRC_OFFSET    = "_parsley_src_offset";
    /**
     * Internal header: a copy of the producer's original {@link #CAUSAL_DEPENDENCIES} header, saved before
     * {@link ParsleyProcessorContext} stamps it with the frontier. Restored in
     * {@code ParsleyConsumer.poll()} so that {@link CausalDependencies#fromRecord} still returns the
     * upstream producer's causal intent, not the delivery-time frontier.
     */
    static final String ORIGINAL_DEPENDENCIES = "_parsley_original_dependencies";
}
