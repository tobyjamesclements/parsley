package io.parsley;

/**
 * The wire-level attribute names Parsley stamps onto messages.
 */
public final class ParsleyAttributes {

    /**
     * The name of the message attribute (e.g. Kafka record header) carrying a record's
     * serialised dependency {@link VectorClock}.
     */
    public static final String VECTOR_CLOCK = "parsley-vector-clock";

    private ParsleyAttributes() {}
}
