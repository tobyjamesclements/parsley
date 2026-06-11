/**
 * Compact binary implementation of {@link io.parsley.VectorClockSerialiser}.
 *
 * <p>Provides {@link io.parsley.serialisation.DefaultVectorClockSerialiser}, which is
 * registered automatically as the default {@link io.parsley.VectorClockSerialiser} via
 * {@link java.util.ServiceLoader}. The wire format is a length-prefixed sequence of
 * {@code (topic, partition, offset)} triples with no external schema dependency.
 */
module io.parsley.serialisation {
    requires io.parsley;
    exports io.parsley.serialisation;
    provides io.parsley.VectorClockSerialiser
            with io.parsley.serialisation.DefaultVectorClockSerialiser;
}
