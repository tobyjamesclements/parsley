/**
 * Compact binary implementation of {@link io.parsley.VectorClockSerialiser}.
 *
 * <p>Registered automatically as the default provider via {@link java.util.ServiceLoader}.
 * The wire format is a length-prefixed sequence of {@code (topic, partition, offset)} triples;
 * no external schema or dependency is required.
 */
package io.parsley.serialisation;
