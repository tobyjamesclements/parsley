package io.github.tobyjamesclements.parsley;

import org.testcontainers.utility.DockerImageName;

/**
 * The single source of the Kafka broker container image the broker ITs run against, so the CI
 * broker-version matrix can swap it without touching each test: {@code -Dparsley.it.kafka.image}
 * overrides the default pin. The default is the documented minimum supported broker version
 * (see {@code docs/adoption.md}); CI additionally runs the suite against the current stable
 * broker matching the {@code kafka.version} client line.
 *
 * <p>{@link CausalProcessorAvroIT} is deliberately outside this seam: it needs a Schema Registry,
 * which ships only with the Confluent platform images, so it pins {@code confluentinc/cp-kafka}
 * separately.
 */
final class ParsleyBrokerImage {

    /** The default broker image — the minimum supported broker version. */
    static final String DEFAULT = "apache/kafka:3.7.0";

    private ParsleyBrokerImage() {
    }

    /** The broker image for this run: {@code -Dparsley.it.kafka.image}, or the default pin. */
    static DockerImageName get() {
        return DockerImageName.parse(System.getProperty("parsley.it.kafka.image", DEFAULT));
    }
}
