package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Establishes that the protocol core names no host facility.
 *
 * <p>Scans the core sources for any reference to a clock, randomness, the network or the
 * substrate, through the shared {@link PurityScan} fence. This is what allows the same
 * engine to run under a simulator and under Kafka Streams.
 */
class CorePurityTest {

    /** Core sources touch neither clock nor randomness nor network nor substrate. */
    @Test
    void coreSourcesTouchNeitherClockNorNetworkNorSubstrate() throws IOException {
        PurityScan.assertSourcesAvoid(
                Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "core"),
                PurityScan.HOST_FACILITIES,
                "the core decides from its arguments alone");
    }
}
