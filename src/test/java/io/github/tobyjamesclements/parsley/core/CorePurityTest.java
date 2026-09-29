package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Establishes that the protocol core names no host facility and no type of the root package.
 *
 * <p>Scans the core sources for any reference to a clock, randomness, the network or the
 * substrate, through the shared {@link PurityScan} fence. This is what allows the same
 * engine to run under a simulator and under Kafka Streams. The root package holds the
 * adapter and the declaration surface, both of which depend on the core; the core naming
 * either, even in prose, would be the dependency running backwards (D116).
 */
class CorePurityTest {

    /** Core sources touch neither clock nor randomness nor network nor substrate, nor the root package. */
    @Test
    void coreSourcesTouchNeitherClockNorNetworkNorSubstrate() throws IOException {
        PurityScan.assertSourcesAvoid(
                Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "core"),
                PurityScan.HOST_FACILITIES,
                List.of(PurityScan.ROOT_PACKAGE_TYPE),
                "the core decides from its arguments alone");
    }
}
