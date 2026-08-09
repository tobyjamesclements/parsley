package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The protocol core must decide locally and purely (SPEC Structural 1, 7): no clocks, no network, no substrate
 * types. This scans the core package's sources — a build-time tripwire for anyone wiring I/O or time into the
 * decision path. The Kafka adapter lives outside this package on purpose.
 */
class CorePurityTest {

    // Matched anywhere in the source, not just imports, so fully-qualified references cannot slip past the scan.
    private static final List<String> FORBIDDEN = List.of(
            "org.apache.kafka",
            "java.net.",
            "java.nio.channels",
            "java.time.",
            "System.currentTimeMillis",
            "System.nanoTime",
            "new java.util.Date",
            "Instant.now",
            "Clock.");

    @Test
    void coreSourcesTouchNeitherClockNorNetworkNorSubstrate() throws IOException {
        Path coreSources = Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "core");
        assertTrue(Files.isDirectory(coreSources), "core sources must be present for this scan");
        try (Stream<Path> files = Files.list(coreSources)) {
            files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                String source;
                try {
                    source = Files.readString(path);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                for (String forbidden : FORBIDDEN) {
                    assertTrue(!source.contains(forbidden),
                            path.getFileName() + " must not use \"" + forbidden
                                    + "\": the core decides from its arguments alone");
                }
            });
        }
    }
}
