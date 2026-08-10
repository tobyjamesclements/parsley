package io.github.tobyjamesclements.parsley.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the protocol core names no host facility.
 *
 * <p>Scans the core sources for any reference to a clock, the network or the substrate. This
 * is what allows the same engine to run under a simulator and under Kafka Streams.
 */
class CorePurityTest {
    private static final List<String> FORBIDDEN = List.of(
            "org.apache.kafka",
            "java.net.",
            "java.nio.channels",
            "java.nio.file",
            "java.io.",
            "java.time.",
            "java.util.Date",
            "java.util.Random",
            "ThreadLocalRandom",
            "Math.random",
            "Thread.sleep",
            "System.currentTimeMillis",
            "System.nanoTime",
            "System.getenv",
            "System.getProperty",
            "Instant.now",
            "Clock.");

    /** Core sources touch neither clock nor randomness nor network nor substrate. */
    @Test
    void coreSourcesTouchNeitherClockNorNetworkNorSubstrate() throws IOException {
        Path coreSources = Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "core");
        assertTrue(Files.isDirectory(coreSources), "core sources must be present for this scan");
        // Recursive, so a future subpackage of core/ cannot escape the scan.
        try (Stream<Path> files = Files.walk(coreSources)) {
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
