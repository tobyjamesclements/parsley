package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the companion surface names no host facility and no adapter type.
 *
 * <p>The session package must be usable at any edge — an HTTP gateway, a plain Kafka
 * client, a read tier over a database — so, like the core it rides on, it may name no
 * clock, no network, no substrate, and additionally no type of the Kafka adapter package.
 * Residency outside {@code core} already guarantees it compiles against the public surface
 * only; this scan guarantees the host-independence half.
 */
class SessionPurityTest {
    private static final List<String> FORBIDDEN = List.of(
            "org.apache.kafka",
            "io.github.tobyjamesclements.parsley.kafka",
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

    /** Session sources touch neither clock nor network nor substrate nor the adapter. */
    @Test
    void sessionSourcesTouchNeitherClockNorNetworkNorSubstrateNorAdapter() throws IOException {
        Path sessionSources = Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "session");
        assertTrue(Files.isDirectory(sessionSources), "session sources must be present for this scan");
        // Recursive, so a future subpackage of session/ cannot escape the scan.
        try (Stream<Path> files = Files.walk(sessionSources)) {
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
                                    + "\": the companion must stay usable at any edge");
                }
            });
        }
    }
}
