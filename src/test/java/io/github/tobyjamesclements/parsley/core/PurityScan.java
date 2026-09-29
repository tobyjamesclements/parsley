package io.github.tobyjamesclements.parsley.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one spelling of the source-scan purity fence, shared by every package that claims
 * host-independence.
 *
 * <p>{@link #HOST_FACILITIES} is the policy: clocks, randomness, the network, the
 * filesystem and the substrate. Keeping the list here rather than copied per test is what
 * stops the fences drifting apart — a facility added for one package is enforced for every
 * package that scans, and a package needing more appends its own entries. An entry is a
 * fixed string, or, where no string can spell the fence, a pattern: {@link #ROOT_PACKAGE_TYPE}
 * is the one such entry, because the root package's name is the prefix of every subpackage,
 * so a string naming it would also name the packages a scanned package may use. Public for
 * the same reason {@code EngineTestFactory} is: the fence is used from more than one test
 * package.
 *
 * <p>The check is textual over the whole source text, comments and javadoc included, so a
 * forbidden string or pattern must not appear even in prose.
 */
public final class PurityScan {

    /** Clock, randomness, network, filesystem and substrate references no pure package may name. */
    public static final List<String> HOST_FACILITIES = List.of(
            "org.apache.kafka",
            "java.net.",
            "java.nio.channels",
            "java.nio.file",
            "java.io.",
            "java.time.",
            "java.util.Date",
            "java.util.Random",
            "java.util.concurrent",
            "ThreadLocalRandom",
            "SecureRandom",
            "UUID.randomUUID",
            "Math.random",
            "Thread.sleep",
            "System.currentTimeMillis",
            "System.nanoTime",
            "System.getenv",
            "System.getProperty",
            "Instant.now",
            "Clock.");

    /**
     * Any reference to a type declared directly in the root package — the declaration
     * surface and the Kafka Streams adapter — whether fully qualified or as an on-demand
     * import of the whole package, which would otherwise admit every root type by its
     * simple name.
     */
    public static final Pattern ROOT_PACKAGE_TYPE =
            Pattern.compile("io\\.github\\.tobyjamesclements\\.parsley\\.(\\*|[A-Z])");

    private PurityScan() {
    }

    /**
     * Scans every {@code .java} file under a source directory and fails on any forbidden
     * string. Recursive, so a future subpackage cannot escape the scan.
     *
     * @param sourceDir the source directory to scan, which must exist
     * @param forbidden the strings no scanned source may contain
     * @param rationale appended to the failure message, saying why the package is fenced
     * @throws IOException if a source file cannot be read
     */
    public static void assertSourcesAvoid(Path sourceDir, List<String> forbidden, String rationale)
            throws IOException {
        assertSourcesAvoid(sourceDir, forbidden, List.of(), rationale);
    }

    /**
     * Scans every {@code .java} file under a source directory once and fails on any
     * forbidden string or pattern. Recursive, so a future subpackage cannot escape the scan.
     *
     * @param sourceDir         the source directory to scan, which must exist
     * @param forbidden         the strings no scanned source may contain
     * @param forbiddenPatterns the patterns no scanned source may match
     * @param rationale         appended to the failure message, saying why the package is fenced
     * @throws IOException if a source file cannot be read
     */
    public static void assertSourcesAvoid(Path sourceDir, List<String> forbidden, List<Pattern> forbiddenPatterns,
                                          String rationale) throws IOException {
        assertTrue(Files.isDirectory(sourceDir), sourceDir + " must be present for this scan");
        try (Stream<Path> files = Files.walk(sourceDir)) {
            files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                String source;
                try {
                    source = Files.readString(path);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                for (String entry : forbidden) {
                    assertTrue(!source.contains(entry),
                            path.getFileName() + " must not use \"" + entry + "\": " + rationale);
                }
                for (Pattern entry : forbiddenPatterns) {
                    assertTrue(!entry.matcher(source).find(),
                            path.getFileName() + " must not match \"" + entry + "\": " + rationale);
                }
            });
        }
    }
}
