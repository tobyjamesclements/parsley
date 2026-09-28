package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import io.github.tobyjamesclements.parsley.core.PurityScan;

/**
 * Establishes that the companion surface names no host facility and rides only the core.
 *
 * <p>The session package must be usable at any edge — an HTTP gateway, a plain Kafka
 * client, a read tier over a database — so, like the core it rides on, it may name no
 * clock, no randomness, no network and no substrate ({@link PurityScan}'s shared policy).
 * One entry is its own: any type of the root package, which holds both the Kafka Streams
 * adapter, because the companion must not couple to the host, and the declaration surface,
 * because the charter is the core's public surface alone — a session participant declares
 * no process, so reaching for the declaration surface is exactly the accretion D99 fences
 * against. The root package is spelled as a pattern because its name is the prefix of every
 * subpackage, the core's included.
 */
class SessionPurityTest {

    /** A fully qualified reference to any type declared directly in the root package. */
    private static final Pattern ROOT_PACKAGE_TYPE =
            Pattern.compile("io\\.github\\.tobyjamesclements\\.parsley\\.[A-Z]");

    /** Session sources touch neither clock nor network nor substrate, nor the root package. */
    @Test
    void sessionSourcesTouchOnlyTheCoreSurface() throws IOException {
        Path sources = Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "session");
        String rationale = "the companion must stay usable at any edge, over the core's public surface alone";
        PurityScan.assertSourcesAvoid(sources, PurityScan.HOST_FACILITIES, rationale);
        PurityScan.assertSourcesMatchNone(sources, List.of(ROOT_PACKAGE_TYPE), rationale);
    }
}
