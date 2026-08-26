package io.github.tobyjamesclements.parsley.session;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import io.github.tobyjamesclements.parsley.core.PurityScan;

/**
 * Establishes that the companion surface names no host facility and rides only the core.
 *
 * <p>The session package must be usable at any edge — an HTTP gateway, a plain Kafka
 * client, a read tier over a database — so, like the core it rides on, it may name no
 * clock, no randomness, no network and no substrate ({@link PurityScan}'s shared policy).
 * Two entries are its own: the Kafka adapter package, because the companion must not
 * couple to the host, and the {@code api} package, because the charter is the core's
 * public surface alone — a session participant declares no process, so reaching for the
 * declaration surface is exactly the accretion D99 fences against.
 */
class SessionPurityTest {

    /** Session sources touch neither clock nor network nor substrate, nor the adapter or api packages. */
    @Test
    void sessionSourcesTouchOnlyTheCoreSurface() throws IOException {
        List<String> forbidden = new ArrayList<>(PurityScan.HOST_FACILITIES);
        forbidden.add("io.github.tobyjamesclements.parsley.kafka");
        forbidden.add("io.github.tobyjamesclements.parsley.api");
        PurityScan.assertSourcesAvoid(
                Path.of("src", "main", "java", "io", "github", "tobyjamesclements", "parsley", "session"),
                forbidden,
                "the companion must stay usable at any edge, over the core's public surface alone");
    }
}
