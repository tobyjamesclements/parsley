package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A supply of private Kafka Streams state directories for one test class, removed once the class
 * has run.
 *
 * <p>Kafka Streams derives its state directory from {@code state.dir} plus the application id, so
 * a class that pins an application id and leaves {@code state.dir} at its default puts every
 * instance it builds on one path. Concurrent instances on that path contend on a single RocksDB
 * lock, and the losers error on store open. That is a failing test when a suite runs its classes
 * concurrently, and something worse under PIT, which counts an erroring test as a killed mutant:
 * contention then reads as detection and inflates the mutation score. A directory per instance
 * removes the contention, and the sweep keeps a run from leaving a tree per instance behind under
 * the OS temp directory.
 *
 * <p>Register it as an extension, so the sweep needs no {@code @AfterAll} of its own:
 *
 * <pre>{@code
 * @RegisterExtension
 * static final TestStateDirectories STATE_DIRS = new TestStateDirectories("decorator-test-");
 * }</pre>
 */
final class TestStateDirectories implements AfterAllCallback {

    private final String prefix;

    /** Every directory {@link #create()} has handed out, in creation order. */
    private final Queue<Path> handedOut = new ConcurrentLinkedQueue<>();

    /**
     * @param prefix the temp-directory name prefix, which names the owning class in a directory
     *               listing when a sweep is missed (a crashed JVM leaves its directories behind)
     */
    TestStateDirectories(String prefix) {
        this.prefix = requireNonNull(prefix, "prefix");
    }

    /**
     * A fresh, unique state directory for one Streams instance. A test that expects construction
     * itself to fail needs one too, since a failed construction cannot be closed to release the
     * locks it took.
     */
    Path create() {
        try {
            Path dir = Files.createTempDirectory(prefix);
            handedOut.add(dir);
            return dir;
        } catch (IOException e) {
            throw new UncheckedIOException("could not create a state directory for the test", e);
        }
    }

    /** Removes every directory handed out to the class that registered this extension. */
    @Override
    public void afterAll(ExtensionContext context) throws IOException {
        for (Path dir : handedOut) {
            if (!Files.exists(dir)) {
                continue;
            }
            try (Stream<Path> tree = Files.walk(dir)) {
                for (Path path : tree.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        }
        handedOut.clear();
    }
}
