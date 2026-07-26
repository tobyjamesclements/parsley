package io.github.tobyjamesclements.parsley;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Enforces Parsley's empty configuration surface: there are no {@code parsley.*} keys. The startup
 * topology checks always run and always fail fast (see {@link ParsleyProcessor}), and topology-epoch
 * coordination was removed from the causal protocol outright (joins need zero coordination), so every
 * behaviour that once had a key is now unconditional.
 *
 * <p>Causal delivery has exactly two dispositions, never a third: a record is <strong>forwarded</strong>
 * once its consumed dependencies are satisfied (a dependency on a coordinate this node does not
 * consume is unconditionally ignored, with a metric, because any consumed ancestor behind it is
 * claimed directly in the same clock, never a failure); while
 * unsatisfied it stays <strong>buffered</strong>, unbounded, changelog-backed. A record whose
 * dependencies are proven impossible to evaluate (an undecodable payload or clock header)
 * unconditionally fails the task. There is no configuration that trades causal safety for liveness,
 * and no diversion — proven impossibility always fails fast, never on pressure or time.
 *
 * <p>A {@code parsley.*} key found in the runtime {@code Properties} or a {@code parsley.properties}
 * classpath resource therefore wires nothing — and a key that wires nothing must not parse quietly,
 * or an operator who believes it took effect would never learn otherwise.
 * {@link #requireNoParsleyKeys(Properties)} fails startup loudly, naming every offending key.
 */
final class ParsleyConfig {

    /** The classpath resource earlier releases read configuration from, now scanned only to reject. */
    static final String RESOURCE = "parsley.properties";

    /** The reserved key prefix; any key under it fails startup. */
    static final String PARSLEY_KEY_PREFIX = "parsley.";

    private ParsleyConfig() {
    }

    /**
     * Fails startup when any {@code parsley.*} key is present in {@code runtimeProps} or in a
     * {@code parsley.properties} classpath resource. Both layers are scanned so a stale deployment
     * learns about the removal at startup, whichever way it used to supply the key. There is no
     * migration path because there is nothing left to configure: the startup topology checks (the
     * former {@code parsley.topology.validation}) are always on, and topology-epoch coordination
     * (the former {@code parsley.coordination.*} keys) was removed — behaviour is strictly more
     * available without it.
     *
     * @param runtimeProps the configuration handed to {@code CausalStreams}
     * @throws IllegalStateException naming every {@code parsley.*} key found
     */
    static void requireNoParsleyKeys(Properties runtimeProps) {
        Set<String> offending = new LinkedHashSet<>();
        collectParsleyKeys(loadClasspathProperties(), offending);
        collectParsleyKeys(runtimeProps, offending);
        if (!offending.isEmpty()) {
            throw new IllegalStateException("removed configuration " + offending + ": Parsley has no "
                    + "configuration keys. The startup topology checks are always on and always fail "
                    + "fast (the former parsley.topology.validation), and topology-epoch coordination "
                    + "was removed from the causal protocol (the former parsley.coordination.* keys — "
                    + "joins need zero coordination). Delete every parsley.* key; no replacement "
                    + "configuration is needed.");
        }
    }

    private static void collectParsleyKeys(Properties props, Set<String> offending) {
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(PARSLEY_KEY_PREFIX)) {
                offending.add(name);
            }
        }
    }

    /** The {@code parsley.properties} classpath resource, or empty if absent. */
    private static Properties loadClasspathProperties() {
        Properties props = new Properties();
        try (InputStream in = ParsleyConfig.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + RESOURCE + " from the classpath", e);
        }
        return props;
    }
}
