package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The mid-run enforcement of E1, stable channel identity. Topic name → UUID is bound once per task
 * lifetime at {@code init()}; if a causal topic is deleted and recreated while a member runs, its
 * records would be ingested under the old UUID, the coordinate rebinding E1 exists to prevent. Since
 * the Streams API exposes no per-record identity signal, {@link CausalStreams} polls the broker's
 * current topic IDs on a fixed interval and every task checks {@link #ensureIntact} before ingesting
 * or stamping; once a recreation is detected the member fails fast and stays down until an operator
 * intervenes (a restart re-resolves identity, where recreation degrades to history loss, never
 * reordering).
 *
 * <p>The detection window is the poll interval: records fetched between the recreation and the next
 * poll may be mislabelled, so recreating a causal topic under a running member remains an operational
 * constraint. One watch exists per {@link CausalStreams} instance, registered JVM-wide under a minted
 * id carried to every task through the {@code producer.} config prefix; tasks register their init-time
 * bindings via {@link #expect}, and the poll compares the broker's view against every expectation. A
 * run without a {@code CausalStreams} instance has no watch and every check is a no-op.
 */
final class ParsleyTopicIdentityWatch {

    private static final Logger log = LoggerFactory.getLogger(ParsleyTopicIdentityWatch.class);

    /**
     * The config key carrying the watch id — injected by {@link CausalStreams} with the
     * {@code producer.} prefix (so it reaches each task's {@code appConfigs()} without touching
     * any {@code *.internals.*} type), read prefixed by {@code ParsleyProcessor}. The
     * double-underscore shape marks it internal wiring, not user configuration.
     */
    static final String CONFIG_KEY = "__parsley.topic-identity.watch__";

    /** Live watches by minted id; registered at {@link CausalStreams} construction, removed at close. */
    // Package-private (not private) purely so tests can observe registration leaks by size.
    static final ConcurrentHashMap<String, ParsleyTopicIdentityWatch> WATCHES = new ConcurrentHashMap<>();

    /** Every name → UUID binding some task of this instance resolved at its init. */
    private final ConcurrentHashMap<String, Uuid> expected = new ConcurrentHashMap<>();
    /** The first detected violation, as a human-readable detail; null while identity is intact. */
    private volatile @Nullable String broken;

    /** Registers {@code watch} under {@code id} for {@link #lookup} from task config. */
    static void register(String id, ParsleyTopicIdentityWatch watch) {
        WATCHES.put(id, watch);
    }

    /** The watch registered under {@code id}, or {@code null} (e.g. a TopologyTestDriver run). */
    static @Nullable ParsleyTopicIdentityWatch lookup(String id) {
        return WATCHES.get(id);
    }

    /** Removes the {@code id} binding; the closing {@link CausalStreams} instance owns this call. */
    static void unregister(String id) {
        WATCHES.remove(id);
    }

    /**
     * Registers the binding a task resolved at init: {@code topic} is expected to keep {@code id}
     * for the life of this instance. A conflicting registration — another task of this same
     * instance resolved a different UUID for the same name — is itself proof the topic was
     * recreated between the two inits, and marks the watch broken immediately.
     */
    void expect(String topic, Uuid id) {
        Uuid previous = expected.putIfAbsent(topic, id);
        if (previous != null && !previous.equals(id)) {
            markBroken("causal topic '" + topic + "' resolved to UUID " + id + " at one task's init "
                    + "but " + previous + " at another's — it was deleted and recreated during startup");
        }
    }

    /**
     * Compares the broker's current topic IDs against every registered expectation, one topic at a
     * time (a batched describe fails wholesale on one missing topic, which would mask the others).
     * A changed UUID or a provably missing topic ({@link UnknownTopicOrPartitionException} in the
     * failure chain — the topic was deleted; its records can never arrive again, and a recreation
     * would rebind coordinates) marks the watch broken. Any other resolution failure (an
     * unreachable broker) is transient: logged and skipped, never a verdict — only positive
     * evidence breaks identity.
     */
    void poll(ParsleyTopicAdmin admin) {
        for (Map.Entry<String, Uuid> expectation : expected.entrySet()) {
            String topic = expectation.getKey();
            Uuid current;
            try {
                current = admin.topicIds(List.of(topic)).get(topic);
            } catch (Exception e) {
                if (isUnknownTopic(e)) {
                    markBroken("causal topic '" + topic + "' (UUID " + expectation.getValue()
                            + " at init) no longer exists — it was deleted while this member ran");
                    continue;
                }
                log.warn("Topic-identity poll could not resolve '{}' (transient; will retry)", topic, e);
                continue;
            }
            if (!expectation.getValue().equals(current)) {
                markBroken("causal topic '" + topic + "' changed UUID from " + expectation.getValue()
                        + " (resolved at init) to " + current
                        + " — it was deleted and recreated while this member ran");
            }
        }
    }

    /**
     * Throws {@link CausalTopicRecreatedException} if any violation has been detected — called by
     * every task before ingesting a record and before stamping a forward, so the member fails fast
     * (the EOS transaction aborts with it) rather than continuing to mislabel coordinates. A no-op
     * while identity is intact: one volatile read on the hot path.
     */
    void ensureIntact() {
        String detail = broken;
        if (detail != null) {
            throw new CausalTopicRecreatedException(detail + ". Channel identity is bound per "
                    + "process lifetime (E1): records of a recreated topic would be ingested and "
                    + "stamped under the old UUID, rebinding causal coordinates. Failing fast; "
                    + "restarting re-resolves identity (the recreated topic's lost history is an "
                    + "E2-style loss, never a reorder). Do not delete and recreate causal topics "
                    + "while members are running.");
        }
    }

    private void markBroken(String detail) {
        if (broken == null) {
            broken = detail;
            log.error("Topic identity broken: {}", detail);
        }
    }

    private static boolean isUnknownTopic(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof UnknownTopicOrPartitionException) {
                return true;
            }
        }
        return false;
    }
}
