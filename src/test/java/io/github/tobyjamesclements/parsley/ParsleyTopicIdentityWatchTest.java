package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyTopicIdentityWatch} — the mid-run E1 enforcement (T3.0 A13): tasks
 * register the topic name → UUID bindings they resolved at init, a poll compares them against the
 * broker's current view, and once a recreation or deletion is detected every
 * {@link ParsleyTopicIdentityWatch#ensureIntact} call fails the caller fast.
 */
class ParsleyTopicIdentityWatchTest {

    private static final Uuid T1_ID = Uuid.randomUuid();
    private static final Uuid T2_ID = Uuid.randomUuid();

    /** A {@link ParsleyTopicAdmin} double resolving per-topic UUIDs or throwing a configured failure. */
    private static final class FixedIdsAdmin implements ParsleyTopicAdmin {
        private final Map<String, Uuid> ids = new HashMap<>();
        private final Map<String, Exception> failures = new HashMap<>();

        FixedIdsAdmin with(String topic, Uuid id) {
            ids.put(topic, id);
            return this;
        }

        FixedIdsAdmin failing(String topic, Exception failure) {
            failures.put(topic, failure);
            return this;
        }

        @Override public Map<String, Uuid> topicIds(List<String> topics) throws Exception {
            Map<String, Uuid> resolved = new HashMap<>();
            for (String topic : topics) {
                Exception failure = failures.get(topic);
                if (failure != null) {
                    throw failure;
                }
                resolved.put(topic, ids.get(topic));
            }
            return resolved;
        }

        @Override public Map<String, Integer> partitionCounts(List<String> topics) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public Map<String, String> cleanupPolicies(List<String> topics) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public Map<Integer, Long> endOffsets(String topic) {
            throw new UnsupportedOperationException("not used by the identity watch");
        }

        @Override public void close() {}
    }

    /**
     * The intact case: every expected topic still resolves to its registered UUID, so the poll
     * finds nothing and {@code ensureIntact} stays a no-op.
     */
    @Test
    void unchangedTopicIdsStayIntactAcrossPolls() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.expect("t2", T2_ID);

        watch.poll(new FixedIdsAdmin().with("t1", T1_ID).with("t2", T2_ID));

        assertDoesNotThrow(watch::ensureIntact,
                "unchanged topic UUIDs must keep the watch intact — the check must stay a no-op");
    }

    /**
     * The A13 core: a topic whose broker-current UUID differs from the init-time resolution was
     * deleted and recreated mid-run; the poll marks the watch broken and every subsequent
     * {@code ensureIntact} throws, naming the topic and both UUIDs.
     */
    @Test
    void aRecreatedTopicBreaksTheWatchAndEnsureIntactThrows() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        Uuid recreated = Uuid.randomUuid();

        watch.poll(new FixedIdsAdmin().with("t1", recreated));

        ParsleyTopicRecreatedException failure =
                assertThrows(ParsleyTopicRecreatedException.class, watch::ensureIntact,
                        "a changed topic UUID must fail every subsequent identity check");
        assertTrue(failure.getMessage().contains("t1"),
                "the failure must name the recreated topic: " + failure.getMessage());
        assertTrue(failure.getMessage().contains(T1_ID.toString())
                        && failure.getMessage().contains(recreated.toString()),
                "the failure must carry both the init-time and the current UUID: " + failure.getMessage());
    }

    /**
     * A topic that no longer exists at all — {@link UnknownTopicOrPartitionException} in the
     * resolution failure chain — is positive evidence of deletion: its records can never arrive
     * again and a recreation would rebind coordinates, so the watch breaks rather than retrying
     * silently forever.
     */
    @Test
    void aDeletedTopicBreaksTheWatch() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);

        watch.poll(new FixedIdsAdmin().failing("t1",
                new ExecutionException(new UnknownTopicOrPartitionException("gone"))));

        assertThrows(ParsleyTopicRecreatedException.class, watch::ensureIntact,
                "a provably deleted causal topic must fail the identity check");
    }

    /**
     * A transient resolution failure (an unreachable broker, a timeout) is never a verdict: the
     * poll logs and retries next tick, and the watch stays intact — only positive evidence of a
     * changed or missing topic may break identity.
     */
    @Test
    void aTransientResolutionFailureNeverBreaksTheWatch() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);

        watch.poll(new FixedIdsAdmin().failing("t1",
                new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("broker away"))));

        assertDoesNotThrow(watch::ensureIntact,
                "a transient admin failure must be retried, never treated as a recreation");
    }

    /**
     * Two tasks of one instance registering different UUIDs for the same topic name is itself
     * proof the topic was recreated between their inits (a recreation racing startup): the watch
     * breaks immediately, without waiting for a poll.
     */
    @Test
    void conflictingInitTimeResolutionsBreakTheWatchImmediately() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.expect("t1", Uuid.randomUuid());

        assertThrows(ParsleyTopicRecreatedException.class, watch::ensureIntact,
                "conflicting init-time resolutions of one topic name must break the watch without a poll");
    }

    /**
     * A poll that finds one broken topic must still leave the first detected detail in place — the
     * failure message reports the first violation, and later polls cannot un-break the watch even
     * if the broker view flaps back.
     */
    @Test
    void aBrokenWatchStaysBrokenAcrossLaterCleanPolls() {
        ParsleyTopicIdentityWatch watch = new ParsleyTopicIdentityWatch();
        watch.expect("t1", T1_ID);
        watch.poll(new FixedIdsAdmin().with("t1", Uuid.randomUuid()));

        watch.poll(new FixedIdsAdmin().with("t1", T1_ID));

        assertThrows(ParsleyTopicRecreatedException.class, watch::ensureIntact,
                "identity, once broken, must stay broken for the process lifetime — a flapping broker "
                        + "view must not silently resume a member that already mislabelled");
    }
}
