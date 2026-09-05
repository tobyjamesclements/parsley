package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Establishes that every consumer the kafka layer builds by hand is pinned against
 * mutating the cluster.
 *
 * <p>The consumer default leaves {@code allow.auto.create.topics} true, so a bare metadata
 * request against a broker with auto-create enabled silently creates the topic it asks
 * about. Kafka Streams pins the config false for every consumer it builds; these are the
 * two consumers Streams does not build for us, and each one's metadata requests touch a
 * topic whose absence is load-bearing: the changelog reader's absence-of-records is the
 * prior-state evidence, and the bootstrap member subscribes to just-resolved received
 * topics. An auto-created empty impostor at either seam converts a refusal into a silent
 * resume (D82). The facts round's probe consumer, the third such consumer, is gone (D115);
 * the identity check at task initialisation uses the admin client alone, which creates
 * nothing.
 */
class ClusterMutationPinningTest {

    /**
     * The ordering-changelog reader must neither create the changelog its metadata request
     * asks about nor silently reposition past records a mid-scan truncation discarded.
     */
    @Test
    void changelogReaderNeverAutoCreatesAndNeverAutoResets() {
        Map<String, Object> props = ParsleyRuntime.changelogReaderProperties(Map.of("bootstrap.servers", "b:9092"));

        assertEquals(false, props.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                "the reader's metadata request must never create the changelog whose record"
                        + " content is the prior-state evidence");
        assertEquals("none", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "a mid-scan out-of-range must fail the scan, not silently truncate the restored view");
        assertEquals("read_committed", props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
                "the scan must not restore records an aborted transaction wrote");
    }

    /** The bootstrap member's subscription must never create a just-deleted received topic. */
    @Test
    void bootstrapMemberNeverAutoCreates() {
        Map<String, Object> props = GroupMembershipCommitter.memberProperties(
                Map.of("bootstrap.servers", "b:9092"), "g");

        assertEquals(false, props.get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG),
                "the member's metadata requests must never create a received topic");
        assertEquals("none", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "the member must never invent a position");
    }

}
