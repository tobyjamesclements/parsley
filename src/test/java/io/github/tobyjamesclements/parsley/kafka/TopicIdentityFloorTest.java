package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that topic resolution refuses a substrate that cannot provide channel
 * identity.
 *
 * <p>SPEC Substrate 1 sets the broker floor at 3.7.0, and Assumption 2 makes a channel a
 * topic-partition identified so that a recreated topic is a different channel — which
 * needs the broker-assigned topic ID. A pre-topic-ID broker describes every topic with the
 * reserved {@link Uuid#ZERO_UUID}, the one hard tripwire below the floor; adopting it as
 * identity would give every topic the same identity and D83's whole machinery — recreation
 * detection, the zero-id decode refusal — nothing to stand on.
 */
class TopicIdentityFloorTest {

    private static TopicDescription description(Uuid topicId, int partitions) {
        Node node = new Node(1, "broker", 9092);
        List<TopicPartitionInfo> infos = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            infos.add(new TopicPartitionInfo(partition, node, List.of(node), List.of(node)));
        }
        return new TopicDescription("orders", false, infos, Set.of(), topicId);
    }

    /**
     * Catches the floor tripwire being dropped or renamed: a description carrying the
     * reserved zero topic ID means the broker predates topic IDs (below SPEC Substrate
     * 1's 3.7.0 floor), and resolution must refuse with SUBSTRATE_MISCONFIGURED — the
     * reason supervisors key on (D55) — naming the topic and the floor, rather than adopt
     * the zero id as channel identity (Assumption 2, D83).
     */
    @Test
    void zeroTopicIdRefusesToStartAsSubstrateMisconfigured() {
        ParsleyFailClosedException refusal = assertThrows(ParsleyFailClosedException.class,
                () -> ParsleyRuntime.requireTopicId("orders", description(Uuid.ZERO_UUID, 2)),
                "a description carrying the reserved zero topic ID must refuse, not resolve;"
                        + " below the broker floor channel identity does not exist");
        assertEquals(ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED, refusal.reason(),
                "the refusal must carry SUBSTRATE_MISCONFIGURED, the reason supervisors key on");
        assertTrue(refusal.getMessage().contains("orders"),
                "the refusal must name the topic the operator has to look at: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("3.7.0"),
                "the refusal must name the broker floor the substrate is below: " + refusal.getMessage());
    }

    /**
     * Catches the refusal over-reaching: a genuinely assigned topic ID must resolve to a
     * {@link TopicInfo} carrying that identity and the described partition count — the
     * fixed view the process runs against.
     */
    @Test
    void aRealTopicIdResolvesToItsIdentityAndWidth() {
        Uuid id = Uuid.randomUuid();

        TopicInfo info = ParsleyRuntime.requireTopicId("orders", description(id, 3));

        assertEquals(TopicInfo.toJavaUuid(id), info.topicId(),
                "the resolved view must carry the broker-assigned identity unchanged");
        assertEquals(3, info.partitions(),
                "the resolved view must carry the described partition count");
    }
}
