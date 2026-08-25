package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fixtures the start-path unit suites share: the hand-built {@link TopicDescription} the
 * resolution pins script, and the interrupt-refusal discipline the interruption pins
 * repeat — set the flag, assert the loud refusal, verify the flag was restored, and clear
 * it so it cannot leak into whatever test the runner schedules next.
 */
final class StartPathFixtures {

    private StartPathFixtures() {
    }

    /**
     * A scripted single-broker description of {@code name}: {@code partitions} healthy
     * partitions on one node, carrying {@code topicId} as the broker-assigned identity.
     */
    static TopicDescription describedTopic(String name, Uuid topicId, int partitions) {
        Node node = new Node(1, "broker", 9092);
        List<TopicPartitionInfo> infos = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            infos.add(new TopicPartitionInfo(partition, node, List.of(node), List.of(node)));
        }
        return new TopicDescription(name, false, infos, Set.of(), topicId);
    }

    /**
     * Runs {@code start} with the interrupt flag set and asserts the interrupted-refusal
     * contract: the step refuses with an {@link IllegalStateException} whose message
     * carries {@code expectedMessageFragment} — the site's own interrupted diagnosis, not
     * a generic failure — and the flag is left set so a caller shutting the runtime down
     * still sees its own signal. The flag is read-and-cleared in a finally, so neither a
     * pass nor a failure can leak interrupt status into a later test.
     *
     * @return the refusal, for any site-specific assertions on its cause
     */
    static IllegalStateException assertRefusesWhenInterrupted(Executable start,
                                                              String expectedMessageFragment) {
        IllegalStateException refusal;
        boolean interrupted;
        Thread.currentThread().interrupt();
        try {
            refusal = assertThrows(IllegalStateException.class, start,
                    "an interrupted start step must refuse loudly, not swallow the interrupt"
                            + " or press on to a conclusion it never corroborated");
        } finally {
            // read-and-clear so a failure above cannot leak interrupt status into later tests
            interrupted = Thread.interrupted();
        }
        assertTrue(interrupted,
                "the interrupt flag must be restored; swallowing it hides the shutdown signal"
                        + " from the caller");
        assertTrue(refusal.getMessage().contains(expectedMessageFragment),
                "the refusal must name the interruption it stopped for — expected \""
                        + expectedMessageFragment + "\" in: " + refusal.getMessage());
        return refusal;
    }
}
