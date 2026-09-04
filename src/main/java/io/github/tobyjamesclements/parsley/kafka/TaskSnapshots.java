package io.github.tobyjamesclements.parsley.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.github.tobyjamesclements.parsley.api.TaskStatus;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.Deliverability;
import io.github.tobyjamesclements.parsley.core.ProcessEngine;

/**
 * The one composition of a task's {@link TaskStatus} from its engine (D103), shared by
 * every host: every channel with holds, what its head waits for, and the frontier's size.
 * Taken on the thread that drives the engine; the decision for each head is the one the
 * drain would act on, so the cost is one decision per held channel.
 */
final class TaskSnapshots {
    private TaskSnapshots() {
    }

    static TaskStatus snapshot(ProcessEngine engine, int partition, Function<ChannelId, String> topicName,
                               Optional<Duration> sinceLastFacts) {
        List<TaskStatus.HeldChannel> heldChannels = new ArrayList<>();
        int heldMessages = 0;
        for (ChannelId channel : engine.receivedChannelSet()) {
            int held = engine.heldCount(channel);
            if (held == 0) {
                continue;
            }
            heldMessages += held;
            List<TaskStatus.Blocker> blockers = new ArrayList<>();
            engine.headVerdict(channel).ifPresent(verdict -> {
                if (verdict instanceof Deliverability.Held heldVerdict) {
                    for (Deliverability.Blocker blocker : heldVerdict.blockers()) {
                        blockers.add(new TaskStatus.Blocker(topicName.apply(blocker.channel()),
                                blocker.channel().partition(), blocker.requiredPosition(), blocker.settledPosition()));
                    }
                }
            });
            heldChannels.add(new TaskStatus.HeldChannel(topicName.apply(channel), channel.partition(), held,
                    engine.headPosition(channel).orElseThrow(), blockers));
        }
        return new TaskStatus(partition, engine.frontierSize(), engine.frontierBytes(),
                heldMessages, heldChannels, sinceLastFacts);
    }
}
