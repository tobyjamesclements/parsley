package io.github.tobyjamesclements.parsley.kafka;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The one scripted {@link TopicIdentitySource} the wiring and revival pins share: answers
 * the scripted verdicts, or fails with the scripted exception, and records what each
 * initialisation asked, in order.
 */
final class ScriptedTopicIdentity implements TopicIdentitySource {
    volatile TopicIdentityVerdicts verdicts = TopicIdentityVerdicts.NONE;
    volatile Exception failure;
    /** The ids each ask named, in order. */
    final List<Set<UUID>> asked = new CopyOnWriteArrayList<>();

    @Override
    public TopicIdentityVerdicts resolve(Set<UUID> topicIds) throws Exception {
        asked.add(Set.copyOf(topicIds));
        if (failure != null) {
            throw failure;
        }
        return verdicts;
    }
}
