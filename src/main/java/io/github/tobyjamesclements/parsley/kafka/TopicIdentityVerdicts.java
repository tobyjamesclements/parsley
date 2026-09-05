package io.github.tobyjamesclements.parsley.kafka;

import java.util.Set;
import java.util.UUID;

/**
 * What a {@link TopicIdentitySource} concluded about the ids it was asked: the topics
 * confirmed gone — deleted outright, or deleted and recreated under their name so that the
 * id a process knows is a dead incarnation — and the ids whose question the substrate could
 * not answer this time, which the asker keeps pending and asks again.
 *
 * @param deleted    ids whose topic no longer exists under any name
 * @param recreated  ids whose last-known name now resolves to a different id
 * @param unanswered ids the substrate gave no answer about — a timed-out or failed
 *                   corroborating describe — so that nothing was concluded either way
 */
record TopicIdentityVerdicts(Set<UUID> deleted, Set<UUID> recreated, Set<UUID> unanswered) {

    /** Every id asked about still names a live topic, and every question was answered. */
    static final TopicIdentityVerdicts NONE = new TopicIdentityVerdicts(Set.of(), Set.of(), Set.of());

    /** Defensively copies every set. */
    TopicIdentityVerdicts {
        deleted = Set.copyOf(deleted);
        recreated = Set.copyOf(recreated);
        unanswered = Set.copyOf(unanswered);
    }

    TopicIdentityVerdicts(Set<UUID> deleted, Set<UUID> recreated) {
        this(deleted, recreated, Set.of());
    }

    /** Whether every question was answered, whatever the answers were. */
    boolean answered() {
        return unanswered.isEmpty();
    }
}
