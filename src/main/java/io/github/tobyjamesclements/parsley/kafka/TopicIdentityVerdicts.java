package io.github.tobyjamesclements.parsley.kafka;

import java.util.Set;
import java.util.UUID;

/**
 * The topics a {@link TopicIdentitySource} confirmed gone: deleted outright, or deleted and
 * recreated under their name so that the id a process knows is a dead incarnation.
 *
 * @param deleted   ids whose topic no longer exists under any name
 * @param recreated ids whose last-known name now resolves to a different id
 */
record TopicIdentityVerdicts(Set<UUID> deleted, Set<UUID> recreated) {

    /** Every id asked about still names a live topic. */
    static final TopicIdentityVerdicts NONE = new TopicIdentityVerdicts(Set.of(), Set.of());

    /** Defensively copies both sets. */
    TopicIdentityVerdicts {
        deleted = Set.copyOf(deleted);
        recreated = Set.copyOf(recreated);
    }
}
