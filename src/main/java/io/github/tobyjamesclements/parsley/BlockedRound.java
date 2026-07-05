package io.github.tobyjamesclements.parsley;

import java.util.Set;

/**
 * A read-only view of an epoch snapshot round that is blocked waiting for members to publish, passed to
 * {@link CausalMembershipStrategy#excludableMembers(BlockedRound)}.
 *
 * @param unpublishedMembers the running members that have not yet published their frontier for this round
 * @param runningMembers      the full running membership the round is collecting from
 */
record BlockedRound(Set<String> unpublishedMembers, Set<String> runningMembers) {

    BlockedRound {
        unpublishedMembers = Set.copyOf(unpublishedMembers);
        runningMembers = Set.copyOf(runningMembers);
    }
}
