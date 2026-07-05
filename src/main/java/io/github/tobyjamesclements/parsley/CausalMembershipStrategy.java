package io.github.tobyjamesclements.parsley;

import java.util.Set;

/**
 * Decides how an epoch transition treats a member that has not yet published its snapshot for the open
 * round. An epoch's new floor is the {@code mergeMin} of the running members' published frontiers; the
 * round cannot commit until every running member has published, so a member that is absent (crashed, or
 * merely slow) holds the transition open. This strategy is consulted while a round is blocked to decide
 * whether any of those members may be <strong>excluded</strong> — removed from the domain so the round
 * commits without them.
 *
 * <p><strong>Safety invariant every implementation must honour:</strong> only a member that is
 * <em>drained</em> may be excluded. Excluding a member that still holds un-drained buffered records
 * strands those records below the new epoch floor, where their real dependencies are stripped and they
 * would be delivered before their causes — a causal-safety violation. A crashed member's buffer is
 * private and durable, so it is never <em>known</em> to be drained from outside; the default strategy
 * therefore excludes no one.
 *
 * <p>The built-in {@link #blockUntilDrained()} never excludes: a blocked round simply waits, for an
 * unbounded time, until every member returns and publishes. Richer algorithms (recovery, buffer handoff,
 * an operator-driven forced exclusion with dead-lettering) plug in here.
 */
interface CausalMembershipStrategy {

    /**
     * Given a round blocked on members that have not yet published, returns the members that may be
     * excluded now. A member may be returned <strong>only if it is drained</strong> (see the class
     * invariant). Returning an empty set keeps the round open — the transition waits until the
     * outstanding members publish.
     *
     * @param round the blocked round: its outstanding (unpublished) members and full running membership
     * @return the subset of members to exclude now; empty to keep waiting
     */
    Set<String> excludableMembers(BlockedRound round);

    /**
     * The strategy that never excludes a member: an epoch transition blocks until every member has
     * completed its snapshot, however long that takes. This is the safe default — it can never strand an
     * un-drained buffer, because it removes no one.
     */
    static CausalMembershipStrategy blockUntilDrained() {
        return ParsleyBlockUntilDrained.INSTANCE;
    }
}
