package io.github.tobyjamesclements.parsley;

import java.util.Set;

/**
 * The default {@link CausalMembershipStrategy}: never excludes a member, so an epoch transition blocks
 * until every member has completed its snapshot, for an unbounded time. Because it removes no one, it can
 * never strand an un-drained buffer — the safe choice that trades reconfiguration liveness for causal
 * safety. See {@link CausalMembershipStrategy#blockUntilDrained()}.
 */
final class ParsleyBlockUntilDrained implements CausalMembershipStrategy {

    static final ParsleyBlockUntilDrained INSTANCE = new ParsleyBlockUntilDrained();

    private ParsleyBlockUntilDrained() {
    }

    @Override
    public Set<String> excludableMembers(BlockedRound round) {
        return Set.of();
    }
}
