package io.github.tobyjamesclements.parsley.api;

import java.util.Optional;

import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * One process's operational state (SPEC Operational 1): whether it has stopped delivering and why, sufficient for
 * an operator to distinguish a deliberate refusal — which recurs identically on restart until acted on — from a
 * transient failure.
 *
 * @param process       the declared process name
 * @param state         the host application's lifecycle state
 * @param refusalReason present when the process failed closed deliberately; the reason names the condition
 *                      (SPEC Operational 6) and recurs identically on restart
 * @param failureDetail the failure's message, refusal or not, for the operator's eyes
 */
public record ProcessStatus(
        String process,
        State state,
        Optional<ParsleyFailClosedException.Reason> refusalReason,
        Optional<String> failureDetail) {

    public enum State { RUNNING, REBALANCING, STOPPED }

    /** True when the stop is a deliberate refusal: restarting without operator action will refuse again. */
    public boolean stoppedDeliberately() {
        return refusalReason.isPresent();
    }
}
