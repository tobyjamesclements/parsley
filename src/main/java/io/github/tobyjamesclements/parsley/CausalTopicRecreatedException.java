package io.github.tobyjamesclements.parsley;

/**
 * Thrown on the record and stamping paths once Parsley's topic-identity watch has detected that
 * a causal topic was deleted (or deleted and recreated) while this member ran. The throw is the
 * point: topic name → UUID identity is bound once per task lifetime at initialisation, so a mid-run
 * recreation would silently rebind causal coordinates — records of the new topic ingested and
 * stamped under the old UUID. Propagating this out of {@code process()} aborts the EOS transaction
 * and fails the member fast.
 *
 * <p>This member can never heal in place: every further record would be mislabelled, so an
 * uncaught-exception handler should not replace the thread. A <em>restart</em> is safe — identity
 * is re-resolved at initialisation, where the recreation degrades to ordinary history loss, never
 * reordering.
 */
public final class CausalTopicRecreatedException extends CausalDeliveryException {

    CausalTopicRecreatedException(String message) {
        super(message, null);
    }
}
