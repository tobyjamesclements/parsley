package io.github.tobyjamesclements.parsley;

/**
 * Thrown on the record and stamping paths once {@link ParsleyTopicIdentityWatch} has detected that
 * a causal topic was deleted (or deleted and recreated) while this member ran. The throw is the
 * point (T3.0 A13 / E1): topic name → UUID identity is bound once per task lifetime at
 * {@code init()}, so a mid-run recreation would silently rebind causal coordinates — records of
 * the new topic ingested and stamped under the old UUID. Propagating this out of {@code process()}
 * aborts the EOS transaction and fails the member fast; a restart re-resolves identity, where
 * recreation degrades to E2-style history loss, never reordering.
 */
final class ParsleyTopicRecreatedException extends IllegalStateException {

    ParsleyTopicRecreatedException(String message) {
        super(message);
    }
}
