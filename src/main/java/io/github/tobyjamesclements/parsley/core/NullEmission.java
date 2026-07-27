package io.github.tobyjamesclements.parsley.core;

/**
 * An instruction to the host to emit one protocol null message to every declared sink, routed
 * to the task's own partition. The host stamps each send through
 * {@link DeliveryProtocol#stampForSend} like any business record, so a null message's clock and
 * a business record's clock cannot diverge.
 *
 * @param key the triggering record's key bytes (or null)
 * @param timestamp the triggering record's timestamp — never the wall clock, so a reprocessing
 *     run over historic event times cannot yank downstream stream time forward
 */
public record NullEmission(byte[] key, long timestamp) {}
