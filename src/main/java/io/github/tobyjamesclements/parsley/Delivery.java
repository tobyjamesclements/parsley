package io.github.tobyjamesclements.parsley;

/**
 * One business record released by the gate, in causal delivery order, ready for the user's
 * logic.
 *
 * @param channel the channel the record was consumed from
 * @param offset its broker offset
 * @param key record key bytes, or null
 * @param value record value bytes, or null
 * @param timestamp the record timestamp
 */
record Delivery(Channel channel, long offset, byte[] key, byte[] value, long timestamp) {}
