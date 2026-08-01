package io.github.tobyjamesclements.parsley;

/**
 * One record fetched from a consumed channel, as the host hands it to the protocol.
 *
 * <p>Records on a channel must be handed over in strictly increasing offset order, which is
 * Kafka's per-partition delivery order through a {@code read_committed} consumer.
 *
 * <p>Delivering a tagged record advances this node's delivered-sequence watermark for the
 * channel and sender, which is what resolves sequence claims.
 *
 * @param channel the consumed channel the record arrived on
 * @param offset the record's broker offset
 * @param clock the record's dependency clock header, or null when the header is absent, since
 *     a producer that stamps nothing claims nothing. Undecodable bytes must have already thrown
 * @param senderId the producing node's stable sender identity, or null for untagged producers
 * @param senderSeq the record's per-channel send sequence at its sender, or {@code -1} when
 *     untagged
 * @param key the record key bytes, or null
 * @param value the record value bytes, or null
 * @param timestamp the record timestamp
 */
record InboundRecord(
        Channel channel,
        long offset,
        VectorClock clock,
        java.util.UUID senderId,
        long senderSeq,
        byte[] key,
        byte[] value,
        long timestamp) {

    /**
     * Rejects a negative offset, and a sender identity and sequence that are not both tagged.
     *
     * @throws IllegalArgumentException if {@code offset} is negative, or exactly one of
     *     {@code senderId} and {@code senderSeq} indicates a tagged record
     */
    InboundRecord {
        if (offset < 0) throw new IllegalArgumentException("offset " + offset);
        if ((senderId == null) != (senderSeq < 0)) {
            throw new IllegalArgumentException("senderId and senderSeq must be tagged together");
        }
    }
}
