package io.github.tobyjamesclements.parsley.core;

/**
 * One record fetched from a consumed channel, as the host hands it to the core. Records on a
 * channel must be handed over in strictly increasing offset order (Kafka's per-partition
 * delivery order through a {@code read_committed} consumer).
 *
 * @param channel the consumed channel the record arrived on
 * @param offset the record's broker offset
 * @param clock the record's dependency clock header, or null when the header is absent (a
 *     producer that stamps nothing claims nothing); undecodable bytes must have already thrown
 * @param senderId the producing node's stable sender identity, or null for untagged producers
 * @param senderSeq the record's per-channel send sequence at its sender, or {@code -1} when
 *     untagged; delivering a tagged record advances this node's delivered-sequence watermark
 *     for {@code (channel, senderId)}, which is what resolves sequence claims
 * @param key the record key bytes, or null
 * @param value the record value bytes, or null
 * @param timestamp the record timestamp
 */
public record InboundRecord(
        Channel channel,
        long offset,
        Clock clock,
        java.util.UUID senderId,
        long senderSeq,
        byte[] key,
        byte[] value,
        long timestamp) {

    public InboundRecord {
        if (offset < 0) throw new IllegalArgumentException("offset " + offset);
        if ((senderId == null) != (senderSeq < 0)) {
            throw new IllegalArgumentException("senderId and senderSeq must be tagged together");
        }
    }
}
