package io.github.tobyjamesclements.parsley;

import java.util.UUID;

/**
 * Everything an outbound send must carry: the dependency clock, and the sender tag that lets
 * receivers resolve sequence claims against this record.
 *
 * <p>The tag is assigned synchronously at {@link DeliveryProtocol#prepareSend}, which is what
 * removes the acknowledgement wait from the stamping path.
 *
 * @param clock the dependency clock to stamp onto the record
 * @param senderId the sending node's identity
 * @param senderSeq this send's sequence from that sender, counted per destination channel
 */
record SendStamp(VectorClock clock, UUID senderId, long senderSeq) {}
