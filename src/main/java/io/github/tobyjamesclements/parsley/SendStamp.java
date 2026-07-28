package io.github.tobyjamesclements.parsley;

import java.util.UUID;

/**
 * Everything an outbound send must carry: the dependency clock, and the sender tag
 * {@code (senderId, senderSeq)} that lets receivers resolve sequence claims against this
 * record. The tag is assigned synchronously at {@link DeliveryProtocol#prepareSend}, which is
 * what removes the acknowledgement wait from the stamping path.
 */
record SendStamp(Clock clock, UUID senderId, long senderSeq) {}
