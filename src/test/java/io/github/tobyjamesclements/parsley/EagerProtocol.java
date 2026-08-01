package io.github.tobyjamesclements.parsley;


import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A deliberately broken protocol: delivers every business record the moment it arrives,
 * ignoring dependency clocks entirely (its own stamps honestly claim what it delivered). Exists
 * to prove the oracle catches causal violations, verification obligation V8.
 */
final class EagerProtocol implements DeliveryProtocol {

    private static final UUID SENDER = UUID.nameUUIDFromBytes("eager".getBytes());

    private final VectorClock delivered = new VectorClock();
    private long seq = -1;

    @Override
    public List<Delivery> onRecord(InboundRecord r) {
        delivered.advanceTo(r.channel(), r.offset());
        return List.of(new Delivery(r.channel(), r.offset(), r.key(), r.value(), r.timestamp()));
    }

    @Override
    public List<Delivery> positionAdvance(Channel channel, long position) {
        return List.of();
    }

    @Override
    public SendStamp prepareSend(Channel destination) {
        return new SendStamp(delivered.copy(), SENDER, ++seq);
    }

    @Override
    public Map<Channel, Long> resumePositions() {
        return Map.of();
    }

    @Override
    public java.util.Set<Channel> stampChannels() {
        return java.util.Set.of();
    }

    @Override
    public void truncate(VectorClock stability) {}
}
