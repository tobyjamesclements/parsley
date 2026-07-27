package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.Delivery;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;
import io.github.tobyjamesclements.parsley.core.InboundRecord;

import java.util.List;
import java.util.Map;

/**
 * A deliberately broken protocol: delivers every business record the moment it arrives,
 * ignoring dependency clocks entirely (its own stamps honestly claim what it delivered). Exists
 * to prove the oracle catches causal violations — verification obligation V8.
 */
final class EagerProtocol implements DeliveryProtocol {

    private final Clock delivered = new Clock();

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
    public Clock stampForSend(Channel destination) {
        return delivered.copy();
    }

    @Override
    public boolean pauseWanted(Channel channel) {
        return false;
    }

    @Override
    public Map<Channel, Long> resumePositions() {
        return Map.of();
    }

    @Override
    public void truncate(Clock stability) {}
}
