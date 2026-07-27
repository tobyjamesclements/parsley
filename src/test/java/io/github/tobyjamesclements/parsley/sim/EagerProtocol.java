package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;
import io.github.tobyjamesclements.parsley.core.Delivery;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;
import io.github.tobyjamesclements.parsley.core.InboundRecord;
import io.github.tobyjamesclements.parsley.core.NullEmission;
import io.github.tobyjamesclements.parsley.core.ProcessResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A deliberately broken protocol: delivers every business record the moment it arrives,
 * ignoring dependency clocks entirely (its own stamps honestly claim what it delivered). Exists
 * to prove the oracle catches causal violations — verification obligation V8.
 */
final class EagerProtocol implements DeliveryProtocol {

    private final Clock delivered = new Clock();

    @Override
    public ProcessResult onRecord(InboundRecord r) {
        delivered.advanceTo(r.channel(), r.offset());
        if (r.nullMessage()) return ProcessResult.EMPTY;
        return new ProcessResult(
                List.of(new Delivery(r.channel(), r.offset(), r.key(), r.value(), r.timestamp())),
                List.of());
    }

    @Override
    public Clock stampForSend(Channel destination) {
        return delivered.copy();
    }

    @Override
    public Optional<NullEmission> afterDelivery(Delivery delivery, int businessForwards) {
        return Optional.empty();
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
