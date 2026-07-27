package io.github.tobyjamesclements.parsley.core;

import java.util.List;

/**
 * The outcome of handing one inbound record to the core: zero or more business deliveries in
 * causal order, plus zero or more null-message emissions the host must perform (gossip relays,
 * and seed/bridge advances that produced no business delivery).
 *
 * <p>The host processes deliveries in order, calling
 * {@link DeliveryProtocol#afterDelivery} after each; that call may add further emissions.
 */
public record ProcessResult(List<Delivery> deliveries, List<NullEmission> emissions) {

    public static final ProcessResult EMPTY = new ProcessResult(List.of(), List.of());
}
