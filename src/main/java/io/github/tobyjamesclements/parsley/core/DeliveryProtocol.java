package io.github.tobyjamesclements.parsley.core;

import java.util.Optional;

/**
 * The core protocol surface a host drives. The production implementation is
 * {@link CausalNode}; the simulator hosts it directly, and deliberately broken implementations
 * exist in tests to prove the oracle catches them.
 *
 * <p>Threading: single-threaded, host-driven, synchronous — indications are pulled, because
 * both Kafka Streams processing and the simulator are synchronous.
 */
public interface DeliveryProtocol {

    /**
     * Hands the core one record fetched from a consumed channel, in per-channel offset order.
     * The core seeds and bridges density, enqueues or delivers, and cascades any releases.
     */
    ProcessResult onRecord(InboundRecord record);

    /**
     * The single stamping site: the dependency clock to attach to an outbound send to
     * {@code destination}, with pending acknowledgements folded and the crossing wait applied
     * (quiescence of unacknowledged own sends to channels other than the destination).
     */
    Clock stampForSend(Channel destination);

    /**
     * Told after the user processor handled one delivery. When it forwarded no business
     * record, the returned emission keeps causal progress observable downstream.
     */
    Optional<NullEmission> afterDelivery(Delivery delivery, int businessForwards);

    /** Backpressure signal: the host should pause fetching {@code channel} while true. */
    boolean pauseWanted(Channel channel);

    /**
     * Where the host's consumer should (re)start fetching each consumed channel, computed at
     * init from restored state: one past the frontier for a channel grown into scope (skip what
     * was already ignored), absent for channels the committed consumer position governs.
     */
    java.util.Map<Channel, Long> resumePositions();

    /**
     * Drops clock entries at or below {@code stability} from every stamp-feeding clock. Sound
     * only when every node's frontier already dominates {@code stability}; supplying that bound
     * is the caller's responsibility.
     */
    void truncate(Clock stability);
}
