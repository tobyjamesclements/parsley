package io.github.tobyjamesclements.parsley.core;

import java.util.List;

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
     * Returned deliveries are in causal delivery order.
     */
    List<Delivery> onRecord(InboundRecord record);

    /**
     * Tells the core the host consumer's position on {@code channel} advanced to
     * {@code position} without returning records: everything below is fetched or
     * consumer-skipped (transaction markers, aborted records). This is the liveness signal that
     * bridges trailing markers — the only thing the original architecture needed a gossip
     * layer of in-band null messages for. May release held records, so it returns deliveries.
     */
    List<Delivery> positionAdvance(Channel channel, long position);

    /**
     * The single stamping site: the clock and sender tag to attach to an outbound send to
     * {@code destination}. The destination must be the concrete channel the record will be
     * appended to — sequence claims are per channel, so the host partitions before stamping.
     * Never blocks: the send's own sequence is assigned synchronously, prior sends are claimed
     * in sequence space until their acknowledgements arrive, and acknowledged sends are
     * claimed as offsets.
     */
    SendStamp prepareSend(Channel destination);

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
