package io.github.tobyjamesclements.parsley;

import java.util.List;

/**
 * The protocol surface a host drives.
 *
 * <p>The production implementation is {@link CausalNode}. The simulator hosts it directly, and
 * deliberately broken implementations exist in tests to prove the oracle catches them.
 *
 * <p>Single-threaded, host-driven and synchronous. Indications are pulled, because both Kafka
 * Streams processing and the simulator are synchronous.
 */
interface DeliveryProtocol {

    /**
     * Hands the protocol one record fetched from a consumed channel, in per-channel offset
     * order.
     *
     * <p>The protocol seeds and bridges density, enqueues or delivers, and cascades any
     * releases.
     *
     * @param record the fetched record, on a consumed channel
     * @return the records released, in causal delivery order, possibly empty
     */
    List<Delivery> onRecord(InboundRecord record);

    /**
     * Tells the protocol the host consumer's position on {@code channel} advanced to
     * {@code position} without returning records.
     *
     * <p>Everything below the position is fetched or consumer-skipped, the skips being
     * transaction markers and aborted records. This is the liveness signal that bridges
     * trailing markers, the one skip no later-fetched record reveals.
     *
     * @param channel the consumed channel whose position advanced
     * @param position the new consumer position
     * @return the records this releases, in causal delivery order, possibly empty
     */
    List<Delivery> positionAdvance(Channel channel, long position);

    /**
     * The single stamping site, returning the clock and sender tag to attach to an outbound
     * send.
     *
     * <p>Never blocks. The send's own sequence is assigned synchronously, prior sends are
     * claimed in sequence space until their acknowledgements arrive, and acknowledged sends
     * are claimed as offsets.
     *
     * @param destination the concrete channel the record will be appended to, since sequence
     *         claims are per channel and so the host partitions before stamping
     * @return the clock and sender tag to stamp onto the record
     */
    SendStamp prepareSend(Channel destination);

    /**
     * Returns where the host's consumer should restart fetching each consumed channel,
     * computed at init from restored state.
     *
     * <p>An entry is one past the frontier for a channel grown into scope, which skips what was
     * already ignored. A channel is absent when the committed consumer position governs it.
     *
     * @return the resume position of each channel that needs one
     */
    java.util.Map<Channel, Long> resumePositions();

    /**
     * Drops clock entries at or below {@code stability} from every stamp-feeding clock.
     *
     * <p>Sound only when the bound is truly globally stable, meaning no present or future
     * consumer's gate can still need the dropped entries. The one coordination-free source of
     * such a bound is the log-start offset. Records deleted by retention are below every
     * reachable baseline, so {@code logStart - 1} per channel qualifies unconditionally, as
     * does everything for a channel whose topic no longer exists.
     *
     * @param stability the globally stable bound to truncate at or below
     */
    void truncate(VectorClock stability);

    /**
     * Returns the channels currently appearing in the stamp-side clocks, being the carried
     * ancestry and the per-channel advertised views.
     *
     * <p>This is the footprint a truncation driver queries log-start offsets for. It shrinks as
     * truncation removes entries.
     *
     * @return the channels the stamp-side clocks still mention
     */
    java.util.Set<Channel> stampChannels();
}
