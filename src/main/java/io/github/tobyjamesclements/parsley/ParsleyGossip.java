package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.api.Record;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * <strong>L3 — gossip: clock dissemination over the topology's own channels.</strong> Epidemic
 * dissemination in the sense of Demers et al. 1987 ("Epidemic Algorithms for Replicated Database
 * Maintenance"): each node relays causal progress onward only while it is news, so knowledge spreads
 * across every path — cycles included — and quiesces when everyone has converged. Its records are
 * <em>null messages</em> in the Chandy–Misra–Bryant sense (Chandy–Misra 1979, Bryant 1977): a
 * timestamp-carrying record whose value is literally null, occupying a real offset on its channel
 * purely to make the sender's clock observable downstream. Presented in the CGR module style
 * ({@code package-info} states the Parsley-wide deviations):
 *
 * <pre>
 * requests:    receive(channel, offset, carried VT)     the null message's own offset is delivered
 *                → (deliveries, learnedSomethingNew)    via L1/L2 (it advances the contiguous
 *                                                       frontier — the only thing that can release
 *                                                       held records); its carried clock feeds the
 *                                                       channel's advertised view and the stamp
 *                                                       only, never the gate
 * indications: advertise(key, timestamp) → null message a stamped, ready-to-forward record, emitted
 *                                                       when a delivery produced no business output
 *                                                       (or a received null message carried news)
 * relay rule:  relay iff carried VT ⊀ known()           I6 = rumor mongering; known() =
 *                                                       frontier ∪ channel clocks ∪ carried
 *                                                       ancestry ∪ ownOutputs (ParsleyChannels.stamp)
 * properties:  I6 (relay on strict advance); liveness of completeness propagation
 * </pre>
 *
 * <p>This module sits on top of {@link ParsleyCausalBroadcast} as a liveness layer — a protocol
 * extension, not part of the CBCAST core: nothing here ever releases a record the L2 gate would
 * hold (a peer's carried claim is not local delivery), and removing the layer entirely would cost
 * only progress visibility on non-emitting paths, never ordering.
 *
 * <p><strong>The I6 relay rule, stated once (the single home of this decision):</strong> a received
 * null message is relayed onward iff its carried clock taught this node something outside its
 * <em>total knowledge</em> — {@code !ParsleyChannels.stamp().dominates(carried)}, taken before the
 * carried clock is folded (afterwards it is dominated by construction), with pending producer acks
 * folded first so the {@code ownOutputs} side of the comparison is current. "New" is never judged
 * against a single channel's clock: a reflected own coordinate (a downstream stamp echoing this
 * node's own produced position around a cycle) is dominated by {@code ownOutputs} and so teaches
 * nothing — the relay settles without the historical own-sink strip that erased real ancestors
 * (#22). A null message's own delivery is never itself a reason to relay: only genuinely new
 * knowledge is, so each relay strictly shrinks the set of unknown facts and any cycle quiesces —
 * the convergence argument for emulating broadcast on a graph with cycles.
 *
 * <p>The <em>emission</em> half of the protocol lives at the call sites in {@code ParsleyProcessor}
 * (the transport — {@code context.forward} plus {@link ParsleyMarkerPartition} routing — is Kafka
 * Streams glue, exactly as L2's underlying send is Kafka's produce): a delivery whose delegate
 * forwarded no business record, a held record whose receipt still advanced completeness, and a
 * received null message that carried news each emit {@link #advertise}'s record, so downstream
 * channel clocks advance gap-free on every path.
 *
 * @param <K> the record key type of the input channels (L2's deliveries)
 * @param <V> the record value type of the input channels
 */
final class ParsleyGossip<K, V> {

    private final ParsleyChannels channels;
    private final ParsleyCausalBroadcast<K, V> broadcast;
    // Every declared sink at this task's own partition — a null message's exact destination set
    // (ParsleyMarkerPartition routes it there), excluded from the stamp's crossing wait:
    // same-coordinate pending sends are covered by partition FIFO + I3, and the cross-sink
    // exemption is O4's recorded null-message exemption. Business forwards never get an exclusion
    // (their destination partition is unknowable at stamp time; see ParsleyCausalBroadcast#broadcast).
    private final Set<TopicPartition> destinations;

    /**
     * @param channels     the L1 module — the total-knowledge clock ({@code stamp()}) the I6 relay
     *                     rule compares against, and the frontier/channel state a received null
     *                     message's offset and carried clock fold into
     * @param broadcast    the L2 core this layer extends — supplies the release cascade for a null
     *                     message's own delivered offset, the reflected-claim diagnostic, and the
     *                     single stamping site {@link ParsleyCausalBroadcast#broadcast}
     * @param destinations every declared sink at this task's own partition — a null message's exact
     *                     destination set, excluded from its stamp's crossing wait (see the field)
     */
    ParsleyGossip(ParsleyChannels channels, ParsleyCausalBroadcast<K, V> broadcast,
                  Set<TopicPartition> destinations) {
        this.channels = channels;
        this.broadcast = broadcast;
        this.destinations = Set.copyOf(destinations);
    }

    /**
     * The per-receive result: every record the null message's own offset released for delivery, in
     * order, plus whether its carried clock genuinely taught this node something outside its total
     * knowledge — the I6 relay signal (see the class Javadoc; the caller relays a downstream null
     * message only when it is {@code true}).
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record Reception<K, V>(List<ParsleyMessage<K, V>> delivered, boolean learnedSomethingNew) {
    }

    /**
     * The gossip <em>receive</em> request: folds one received null message into this node's state.
     * It does two independent things, and the distinction is the crux of correctness here:
     * <ol>
     *   <li><strong>Always</strong> delivers the null message's own {@code (channelId, partition,
     *       offset)} into its channel's contiguous frontier — exactly like a business record's own
     *       coordinate ({@code seed/bridge} then {@link ParsleyChannels#delivered} then the release
     *       cascade) — so a channel carrying only null messages (a non-emitting path) still
     *       advances. A null message occupies a real offset on its partition, so the frontier's
     *       gap-free absorb walk must count it or it stalls below the message forever, stranding
     *       every later record on that channel.</li>
     *   <li>Folds the carried clock into the channel's advertised view — the outbound-stamp input
     *       (I9: the whole clock, never stripped), <strong>never the gate</strong>. A peer's claim
     *       that a coordinate was delivered <em>there</em> is not proof it was delivered
     *       <em>here</em>; releases on this path come only from the null message's own offset
     *       advancing its channel's frontier. Gating on the max-merged completeness here used to
     *       let a null message claiming a sibling channel's coordinate release a held record before
     *       this node had itself delivered that cause — an effect-before-cause delivery to the
     *       delegate.</li>
     * </ol>
     *
     * <p>The I6 comparison is taken between the two — after the producer-ack fold (so a carried
     * clock reflecting this node's own recent output reads as already known), before the carried
     * clock folds (afterwards it is dominated by construction).
     *
     * @param channelId the topic UUID of the null message's source channel
     * @param partition the partition of the null message's source channel
     * @param offset    the null message's own offset on its source channel
     * @param carried   the completeness clock the null message carried (empty when the header was
     *                  absent — an undecodable header fails the task upstream, before this call)
     * @return the records released in the process, plus the I6 relay signal
     */
    Reception<K, V> receive(Uuid channelId, int partition, long offset, ParsleyVectorClock carried) {
        List<ParsleyMessage<K, V>> out = new ArrayList<>();

        // A null message's own channel is transactional too (Parsley forwards it under EOS), so it
        // carries the same commit-marker holes; the L1 receive request seeds and bridges them before
        // the message's own offset is delivered.
        if (channels.receive(channelId, partition, offset)) {
            broadcast.propagate(out, channelId, partition);
        }

        broadcast.recordReflectedClaims(carried);
        // The I6 comparison, taken BEFORE the carried clock is folded below (afterwards it is
        // dominated by construction). Fold pending acks first so ownOutputs is current — a carried
        // clock reflecting this node's own recent output must read as already known.
        channels.foldAcknowledgedOutputs();
        boolean learnedSomethingNew = !channels.stamp().dominates(carried);
        channels.channelUpdate(channelId, partition, carried);
        channels.delivered(channelId, partition, offset);
        broadcast.propagate(out, channelId, partition);

        return new Reception<>(out, learnedSomethingNew);
    }

    /**
     * The gossip <em>advertise</em> indication: builds this node's null message — a record with a
     * null value, marked by the {@link ParsleyHeader#NULL_MESSAGE} header, stamped with the current
     * outbound vector timestamp by the single stamping site
     * ({@link ParsleyCausalBroadcast#broadcast}, so a null message's clock and a business record's
     * clock cannot diverge by construction) — ready for the caller to forward to every sink.
     *
     * <p>{@code key} is the triggering record's key, carried through as informational wire content,
     * not for routing: {@link ParsleyMarkerPartition} (set by the caller's forward path) routes the
     * message to this task's own owned partition regardless of key, including when it is
     * {@code null}. {@code timestamp} is the <em>triggering record's</em> timestamp, never the wall
     * clock: a null message's timestamp carries no causal meaning (only its headers do), but Kafka
     * Streams advances downstream stream time from every polled record's timestamp before the
     * record is classified, so a wall-clock stamp emitted during a reprocessing run over historic
     * event times would yank downstream delegates' windows, grace periods, and suppressions to
     * now. Under trigger timestamps, downstream stream time advances only as the data's time does.
     * The retention trade this makes: a sink segment holding only null messages looks old to
     * broker time-based retention exactly when its triggers are old — a backfill — and during a
     * backfill the business outputs on the same sink carry the same old timestamps, so retention
     * on causal topics must already cover the backfill depth (E2's retention-sizing constraint,
     * restated, not a new one). An undersized retention then fails in the safe direction: expired
     * null messages below a lagging consumer's position hit {@code AutoOffsetReset.none()}'s loud
     * stall, where a wall-clock stamp silently corrupted downstream event-time results. The
     * stamp's crossing wait excludes exactly this task's own sink partitions (see
     * {@link #destinations}).
     *
     * @param key       the triggering record's key, or {@code null} when none has been observed
     * @param timestamp the triggering record's timestamp
     * @param <KOut>    the outbound key type
     * @param <VOut>    the outbound value type
     * @return the stamped null message, ready to forward
     */
    @SuppressWarnings("NullAway") // null value by design: a null message carries no business payload
    <KOut, VOut> Record<KOut, VOut> advertise(@Nullable KOut key, long timestamp) {
        Headers headers = ParsleyHeader.mutableHeaders();
        headers.add(ParsleyHeader.NULL_MESSAGE, new byte[0]);
        return broadcast.broadcast(new Record<>(key, null, timestamp, headers), destinations);
    }
}
