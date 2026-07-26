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
 * L3, the gossip layer: clock dissemination over the topology's own channels. Its records are null
 * messages in the Chandy–Misra–Bryant sense (a timestamp-carrying record with a null value,
 * occupying a real offset purely to make the sender's clock observable), spread epidemically in the
 * sense of Demers et al. 1987 so knowledge reaches every path, cycles included, and quiesces once
 * everyone has converged. It sits on top of {@link ParsleyCausalBroadcast} as a liveness extension:
 * nothing here ever releases a record the delivery gate would hold, and removing it would cost only
 * progress visibility on non-emitting paths, never ordering.
 *
 * <p>A received null message's own offset is delivered normally, advancing the contiguous frontier
 * that releases held records; its carried clock feeds the channel's advertised view and the stamp
 * only, never the gate. The relay rule is the single home of this decision: relay onward iff
 * the carried clock advanced this node's total knowledge on a channel it consumes,
 * {@code !stamp().dominates(carried.retaining(consumedScope))}, with pending acks folded first. This
 * is the CMB trigger discipline, where only an advance of the node's own input channels obliges a
 * relay. Everything else the clock carries is custody: it folds into the stamp unconditionally
 * and rides every later emission but never obliges a relay, because custody hearsay lags its source
 * by a gossip lap and relaying on it would never quiesce on a cycle. Withholding a relay starves
 * nobody, since a suppressed relay also suppresses the claim it would have stamped.
 *
 * <p>The emission half lives at the call sites in {@code ParsleyProcessor}: a delivery whose delegate
 * forwarded nothing, a held record whose receipt advanced completeness, and a received null message
 * that advanced a consumed channel each emit an {@link #advertise} record, so downstream channel
 * clocks advance gap-free on every path.
 *
 * @param <K> the record key type of the input channels
 * @param <V> the record value type of the input channels
 */
final class ParsleyGossip<K, V> {

    private final ParsleyChannels channels;
    private final ParsleyCausalBroadcast<K, V> broadcast;
    // Every declared sink at this task's own partition — a null message's exact destination set
    // (ParsleyMarkerPartition routes it there), excluded from the stamp's crossing wait:
    // same-coordinate pending sends are covered by partition FIFO and per-producer stamp
    // monotonicity, and the cross-sink exemption is safe only because a null message's destination
    // set is known exactly at stamp time. Business forwards never get an exclusion
    // (their destination partition is unknowable at stamp time; see ParsleyCausalBroadcast#broadcast).
    private final Set<TopicPartition> destinations;
    // The relay trigger scope: this node's declared input topics at its own task partition — the
    // coordinates whose first-hand state catches up to every appended offset (see the class
    // Javadoc). Never a filter on what folds (the merge is unconditional), only on what obliges
    // a relay. The own-partition restriction is load-bearing in production: a foreign partition
    // of a consumed topic is a sibling task's scope, never fetched here, so its coverage would
    // lag forever and re-open the storm on any cycle crossing a multi-partition topic.
    private final ParsleyVectorClock.CoordinatePredicate consumedScope;

    /**
     * @param broadcast     the L2 core this layer extends — supplies the release cascade for a null
     *                      message's own delivered offset, the reflected-claim diagnostic, the
     *                      single stamping site {@link ParsleyCausalBroadcast#broadcast}, and (via
     *                      {@link ParsleyCausalBroadcast#channels()}) the L1 module: the
     *                      total-knowledge clock ({@code stamp()}) the relay rule compares
     *                      against, and the frontier/channel state a received null message's offset
     *                      and carried clock fold into
     * @param destinations  every declared sink at this task's own partition — a null message's exact
     *                      destination set, excluded from its stamp's crossing wait (see the field)
     * @param consumedScope this node's consumed coordinates — declared input topics at the task's
     *                      own partition — the only coordinates whose carried-clock advance obliges
     *                      a relay (see the field and the class Javadoc)
     */
    ParsleyGossip(ParsleyCausalBroadcast<K, V> broadcast, Set<TopicPartition> destinations,
                  ParsleyVectorClock.CoordinatePredicate consumedScope) {
        this.channels = broadcast.channels();
        this.broadcast = broadcast;
        this.destinations = Set.copyOf(destinations);
        this.consumedScope = consumedScope;
    }

    /**
     * The per-receive result: every record the null message's own offset released for delivery, in
     * order, plus whether its carried clock advanced this node's total knowledge on a channel it
     * consumes — the relay signal (see the class Javadoc; the caller relays a downstream null
     * message only when it is {@code true}). The name is the CMB input-channel-clock advance;
     * custody-only news (a claim on a channel outside the consumed scope) folds into the stamp but
     * leaves this {@code false}.
     *
     * @param <K> the record key type
     * @param <V> the record value type
     */
    record Reception<K, V>(List<ParsleyMessage<K, V>> delivered, boolean advancedConsumedChannel) {
    }

    /**
     * The receive request: folds one received null message into this node's state, doing two
     * independent things. It always delivers the null message's own {@code (channelId, partition,
     * offset)} into its channel's contiguous frontier, exactly like a business record (seed/bridge,
     * {@link ParsleyChannels#delivered}, release cascade), so a channel carrying only null messages
     * still advances and the gap-free walk does not stall below it. Separately, it folds the carried
     * clock into the channel's advertised view, the outbound-stamp input (the whole clock), never
     * the gate: a peer's claim that a coordinate was delivered there is not proof it was delivered
     * here, so releases on this path come only from the null message's own offset advancing the
     * frontier. The relay comparison is taken between the two, after the producer-ack fold and
     * before the carried clock folds.
     *
     * @param channelId the topic UUID of the null message's source channel
     * @param partition the partition of the null message's source channel
     * @param offset    the null message's own offset on its source channel
     * @param carried   the completeness clock the null message carried (empty when the header was
     *                  absent; an undecodable header fails the task upstream, before this call)
     * @return the records released in the process, plus the relay signal
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
        // The relay comparison, taken BEFORE the carried clock is folded below (afterwards it is
        // dominated by construction). Fold pending acks first so ownOutputs is current — a carried
        // clock reflecting this node's own recent output must read as already known. Only the
        // carried clock's consumed-scope coordinates can oblige a relay; the whole clock still
        // folds below — see the class Javadoc for why hearsay must not oblige.
        channels.foldAcknowledgedOutputs();
        boolean advancedConsumedChannel = !channels.stamp().dominates(carried.retaining(consumedScope));
        channels.channelUpdate(channelId, partition, carried);
        channels.delivered(channelId, partition, offset);
        broadcast.propagate(out, channelId, partition);

        return new Reception<>(out, advancedConsumedChannel);
    }

    /**
     * The advertise indication: builds this node's null message, a record with a null value marked by
     * {@link ParsleyHeader#NULL_MESSAGE} and stamped by the single stamping site
     * ({@link ParsleyCausalBroadcast#broadcast}, so its clock cannot diverge from a business record's),
     * ready for the caller to forward to every sink.
     *
     * <p>{@code key} is the triggering record's key, informational wire content only, since
     * {@link ParsleyMarkerPartition} routes the message to this task's own partition regardless of key.
     * {@code timestamp} is the triggering record's timestamp, never the wall clock: Kafka Streams
     * advances downstream stream time from every polled record's timestamp, so a wall-clock stamp
     * during a reprocessing run would yank downstream windows and grace periods to now. A sink segment
     * of only null messages then looks old to retention exactly when its triggers are old (a backfill),
     * so retention on causal topics must cover the backfill depth; an
     * undersized retention fails safely into {@code AutoOffsetReset.none()}'s loud stall.
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
