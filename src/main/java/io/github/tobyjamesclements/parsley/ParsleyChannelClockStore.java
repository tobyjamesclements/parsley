package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.List;

/**
 * A durable per-channel clock: for every input channel {@code (topicId, partition)} this node
 * consumes, tracks the latest {@link ParsleyClock} advertised on that channel (advanced
 * monotonically by pairwise-maximum so that the most-recent record's stamp always dominates).
 *
 * <p>The per-channel max is combined via {@link ParsleyClock#mergeMin} across channels to produce
 * the node's {@link ParsleyEngine#completeness() completeness frontier}: the boundary in logical
 * time past which every input branch has proven it has delivered. Keeping the per-channel clocks
 * separately (rather than a single running min over every record) is essential for liveness —
 * a running min would pin at the first/smallest value ever seen and never recover; a per-channel
 * max-then-min rises as each slow branch catches up.
 *
 * <p>The channel clock for {@code (T, p)} is updated only when a record from that channel is
 * actually <em>delivered</em> (forwarded to the delegate), not when it is first received or
 * buffered. Watermarks are treated as delivered immediately on receipt. This prevents a buffered
 * record's own deps from inflating the channel clock before the record is causal-order-safe.
 *
 * <p>Implementations must be backed by a changelog-replicated state store so per-channel clocks
 * survive restarts. {@link RocksChannelClockStore} provides the production implementation.
 */
interface ParsleyChannelClockStore {

    /**
     * Updates the channel clock for {@code (topicId, partition)} by max-merging {@code clock}
     * into the stored value (monotonic: the stored clock never decreases). A first call for a
     * channel that has never been seen initialises it from {@code clock}.
     *
     * @param topicId   the source topic UUID
     * @param partition the source partition
     * @param clock     the latest frontier advertised by this channel
     */
    void update(Uuid topicId, int partition, ParsleyClock clock);

    /**
     * Returns the current clock for {@code (topicId, partition)}, or {@link ParsleyClock#empty()}
     * if the channel has never been updated.
     *
     * @param topicId   the source topic UUID
     * @param partition the source partition
     * @return the current channel clock; never {@code null}
     */
    ParsleyClock get(Uuid topicId, int partition);

    /**
     * Removes any stored entry for {@code (topicId, partition)}, so a stale channel no longer
     * contributes to the completeness min. Called during {@code init} to prune coordinates whose
     * topic UUID is no longer in scope (e.g. after a topic was deleted and recreated with a new
     * UUID).
     *
     * @param topicId   the stale topic UUID
     * @param partition the stale partition
     */
    void remove(Uuid topicId, int partition);

    /**
     * Returns all stored {@code (topicId, partition, clock)} entries, for completeness
     * computation and for iterating during a prune pass. Order is not specified.
     *
     * @return all channel entries; never {@code null}; may be empty
     */
    List<ChannelEntry> allEntries();

    /**
     * A stored per-channel clock entry.
     */
    record ChannelEntry(Uuid topicId, int partition, ParsleyClock clock) {}
}
