package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;

import java.util.function.Supplier;

/**
 * The completeness clock as of the task's last <em>committed</em> Kafka transaction — the only value
 * safe to publish on the non-transactional epoch-events side channel.
 *
 * <p><strong>Why the live clock is not publishable.</strong> Under {@code exactly_once_v2}, the
 * in-memory completeness reflects deliveries whose changelog writes and forwards sit in the current,
 * uncommitted transaction; a crash before commit rolls them back, but a {@link
 * ParsleyEpochEvent.FrontierPublished} appended to the epoch-events log — an idempotent side-channel
 * producer, deliberately outside the task's transaction — does not roll back with them. The committed
 * epoch floor (the merge-min of published clocks) could then exceed a member's durable progress. A
 * windowed transition tolerates that (the window closes only once the local frontier dominates the
 * floor), but a fresh joiner settles at the floor <em>directly</em>, treating everything below it as
 * pre-epoch history — records the crashed member re-forwards after replay would be misclassified and
 * released without ordering. Publishing only committed state closes the hole; the floor is merely
 * (at most one commit interval) more conservative, which is always safe.
 *
 * <p><strong>How commit-time is observed.</strong> This is a registered (non-persistent, non-logged)
 * {@link StateStore} solely so Kafka Streams calls {@link #flush()} in every task commit cycle —
 * there is no other commit hook in the Processor API. Two slots make it crash-safe: {@code flush()}
 * promotes the <em>previous</em> flush's snapshot to {@link #committed()} and takes a fresh one as
 * pending. A flush in cycle {@code N} can only run after cycle {@code N-1}'s transaction fully
 * committed, so the promoted snapshot is always durable; the fresh pending snapshot (cycle {@code N},
 * possibly aborted later) is never published until a <em>subsequent</em> flush proves its transaction
 * committed. A crash between flush and commit therefore discards the optimistic pending value along
 * with the task instance. Both slots seed from the restored completeness at {@link #bind} — durable
 * by definition, since it was rebuilt from the committed changelog.
 *
 * <p>{@link #committed()} is read by the epoch runtime's background thread (via
 * {@code registerLocalCompleteness}) as well as the task thread, hence the volatiles.
 */
final class ParsleyCommittedCompleteness implements StateStore {

    private final String name;
    private volatile boolean open;
    private volatile Supplier<ParsleyVectorClock> live = ParsleyVectorClock::empty;
    private volatile ParsleyVectorClock pending = ParsleyVectorClock.empty();
    private volatile ParsleyVectorClock committed = ParsleyVectorClock.empty();

    ParsleyCommittedCompleteness(String name) {
        this.name = name;
    }

    /**
     * Wires the live completeness supplier and seeds both slots from {@code restored} — the
     * completeness rebuilt from the committed changelog at task init, durable by definition. Called
     * once, from {@code ParsleyProcessor#init}, before the first publish can happen.
     */
    void bind(Supplier<ParsleyVectorClock> liveCompleteness, ParsleyVectorClock restored) {
        this.live = liveCompleteness;
        this.pending = restored;
        this.committed = restored;
    }

    /** The completeness as of the last committed transaction — the only publishable clock. */
    ParsleyVectorClock committed() {
        return committed;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(StateStoreContext stateStoreContext, StateStore root) {
        // Registered purely for the commit-cycle flush() callback: nothing is persisted, logged, or
        // restored (logging is disabled on the builder, so the restore callback can never fire).
        stateStoreContext.register(root, (key, value) -> { });
        open = true;
    }

    @Override
    public void flush() {
        // Promote-then-snapshot: the previous flush's snapshot belongs to a transaction that has
        // provably committed by now (this flush's cycle could not have started otherwise); the fresh
        // snapshot waits for the same proof from the next flush.
        committed = pending;
        pending = live.get();
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }
}
