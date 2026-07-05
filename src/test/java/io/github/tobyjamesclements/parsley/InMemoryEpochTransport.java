package io.github.tobyjamesclements.parsley;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A hand-rolled {@link ParsleyEpochTransport} double: an in-memory, append-only, totally-ordered log with
 * a per-instance read cursor — the two properties {@link ParsleyEpochLog}'s determinism relies on, with
 * no broker. Several transports over one {@link SharedLog} model several nodes reading the same log: each
 * has its own cursor, so each independently replays the shared order.
 *
 * <p>Single-threaded by construction — tests drive {@link ParsleyEpochRuntime#runOnce()} synchronously,
 * so no synchronization is needed.
 */
final class InMemoryEpochTransport implements ParsleyEpochTransport {

    /** The shared backing log; one instance is shared across every transport standing in for a node. */
    static final class SharedLog {
        private final List<ParsleyEpochEvent> events = new ArrayList<>();

        /** Every event appended so far, in log order. */
        List<ParsleyEpochEvent> events() {
            return List.copyOf(events);
        }

        /** The number of committed epochs (distinct {@link ParsleyEpochEvent.EpochCommitted} records) on the log. */
        long commitCount() {
            return events.stream().filter(e -> e instanceof ParsleyEpochEvent.EpochCommitted).count();
        }
    }

    private final SharedLog log;
    private final int bootstrapTarget;
    private int cursor;

    InMemoryEpochTransport(SharedLog log) {
        this.log = log;
        // The backlog present at construction — this reader is caught up once it has folded past it.
        this.bootstrapTarget = log.events().size();
    }

    @Override
    public void append(ParsleyEpochEvent event) {
        log.events.add(event);
    }

    @Override
    public List<ParsleyEpochEvent> poll(Duration timeout) {
        List<ParsleyEpochEvent> fresh = new ArrayList<>(log.events.subList(cursor, log.events.size()));
        cursor = log.events.size();
        return fresh;
    }

    @Override
    public boolean caughtUp() {
        return cursor >= bootstrapTarget;
    }

    @Override
    public void close() {
    }
}
