package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;

/**
 * Wraps a {@link MockOrphanIndex}, appending a label to a shared, test-owned log every time
 * {@link #markOrphaned} is called. Paired with {@link RecordingBufferStore} (logging the same list) to
 * let a test assert the relative order of an orphan-index write versus a buffer removal — the
 * write-ordering invariant {@link ParsleyEngine}'s "Dead-letter persistence ordering" Javadoc documents.
 */
final class RecordingOrphanIndex implements ParsleyOrphanIndex {

    private final ParsleyOrphanIndex delegate = new MockOrphanIndex();
    private final List<String> log;
    private final Map<Uuid, String> labels;

    /**
     * @param log    the shared call-order log this index appends {@code "mark:<label>@<floor>"} entries to
     * @param labels a topic UUID &rarr; readable name lookup, for legible log entries and assertion failures
     */
    RecordingOrphanIndex(List<String> log, Map<Uuid, String> labels) {
        this.log = log;
        this.labels = labels;
    }

    @Override
    public boolean markOrphaned(Uuid topicId, int partition, long floor) {
        log.add("mark:" + labels.getOrDefault(topicId, topicId.toString()) + "@" + floor);
        return delegate.markOrphaned(topicId, partition, floor);
    }

    @Override
    public long orphanFloor(Uuid topicId, int partition) {
        return delegate.orphanFloor(topicId, partition);
    }

    @Override
    public void prune(Uuid topicId, int partition) {
        delegate.prune(topicId, partition);
    }
}
