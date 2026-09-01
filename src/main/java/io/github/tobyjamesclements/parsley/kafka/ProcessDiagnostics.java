package io.github.tobyjamesclements.parsley.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.github.tobyjamesclements.parsley.api.TaskStatus;

/**
 * The latest {@link TaskStatus} of every live task of one process.
 *
 * <p>Tasks publish from their stream threads and {@code status()} reads from any thread, so
 * the map is concurrent and each entry is an immutable snapshot. A task retires its entry
 * when it closes, so a task reassigned elsewhere does not linger as stale state here.
 */
final class ProcessDiagnostics {
    private final ConcurrentHashMap<Integer, TaskStatus> byPartition = new ConcurrentHashMap<>();

    void publish(TaskStatus status) {
        byPartition.put(status.partition(), status);
    }

    void retire(int partition) {
        byPartition.remove(partition);
    }

    /**
     * @return every live task's latest snapshot, in partition order
     */
    List<TaskStatus> snapshot() {
        List<TaskStatus> tasks = new ArrayList<>(byPartition.values());
        tasks.sort(Comparator.comparingInt(TaskStatus::partition));
        return List.copyOf(tasks);
    }
}
