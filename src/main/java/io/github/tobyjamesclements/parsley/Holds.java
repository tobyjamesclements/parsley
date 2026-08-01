package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The publication seam between the stream threads, which alone may read protocol state, and
 * {@link CausalStreams#explainHolds()}, which any thread may call.
 *
 * <p>Each task's adapter builds an immutable snapshot inside its own wall-clock punctuator, on
 * the stream thread and between records, and publishes it here. Readers only ever see a
 * published list. That is the same discipline the metric gauges follow, and it is what keeps a
 * cross-thread diagnostic surface off the processing path's locks.
 *
 * <p>Keyed by application id so several applications in one JVM (tests, in particular) stay
 * separate, and by task within one, so a migrated task's entry is replaced rather than
 * duplicated. A closing processor removes its own entry.
 */
final class Holds {

    private static final ConcurrentMap<String, ConcurrentMap<String, List<HeldRecord>>> BY_APPLICATION =
            new ConcurrentHashMap<>();

    private Holds() {}

    /**
     * Publishes one task's snapshot, replacing its previous one.
     *
     * <p>An empty snapshot removes the task's entry rather than publishing an empty list.
     *
     * @param applicationId the application the task belongs to
     * @param taskKey the task, distinguishing one task's entry from another's
     * @param snapshot the held heads, copied so later mutation cannot reach a reader
     */
    static void publish(String applicationId, String taskKey, List<HeldRecord> snapshot) {
        ConcurrentMap<String, List<HeldRecord>> byTask =
                BY_APPLICATION.computeIfAbsent(applicationId, k -> new ConcurrentHashMap<>());
        if (snapshot.isEmpty()) {
            byTask.remove(taskKey);
        } else {
            byTask.put(taskKey, List.copyOf(snapshot));
        }
    }

    /**
     * Drops one task's snapshot. Called when its processor closes, including on migration.
     *
     * @param applicationId the application the task belongs to
     * @param taskKey the task whose entry to drop
     */
    static void clear(String applicationId, String taskKey) {
        ConcurrentMap<String, List<HeldRecord>> byTask = BY_APPLICATION.get(applicationId);
        if (byTask != null) {
            byTask.remove(taskKey);
            if (byTask.isEmpty()) BY_APPLICATION.remove(applicationId, byTask);
        }
    }

    /**
     * Returns every held head across the application's local tasks, longest-held first.
     *
     * <p>That is the order a reader wants, since the oldest hold is the one gating everything
     * else.
     *
     * @param applicationId the application to report on
     * @return the held heads, longest-held first, or an empty list when nothing is held
     */
    static List<HeldRecord> forApplication(String applicationId) {
        ConcurrentMap<String, List<HeldRecord>> byTask = BY_APPLICATION.get(applicationId);
        if (byTask == null) return List.of();
        List<HeldRecord> all = new ArrayList<>();
        for (Map.Entry<String, List<HeldRecord>> e : byTask.entrySet()) {
            all.addAll(e.getValue());
        }
        all.sort(Comparator.comparingLong(HeldRecord::heldMs).reversed());
        return List.copyOf(all);
    }
}
