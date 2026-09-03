package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The scripted {@link AdminFactsSource} base the facts-round unit suites extend: no real
 * admin client, one declared topic ({@link #Z_ID} bound to the name "z"), a hand-held
 * clock, and silent no-answer overrides for every seam a scenario does not drive. Each
 * test file's double overrides only the seam its scenario scripts, so the shared
 * constructor shape and the no-op scaffolding live in exactly one place.
 */
abstract class ScriptedAdminFacts extends AdminFactsSource {
    /** The declared topic id, bound to the name "z" in every scripted source, and therefore pinned. */
    static final UUID Z_ID = new UUID(3, 3);
    /** The confirmation window every scripted source runs with. */
    static final long WINDOW_MILLIS = 1_000;

    /** The hand-held clock the rounds read; tests advance it directly. */
    final AtomicLong nowMillis;

    ScriptedAdminFacts() {
        this(new AtomicLong());
    }

    private ScriptedAdminFacts(AtomicLong nowMillis) {
        super(null, "g", Map.of(Z_ID, "z"), WINDOW_MILLIS, nowMillis::get);
        this.nowMillis = nowMillis;
    }

    /** No by-name answers unless the scenario scripts them. */
    @Override
    Map<String, Object> describeByNames(Set<String> names) {
        return Map.of();
    }

    /** No earliest-offset answers unless the scenario scripts them. */
    @Override
    Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
            Map<TopicPartition, OffsetSpec> queries) {
        return Map.of();
    }

    /** No committed offsets unless the scenario scripts them. */
    @Override
    Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        return Map.of();
    }
}
