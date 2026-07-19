package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pre-start consumer-group offset seeding for a {@link CausalStreams} instance, run once from
 * {@link CausalStreams#start()} before the underlying {@code KafkaStreams} joins the group.
 *
 * <p><strong>Why.</strong> Every causal source is declared with {@code AutoOffsetReset.none()} (see
 * {@link CausalTopology}) so Kafka Streams fails fast — rather than silently jumping forward — when a
 * committed offset falls out of range (retention or {@code deleteRecords} outran a lagging consumer); a
 * silent forward jump would let {@link ParsleyChannels#bridge} fold real lost records as if they were
 * transaction markers. But {@code none()} also fails on a <em>genuine first start</em>, where no committed
 * offset exists yet. This seeder closes that gap: on a true first start it commits each source partition's
 * log-start offset, so {@code none()} sees a valid committed position and reads from the beginning exactly
 * as {@code earliest} would — while a real out-of-range restart is left untouched and correctly fails.
 *
 * <p><strong>First start is not "no committed offset".</strong> A consumer group's offsets expire once it
 * has been empty for {@code offsets.retention.minutes} (7 days by default), and an operator can delete them
 * outright — either can leave a partition with no committed offset while the node's causal state (the
 * persisted {@code highestReceived}, in the frontier changelog) survives. Seeding to log-start then would
 * commit a position <em>above</em> the surviving frontier, and the bridge would fold the intervening real
 * records as markers — the very violation {@code none()} exists to prevent. So this seeder refuses to seed a
 * missing partition when any Parsley changelog for this application still exists: that is not a first start,
 * and the only safe response is to fail loudly and require an explicit, complete reset.
 *
 * <p><strong>Concurrent startup.</strong> {@code alterConsumerGroupOffsets} requires an empty group and is
 * not transactional, so once a peer instance has joined it fails (wholesale or per partition). Two
 * mitigations, both fail-closed: (1) a tolerated group-not-empty failure is followed by a re-list that must
 * show every target partition committed (by the peer), else start aborts; (2) the surviving-state guard
 * itself re-lists before failing, because a peer that has already started created this application's
 * changelogs only <em>after</em> committing its own seed — so a fresh coordinator read is guaranteed to
 * observe that commit (a happens-before, not a timing guess), distinguishing a peer's concurrent first start
 * from genuine offset expiry.
 */
final class ParsleyOffsetSeeder {

    private static final Logger log = LoggerFactory.getLogger(ParsleyOffsetSeeder.class);

    private ParsleyOffsetSeeder() {
    }

    /**
     * Seeds any un-committed causal source partition to its log-start offset, if and only if this is a
     * genuine first start. Aborts loudly (throwing) on surviving-state-with-missing-offset, on a source
     * topic that does not exist, or on any admin failure other than a tolerated group-not-empty that a peer
     * then covers.
     *
     * @param admin           the narrow admin seam
     * @param applicationId   the Streams {@code application.id} (the consumer group id)
     * @param sourceTopics    every causal source topic (business and passthrough) whose partitions feed a
     *                        causal stage
     * @param changelogTopics this application's exact state-store changelog topic names — the marker of
     *                        surviving causal state; any one of them existing means this is not a first start
     */
    static void seed(ParsleySeedAdmin admin, String applicationId, Set<String> sourceTopics,
            Set<String> changelogTopics) throws Exception {
        if (sourceTopics.isEmpty()) {
            return;
        }
        Set<TopicPartition> sourcePartitions = sourcePartitions(admin, sourceTopics);
        Map<TopicPartition, Long> committed = admin.committedOffsets(applicationId);
        Set<TopicPartition> missing = missing(sourcePartitions, committed);
        if (missing.isEmpty()) {
            // Every source partition already has a committed offset: a normal restart (or an
            // already-running group). Nothing to seed; a genuinely out-of-range offset is left to fail
            // fast at fetch under none().
            return;
        }

        if (survivingState(admin, changelogTopics)) {
            // Not a first start — this application has run and left causal state. A missing offset here is
            // offset expiry or a manual offset delete. Re-list once (a concurrently-starting peer creates
            // changelogs only after committing its seed, so a fresh read observes that commit); only a
            // partition still uncommitted after the fresh read is a genuine problem.
            Set<TopicPartition> stillMissing = missing(sourcePartitions, admin.committedOffsets(applicationId));
            if (stillMissing.isEmpty()) {
                return;                                            // a peer's concurrent first start covered them
            }
            throw new IllegalStateException("causal source partition(s) " + sorted(stillMissing)
                    + " have no committed offset, but this application ('" + applicationId + "') has surviving "
                    + "Parsley state (a changelog topic exists) — so this is not a first start. The likely "
                    + "causes are offset expiry (offsets.retention.minutes) or a manual offset delete while "
                    + "the causal state survived; seeding these now could deliver records past the surviving "
                    + "frontier without their causes. Refusing to seed. If the offsets were lost, either "
                    + "restore them or perform a complete reset (delete this application's state stores, "
                    + "changelog topics, and consumer-group offsets together) so it restarts as a true first "
                    + "start. If instead you added partitions to a source topic, commit a starting offset for "
                    + "the new partition(s) manually — a genuinely new partition carries no surviving frontier "
                    + "and is safe to start from its log-start.");
        }

        // Genuine first start: seed every missing partition to its log-start offset.
        Map<TopicPartition, Long> earliest = admin.earliestOffsets(missing);
        Map<TopicPartition, Long> seeds = new HashMap<>();
        for (TopicPartition partition : missing) {
            Long logStart = earliest.get(partition);
            if (logStart == null) {
                throw new IllegalStateException("could not resolve the log-start offset for causal source "
                        + "partition " + partition + " while seeding a first start");
            }
            seeds.put(partition, logStart);
        }

        Map<TopicPartition, Throwable> outcomes = admin.commitSeedOffsets(applicationId, seeds);
        boolean toleratedGroupNotEmpty = false;
        for (Map.Entry<TopicPartition, Throwable> outcome : outcomes.entrySet()) {
            Throwable failure = outcome.getValue();
            if (failure == null) {
                continue;
            }
            if (isGroupNotEmpty(failure)) {
                // A peer instance has already joined the group; it is seeding (or has seeded). Tolerate,
                // then verify below that the partition actually ended up committed.
                toleratedGroupNotEmpty = true;
                continue;
            }
            throw new IllegalStateException("failed to seed the first-start offset for causal source "
                    + "partition " + outcome.getKey() + " (group '" + applicationId + "')", failure);
        }
        if (toleratedGroupNotEmpty) {
            Set<TopicPartition> stillMissing = missing(seeds.keySet(), admin.committedOffsets(applicationId));
            if (!stillMissing.isEmpty()) {
                throw new IllegalStateException("could not seed first-start offsets for causal source "
                        + "partition(s) " + sorted(stillMissing) + ": the consumer group '" + applicationId
                        + "' is no longer empty and no peer instance committed them. Retry the start once the "
                        + "group settles.");
            }
        }
        log.info("Seeded {} causal source partition(s) to log-start for a first start of group '{}'",
                seeds.size(), applicationId);
    }

    private static Set<TopicPartition> sourcePartitions(ParsleySeedAdmin admin, Set<String> sourceTopics)
            throws Exception {
        Map<String, Integer> counts = admin.partitionCounts(sourceTopics);
        Set<TopicPartition> partitions = new HashSet<>();
        for (String topic : sourceTopics) {
            Integer count = counts.get(topic);
            if (count == null || count <= 0) {
                throw new IllegalStateException("causal source topic '" + topic + "' has no partitions at "
                        + "startup (does it exist?); cannot seed its offsets");
            }
            for (int partition = 0; partition < count; partition++) {
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        return partitions;
    }

    private static Set<TopicPartition> missing(Set<TopicPartition> partitions, Map<TopicPartition, Long> committed) {
        Set<TopicPartition> missing = new HashSet<>();
        for (TopicPartition partition : partitions) {
            if (!committed.containsKey(partition)) {
                missing.add(partition);
            }
        }
        return missing;
    }

    /**
     * Whether any of this application's own state-store changelog topics exists — the marker of surviving
     * causal state. Matched by exact name ({@code changelogTopics} carries the resolved
     * {@code <application.id>-<store>-changelog} names), not a name prefix, so an unrelated application whose
     * id merely shares this one's prefix (e.g. {@code orders} vs {@code orders-enrichment}) cannot be
     * mistaken for this application's own state and wrongly fail its genuine first start.
     */
    private static boolean survivingState(ParsleySeedAdmin admin, Set<String> changelogTopics) throws Exception {
        if (changelogTopics.isEmpty()) {
            return false;
        }
        for (String topic : admin.listTopicNames()) {
            if (changelogTopics.contains(topic)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isGroupNotEmpty(Throwable failure) {
        return failure instanceof UnknownMemberIdException || failure instanceof GroupNotEmptyException;
    }

    private static Set<String> sorted(Set<TopicPartition> partitions) {
        Set<String> names = new TreeSet<>();
        partitions.forEach(partition -> names.add(partition.toString()));
        return names;
    }
}
