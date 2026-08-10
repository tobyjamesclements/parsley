package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

/**
 * Gathers broker position facts through the admin client.
 *
 * <p>Two questions are asked of each channel: how far the log is readable under
 * {@code read_committed}, and how far retention has discarded. The first lets a channel
 * settle past positions no message will ever occupy, such as those consumed by an aborted
 * transaction. The second distinguishes positions this process chose not to read from
 * positions it can no longer read.
 *
 * <p>A topic that cannot be described is treated as unavailable rather than gone. Absence of
 * evidence is not evidence of deletion, and treating it as such would settle a channel that
 * still has messages to yield.
 */
class AdminFactsSource implements FactsSource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminFactsSource.class);
    private static final long TIMEOUT_SECONDS = 10;

    enum NameVerdict {
        SAME_ID,

        RECREATED,

        NAME_GONE,

        DENIED,

        UNAVAILABLE
    }

    private final Admin admin;
    private final String groupId;
    private final Map<String, Object> probeConsumerProperties;
    private final Map<UUID, String> topicNamesById = new HashMap<>();

    private final Map<UUID, Long> unknownSince = new HashMap<>();

    private final Set<UUID> confirmedDead = new HashSet<>();

    private final Set<UUID> confirmedRecreated = new HashSet<>();

    private final Map<UUID, Long> lastAsked = new HashMap<>();

    private final Set<UUID> pinnedIds;
    private final long deadConfirmationMillis;

    private final long evictionMillis;
    private final java.util.function.LongSupplier clock;
    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> probe;

    private boolean closed;

    AdminFactsSource(Admin admin, String groupId, Map<UUID, String> knownTopicNames,
                     Map<String, Object> probeConsumerProperties,
                     long deadConfirmationMillis, java.util.function.LongSupplier clock) {
        this.admin = admin;
        this.groupId = groupId;
        this.probeConsumerProperties = Map.copyOf(probeConsumerProperties);
        this.topicNamesById.putAll(knownTopicNames);
        this.pinnedIds = Set.copyOf(knownTopicNames.keySet());
        this.deadConfirmationMillis = deadConfirmationMillis;
        this.evictionMillis = Math.max(8 * deadConfirmationMillis, 5 * 60_000L);
        this.clock = clock;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Synchronized because one instance serves every task of a process, and the admin
     * client is queried in batches.
     */
    @Override
    public synchronized PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                                             Set<ChannelId> frontierChannels) throws Exception {
        if (closed) {
            return PositionFacts.EMPTY;
        }
        Set<UUID> topicIds = new HashSet<>();
        for (ChannelId channel : receivedChannels) {
            topicIds.add(channel.topicId());
        }
        for (ChannelId channel : frontierChannels) {
            topicIds.add(channel.topicId());
        }
        long askedAt = clock.getAsLong();
        for (UUID id : topicIds) {
            lastAsked.put(id, askedAt);
        }
        long evictionHorizon = askedAt - evictionMillis;
        lastAsked.entrySet().removeIf(entry -> {
            if (entry.getValue() >= evictionHorizon || pinnedIds.contains(entry.getKey())) {
                return false;
            }
            confirmedDead.remove(entry.getKey());
            confirmedRecreated.remove(entry.getKey());
            forget(entry.getKey());
            return true;
        });

        Set<UUID> undescribed = new HashSet<>(topicIds);
        undescribed.removeAll(confirmedDead);
        undescribed.removeAll(confirmedRecreated);
        Map<UUID, String> liveNames = describeByIds(undescribed);

        Set<UUID> unknownIds = new HashSet<>(undescribed);
        unknownIds.removeAll(liveNames.keySet());
        Set<String> namesToCheck = new HashSet<>();
        for (UUID id : unknownIds) {
            String lastKnown = topicNamesById.get(id);
            if (lastKnown != null) {
                namesToCheck.add(lastKnown);
            }
        }

        Set<UUID> deadIdsToRecheck = new HashSet<>();
        for (UUID id : topicIds) {
            String lastKnown = topicNamesById.get(id);
            if (confirmedDead.contains(id) && lastKnown != null) {
                deadIdsToRecheck.add(id);
                namesToCheck.add(lastKnown);
            }
        }
        Map<String, Object> byNameOutcome = describeByNames(namesToCheck);

        long now = clock.getAsLong();
        for (UUID id : unknownIds) {
            String lastKnown = topicNamesById.get(id);
            NameVerdict verdict = classifyName(byNameOutcome, lastKnown, id);
            switch (verdict) {
                case SAME_ID -> unknownSince.remove(id);
                case RECREATED -> markRecreated(id);
                case DENIED -> {
                    unknownSince.remove(id);
                    LOG.warn("{}: describe denied for topic '{}' ({}); treating as denied, not dead",
                            groupId, lastKnown, id);
                }
                case NAME_GONE, UNAVAILABLE -> {
                    if (verdict == NameVerdict.NAME_GONE || lastKnown == null) {
                        long window = lastKnown == null ? 4 * deadConfirmationMillis : deadConfirmationMillis;
                        long since = unknownSince.computeIfAbsent(id, i -> now);
                        if (now - since >= window) {
                            markDead(id);
                        }
                    } else {
                        // The name was asked about but the answer did not arrive, so the
                        // name-gone observation is no longer continuous. The window restarts:
                        // a dead verdict is confirmed only by an unbroken run of affirmative
                        // name-gone answers, never by two isolated ones spanning an outage.
                        unknownSince.remove(id);
                    }
                }
            }
        }
        for (UUID id : deadIdsToRecheck) {
            if (classifyName(byNameOutcome, topicNamesById.get(id), id) == NameVerdict.RECREATED) {
                markRecreated(id);
            }
        }

        Set<UUID> deadTopicIds = new HashSet<>(confirmedDead);
        Set<UUID> recreatedTopicIds = new HashSet<>(confirmedRecreated);
        for (UUID id : liveNames.keySet()) {
            unknownSince.remove(id);
        }

        Map<TopicPartition, OffsetSpec> offsetQueries = new HashMap<>();
        Map<ChannelId, TopicPartition> partitionsByChannel = new HashMap<>();
        for (ChannelId channel : union(receivedChannels, frontierChannels)) {
            String name = liveNames.get(channel.topicId());
            if (name != null) {
                TopicPartition tp = new TopicPartition(name, channel.partition());
                partitionsByChannel.put(channel, tp);
                offsetQueries.put(tp, OffsetSpec.earliest());
            }
        }
        // Per partition rather than batched: one leaderless partition must confine its loss
        // to its own channel, not black out the whole round — the round is also the only
        // mid-run carrier of the dead and recreated verdicts computed above.
        Map<TopicPartition, Long> logStartByPartition = new HashMap<>();
        if (!offsetQueries.isEmpty()) {
            Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures =
                    earliestOffsets(offsetQueries);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            for (var entry : futures.entrySet()) {
                try {
                    long remaining = Math.max(1L, deadline - System.nanoTime());
                    logStartByPartition.put(entry.getKey(),
                            entry.getValue().get(remaining, TimeUnit.NANOSECONDS).offset());
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    LOG.warn("{}: earliest-offset query failed for {}; withholding its facts this round",
                            groupId, entry.getKey(), e);
                }
            }
        }

        Map<UUID, String> confirmedNames = describeByIds(liveNames.keySet());

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed = committedOffsets();

        Map<ChannelId, Long> committedNextRead = new TreeMap<>();
        Map<ChannelId, Long> logStart = new TreeMap<>();
        Set<ChannelId> deadChannels = new TreeSet<>();
        Set<ChannelId> recreatedChannels = new TreeSet<>();
        for (ChannelId channel : union(receivedChannels, frontierChannels)) {
            if (recreatedTopicIds.contains(channel.topicId())) {
                recreatedChannels.add(channel);
                continue;
            }
            if (deadTopicIds.contains(channel.topicId())) {
                deadChannels.add(channel);
                continue;
            }
            TopicPartition tp = partitionsByChannel.get(channel);
            if (tp == null || !tp.topic().equals(confirmedNames.get(channel.topicId()))) {
                continue;
            }
            Long start = logStartByPartition.get(tp);
            if (start != null) {
                logStart.put(channel, start);
            }
            if (receivedChannels.contains(channel)) {
                var offsetAndMetadata = committed.get(tp);
                if (offsetAndMetadata != null) {
                    committedNextRead.put(channel, offsetAndMetadata.offset());
                }
            }
        }
        probeTrailingRuns(fedUpToHints, partitionsByChannel, confirmedNames, committedNextRead);
        return new PositionFacts(committedNextRead, logStart, deadChannels, recreatedChannels);
    }

    private void markDead(UUID id) {
        confirmedDead.add(id);
        unknownSince.remove(id);

        if (!pinnedIds.contains(id)) {
            topicNamesById.remove(id);
        }
    }

    private void markRecreated(UUID id) {
        confirmedRecreated.add(id);
        confirmedDead.remove(id);
        forget(id);
    }

    private void forget(UUID id) {
        unknownSince.remove(id);
        topicNamesById.remove(id);
    }

    /**
     * Queries the earliest readable offset for each partition, one future per partition so a
     * single unavailable partition fails alone.
     */
    Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsets(
            Map<TopicPartition, OffsetSpec> queries) {
        ListOffsetsResult result = admin.listOffsets(queries, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = new HashMap<>();
        for (TopicPartition tp : queries.keySet()) {
            futures.put(tp, result.partitionResult(tp));
        }
        return futures;
    }

    /** Fetches the group's committed read positions from the coordinator. */
    Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committedOffsets() throws Exception {
        return admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    Map<String, Object> describeByNames(Set<String> names) {
        Map<String, Object> outcome = new HashMap<>();
        if (names.isEmpty()) {
            return outcome;
        }
        Map<String, KafkaFuture<TopicDescription>> futures = admin.describeTopics(names).topicNameValues();
        for (var entry : futures.entrySet()) {
            try {
                TopicDescription description = entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                outcome.put(entry.getKey(), TopicInfo.toJavaUuid(description.topicId()));
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    outcome.put(entry.getKey(), NameVerdict.NAME_GONE);
                } else if (e.getCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException) {
                    outcome.put(entry.getKey(), NameVerdict.DENIED);
                } else {
                    outcome.put(entry.getKey(), NameVerdict.UNAVAILABLE);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome.put(entry.getKey(), NameVerdict.UNAVAILABLE);
                return outcome;
            } catch (Exception e) {
                outcome.put(entry.getKey(), NameVerdict.UNAVAILABLE);
            }
        }
        return outcome;
    }

    private NameVerdict classifyName(Map<String, Object> byNameOutcome, String lastKnown, UUID id) {
        if (lastKnown == null) {
            return NameVerdict.UNAVAILABLE;
        }
        Object result = byNameOutcome.get(lastKnown);
        if (result == null) {
            return NameVerdict.UNAVAILABLE;
        }
        if (result instanceof UUID resolved) {
            return resolved.equals(id) ? NameVerdict.SAME_ID : NameVerdict.RECREATED;
        }
        return (NameVerdict) result;
    }

    private void probeTrailingRuns(Map<ChannelId, Long> fedUpToHints,
                                   Map<ChannelId, TopicPartition> partitionsByChannel,
                                   Map<UUID, String> confirmedNames,
                                   Map<ChannelId, Long> committedNextRead) {
        Map<ChannelId, Long> probed = new HashMap<>();
        for (var hint : fedUpToHints.entrySet()) {
            ChannelId channel = hint.getKey();
            long fed = hint.getValue();
            if (fed == Long.MAX_VALUE) {
                continue;
            }
            TopicPartition tp = partitionsByChannel.get(channel);
            Long committedOffset = committedNextRead.get(channel);
            if (tp == null || !tp.topic().equals(confirmedNames.get(channel.topicId()))
                    || (committedOffset != null && committedOffset > fed + 1)) {
                continue;
            }
            try {
                var consumer = probeConsumer();
                consumer.assign(java.util.List.of(tp));
                consumer.seek(tp, fed + 1);
                long report = fed + 1;

                for (int attempt = 0; attempt < 4; attempt++) {
                    var polled = consumer.poll(java.time.Duration.ofMillis(250));
                    if (!polled.isEmpty()) {
                        report = polled.records(tp).get(0).offset();
                        break;
                    }
                    report = consumer.position(tp);
                    if (report > fed + 1) {
                        break;
                    }
                }
                if (report > fed + 1) {
                    probed.put(channel, report);
                }
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                closeProbe();
                return;
            } catch (RuntimeException e) {
                LOG.warn("{}: probe of {} failed; retrying next round", groupId, tp, e);
                closeProbe();
            }
        }
        if (probed.isEmpty()) {
            return;
        }

        Map<UUID, String> namesAfterProbe;
        try {
            Set<UUID> probedIds = new HashSet<>();
            for (ChannelId channel : probed.keySet()) {
                probedIds.add(channel.topicId());
            }
            namesAfterProbe = describeByIds(probedIds);
        } catch (Exception e) {
            LOG.warn("{}: could not confirm probed identities; discarding this round's probe results", groupId, e);
            return;
        }
        probed.forEach((channel, report) -> {
            TopicPartition tp = partitionsByChannel.get(channel);
            if (tp != null && tp.topic().equals(namesAfterProbe.get(channel.topicId()))) {
                LOG.info("{}: probe settled {} positions {}..{} as never-yielding",
                        groupId, tp, fedUpToHints.get(channel) + 1, report - 1);
                committedNextRead.merge(channel, report, Math::max);
            }
        });
    }

    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> probeConsumer() {
        if (probe == null) {
            Map<String, Object> props = new HashMap<>(probeConsumerProperties);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
            props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.remove(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG);
            probe = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props,
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                    new org.apache.kafka.common.serialization.ByteArrayDeserializer());
        }
        return probe;
    }

    private void closeProbe() {
        if (probe == null) {
            return;
        }
        boolean interrupted = Thread.interrupted();
        try {
            probe.close();
        } catch (RuntimeException e) {
            LOG.warn("{}: probe consumer close failed", groupId, e);
        } finally {
            probe = null;
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    synchronized void close() {
        closed = true;
        closeProbe();
    }

    Map<UUID, String> describeByIds(Set<UUID> topicIds) throws Exception {
        Map<UUID, String> names = new HashMap<>();
        if (topicIds.isEmpty()) {
            return names;
        }
        var uuids = topicIds.stream().map(TopicInfo::toKafkaUuid).toList();
        Map<Uuid, KafkaFuture<TopicDescription>> futures =
                admin.describeTopics(TopicCollection.ofTopicIds(uuids)).topicIdValues();
        for (var entry : futures.entrySet()) {
            UUID id = TopicInfo.toJavaUuid(entry.getKey());
            try {
                TopicDescription description = entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                names.put(id, description.name());
                topicNamesById.put(id, description.name());
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicIdException
                        || e.getCause() instanceof UnknownTopicOrPartitionException) {
                    continue;
                }
                throw e;
            }
        }
        return names;
    }

    private static Set<ChannelId> union(Set<ChannelId> a, Set<ChannelId> b) {
        Set<ChannelId> union = new TreeSet<>(a);
        union.addAll(b);
        return union;
    }
}
