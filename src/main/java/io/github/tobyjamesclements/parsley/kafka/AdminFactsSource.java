package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
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
 * Position facts from the cluster (SPEC Assumption 15): the group's committed offsets are the host's read-position
 * report (SPEC Host obligation 2 — a committed position covers exactly what was fed or will never arrive, because the
 * host commits it atomically with the step that consumed it); log-start offsets are earliest retained positions; and
 * topic existence is checked by topic ID, so a recreated topic never impersonates a dead channel. Every fact is a
 * lower bound, so a stale answer is always safe. Errors surface to the caller, which skips the round.
 *
 * <p>Identity race guard: log starts are queried by topic name, but attributed to a topic ID only when a describe
 * *after* the offset query still maps that name to the same ID — discarding a cause requires certainty
 * (SPEC Structural 13).</p>
 */
class AdminFactsSource implements FactsSource {

    private static final Logger LOG = LoggerFactory.getLogger(AdminFactsSource.class);
    private static final long TIMEOUT_SECONDS = 10;

    /** What describing an id's last-known name established about the id (D44). */
    private enum NameVerdict {
        /** The name resolves to this very id: the topic is alive, whatever the by-id describe said. */
        SAME_ID,
        /** The name resolves to a different id: this id's topic is definitively gone — ids are never reused, and
         * a stale metadata view can serve an old binding but cannot invent a new one. */
        RECREATED,
        /** The name no longer exists either: consistent with deletion; still debounced against stale metadata. */
        NAME_GONE,
        /** Describe on the name was denied. The broker masks denied by-id describes as unknown-topic to avoid
         * leaking existence, so an unknown id with a denied name is evidence of denial, not of death. */
        DENIED,
        /** The name query failed some other way this round: no evidence either way. */
        UNAVAILABLE
    }

    private final Admin admin;
    private final String groupId;
    private final Map<String, Object> probeConsumerProperties;
    private final Map<UUID, String> topicNamesById = new HashMap<>();
    /** Monotonic-clock instant an id's corroborated-unknown run began. Time-based rather than round-based: one
     * facts source serves every task of the application, so counting rounds would shrink the debounce window as
     * task count grows, while any task's live sighting rightly resets the shared timer (D44). The clock must be
     * monotonic — a stepped wall clock would collapse the window into a premature, persisted dead verdict. */
    private final Map<UUID, Long> unknownSince = new HashMap<>();
    /** Ids confirmed dead: terminal (ids are never reused). */
    private final Set<UUID> confirmedDead = new HashSet<>();
    /** Ids confirmed recreated: terminal, and sticky — every round reports them until callers stop asking, so
     * every task's engine sees the identity change no matter which round first detected it, and a round lost in
     * a handoff loses nothing. */
    private final Set<UUID> confirmedRecreated = new HashSet<>();
    /** When each id was last asked about, by any task. Tracking state is evicted only when *no* task has asked
     * for a generous horizon — never against a single caller's set, which would let tasks with differing
     * frontiers erase each other's timers and verdicts. Bounds growth from injected foreign ids. */
    private final Map<UUID, Long> lastAsked = new HashMap<>();
    /** The ids this application declared (its own topics, seeded at construction): never evicted. Their name
     * bindings are the only evidence that can classify a later recreation as RECREATED rather than plain death,
     * and a task stalled in rebalance or restore must not lose that evidence to a sibling's eviction sweep. The
     * set is bounded by the declaration, so pinning costs nothing. */
    private final Set<UUID> pinnedIds;
    private final long deadConfirmationMillis;
    /** Eviction is a memory bound for injected foreign ids, not a correctness mechanism, so its horizon has an
     * absolute floor: at small facts intervals a purely interval-derived horizon would shrink to seconds and a
     * routine rebalance stall would erase a live debounce timer. */
    private final long evictionMillis;
    private final java.util.function.LongSupplier clock;
    private org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> probe;
    /** Set by {@link #close()}: a gather that starts after close (a straggling task init during a torn-down
     * runtime) must report nothing rather than lazily resurrect the probe consumer, which would leak. */
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

    @Override
    public synchronized PositionFacts gather(Set<ChannelId> receivedChannels, Map<ChannelId, Long> fedUpToHints,
                                             Set<ChannelId> frontierChannels) throws Exception {
        if (closed) {
            return PositionFacts.EMPTY; // the runtime is closing: report nothing rather than resurrect the probe
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

        // Corroborate every unknown id against its last-known name before concluding anything (D44): the broker
        // masks authorization denials on by-id describes as unknown-topic, and metadata answers can be stale — but
        // a name bound to a *different* id is affirmative evidence the old topic is gone, with no debounce needed.
        Set<UUID> unknownIds = new HashSet<>(undescribed);
        unknownIds.removeAll(liveNames.keySet());
        Set<String> namesToCheck = new HashSet<>();
        for (UUID id : unknownIds) {
            String lastKnown = topicNamesById.get(id);
            if (lastKnown != null) {
                namesToCheck.add(lastKnown);
            }
        }
        // Dead-confirmed declared ids keep their name binding, and the name is re-checked every round: the name
        // resolving to a *different* id is the only evidence that can reclassify the death as the recreation it
        // became, and every task's engine must see that identity change (CHANNEL_IDENTITY_CHANGED, with its reset
        // remedy) rather than fail on the next feed with a feed-order diagnosis.
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
                case SAME_ID -> unknownSince.remove(id);     // alive: the by-id answer was a metadata blip
                case RECREATED -> markRecreated(id);         // definitive: the name moved on, this id is dead
                case DENIED -> {
                    unknownSince.remove(id);                 // denial explains the mask; not evidence of death
                    LOG.warn("{}: describe denied for topic '{}' ({}); treating as denied, not dead",
                            groupId, lastKnown, id);
                }
                case NAME_GONE, UNAVAILABLE -> {
                    if (verdict == NameVerdict.NAME_GONE || lastKnown == null) {
                        // NAME_GONE is corroborated unknown: the standard debounce covers stale metadata. An id
                        // with no known name cannot be corroborated at all — it may be foreign injection, but it
                        // may equally be a legitimately new topic whose id reached this process through upstream
                        // metadata before this admin client's view caught up — so it gets a far longer window:
                        // wrongly killing a live cause is a safety-adjacent loss, while a genuinely foreign id
                        // merely lingers a little longer before it is discarded.
                        long window = lastKnown == null ? 4 * deadConfirmationMillis : deadConfirmationMillis;
                        long since = unknownSince.computeIfAbsent(id, i -> now);
                        if (now - since >= window) {
                            markDead(id);
                        }
                    }
                    // UNAVAILABLE with a known name: no evidence this round; the timer neither starts nor resets.
                }
            }
        }
        for (UUID id : deadIdsToRecheck) {
            if (classifyName(byNameOutcome, topicNamesById.get(id), id) == NameVerdict.RECREATED) {
                markRecreated(id); // the name moved on after death was confirmed: upgrade the verdict
            }
        }
        // Built once, after the verdict loop: the sticky sets are the single source of this round's report.
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
        Map<TopicPartition, Long> logStartByPartition = new HashMap<>();
        if (!offsetQueries.isEmpty()) {
            admin.listOffsets(offsetQueries, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .forEach((tp, info) -> logStartByPartition.put(tp, info.offset()));
        }

        // Re-describe by ID after the offset query: only attribute a log start to a channel whose topic name still
        // resolves to the same topic ID, so a recreation between the two queries can never mis-prune a cause.
        Map<UUID, String> confirmedNames = describeByIds(liveNames.keySet());

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

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
                continue; // identity unconfirmed this round: report nothing rather than guess
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

    /** Topic IDs are never reused, so death is sticky — but not final: a declared id keeps its name binding, and
     * a later round observing the name bound to a different id upgrades the verdict to recreated. Eviction happens
     * only when no caller has asked about the id within the eviction horizon (and never for declared ids). */
    private void markDead(UUID id) {
        confirmedDead.add(id);
        unknownSince.remove(id);
        // Declared ids keep their name binding past death: it is the only evidence that can later reclassify
        // this death as a recreation, and the pinned set is bounded by the declaration.
        if (!pinnedIds.contains(id)) {
            topicNamesById.remove(id);
        }
    }

    /** Terminal and sticky — and reported as recreation every round, so every task's engine performs its own
     * identity refusal rather than one task silently settling a channel another task saw recreated. */
    private void markRecreated(UUID id) {
        confirmedRecreated.add(id);
        confirmedDead.remove(id); // a recreation observed after death-confirmation upgrades the verdict
        forget(id);
    }

    /** Drop every per-id evidence entry (markDead deliberately keeps a pinned id's name binding instead — the
     * dead-to-recreated upgrade needs it). Adding a new evidence map means adding it here and deciding there. */
    private void forget(UUID id) {
        unknownSince.remove(id);
        topicNamesById.remove(id);
    }

    /** Describe topics by name, classifying each outcome. A name maps to the {@link UUID} it currently resolves
     * to, or to a {@link NameVerdict} explaining why it could not be resolved. */
    private Map<String, Object> describeByNames(Set<String> names) {
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
                Thread.currentThread().interrupt(); // shutting down: stop classifying, report nothing this round
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

    /**
     * The host's committed offsets never advance for a partition without a processed record in the current task
     * lifetime, so a trailing run of never-yielding positions (aborted transactions, control records) can stall a
     * held message forever after a restart (SPEC Liveness 3). A read_committed probe seeked just above the caller's
     * fed-or-never frontier settles the question from the substrate itself (SPEC Assumption 15): the first real
     * record's offset — or, when the poll returns nothing, the consumer position having advanced past aborted
     * batches — bounds a run of positions that will never yield a message this process receives. Probe failures are
     * skipped; the next round retries.
     *
     * <p>The probe is itself a name-based query, so its answers obey the same rule as the log starts (D22): a
     * probed offset is attributed to a channel id only when the round's confirming describe bound the name to that
     * very id before the probe, and a describe *after* the probe still does — a recreation interleaved anywhere
     * around the probe must never settle the old channel's positions with the new incarnation's offsets.</p>
     */
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
                continue; // identity unconfirmed this round, or the host's own report already passes the hint
            }
            try {
                var consumer = probeConsumer();
                consumer.assign(java.util.List.of(tp));
                consumer.seek(tp, fed + 1);
                long report = fed + 1;
                // A poll can return before its fetch response is processed; retry within the round until the
                // position moves or a real record arrives, so one facts round usually settles the question. A real
                // record ends the round immediately — its offset is the bound, and polling on would walk past it.
                for (int attempt = 0; attempt < 4; attempt++) {
                    var polled = consumer.poll(java.time.Duration.ofMillis(250));
                    if (!polled.isEmpty()) {
                        report = polled.records(tp).get(0).offset(); // positions below the first real record never yield
                        break;
                    }
                    report = consumer.position(tp);                  // advanced past aborted batches it fetched, if any
                    if (report > fed + 1) {
                        break;
                    }
                }
                if (report > fed + 1) {
                    probed.put(channel, report);
                }
            } catch (org.apache.kafka.common.errors.InterruptException e) {
                // The runtime is closing and interrupted this thread (the exception has re-set the flag): stop
                // probing altogether — recreating a consumer per remaining hint would just thrash through the
                // same interrupt.
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
        // D22 for the probe itself: re-describe the probed ids and attribute a probed offset only where the name
        // still binds to the same id — discarding the round's probes on failure loses liveness for one round,
        // never correctness.
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

    /** The one place the probe is discarded. Runs with the interrupt flag cleared: KafkaConsumer.close() throws
     * InterruptException when the calling thread is interrupted — and shutdown, which interrupts the facts
     * thread, is exactly when this cleanup runs — which would skip the null-out and leave a closed consumer
     * behind for the next round. The flag is restored, so the shutdown signal itself is never swallowed. */
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

    /** Topic names for the ids that still exist; ids that definitively no longer exist are simply absent.
     * Package-visible and overridable as the test seam for the identity race guard (D22, D50): interleaving a
     * recreation between the offsets query and the confirming describe needs control of this call's answers. */
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
                    // Definitive: the topic no longer exists. Topic IDs are never reused.
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
