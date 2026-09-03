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
 * <p>Every round asks two questions: how far retention has discarded on each channel, and
 * whether each topic still exists under the identity the process knows. The first is what
 * prunes causes that can no longer matter and refuses a held message the substrate has
 * discarded; the second is what prunes causes on deleted channels and refuses a recreated
 * one. The seed round at task initialisation also lists the group's committed read
 * positions, the baseline below which positions this process chose not to read count as
 * settled. No round reads positions after that: a cause always names a delivered record,
 * so receiving that record is what settles it, whatever aborted batches or control records
 * lie below it (D114).
 *
 * <p>A topic that cannot be described is treated as unavailable rather than gone. Absence of
 * evidence is not evidence of deletion, and treating it as such would settle a channel that
 * still has messages to yield.
 */
class AdminFactsSource implements FactsSource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminFactsSource.class);
    private static final long TIMEOUT_SECONDS = 10;
    /** How long a startup seed may wait for the source before starting unseeded. */
    private static final long SEED_WAIT_MILLIS = 5_000;

    /**
     * Serialises rounds: one instance serves every task of a process, and the admin client
     * is queried in batches. A lock rather than a monitor, so the seed path can bound its
     * wait instead of stacking initialising tasks behind a slow broker.
     */
    private final java.util.concurrent.locks.ReentrantLock roundLock = new java.util.concurrent.locks.ReentrantLock();

    enum NameVerdict {
        SAME_ID,

        RECREATED,

        NAME_GONE,

        DENIED,

        UNAVAILABLE
    }

    private final Admin admin;
    private final String groupId;
    private final Map<UUID, String> topicNamesById = new HashMap<>();

    /**
     * An open confirmation window: when its first affirmative observation landed, and when
     * its latest did. Maturity needs both bounds — a span of window length between the
     * first and latest observation, and no gap between consecutive observations reaching
     * the window length, so two isolated sightings bracketing a blind period (a starved
     * facts executor, a task out for a rebalance) can never confirm on elapsed time alone
     * (D85).
     */
    private static final class ConfirmationWindow {
        long since;
        long lastSeen;

        ConfirmationWindow(long now) {
            this.since = now;
            this.lastSeen = now;
        }
    }

    /**
     * The open dead-confirmation windows. Opened and extended only by an affirmed
     * name-gone answer; an id whose name was never learned has nothing to corroborate
     * against and is never confirmed dead at all.
     */
    private final Map<UUID, ConfirmationWindow> deadWindows = new HashMap<>();

    /**
     * The open recreation-confirmation windows. A recreated answer is as stale-prone as a
     * name-gone one — a broker whose metadata lags this process's own resolution serves
     * the previous incarnation's binding, indistinguishable from a genuine recreation —
     * so it earns the same continuous corroboration before conviction (D85).
     */
    private final Map<UUID, ConfirmationWindow> recreatedWindows = new HashMap<>();

    private final Set<UUID> confirmedDead = new HashSet<>();

    private final Set<UUID> confirmedRecreated = new HashSet<>();

    private final Map<UUID, Long> lastAsked = new HashMap<>();

    private final Set<UUID> pinnedIds;
    private final long deadConfirmationMillis;

    private final long evictionMillis;
    private final java.util.function.LongSupplier clock;

    private volatile boolean closed;

    AdminFactsSource(Admin admin, String groupId, Map<UUID, String> knownTopicNames,
                     long deadConfirmationMillis, java.util.function.LongSupplier clock) {
        this.admin = admin;
        this.groupId = groupId;
        this.topicNamesById.putAll(knownTopicNames);
        this.pinnedIds = Set.copyOf(knownTopicNames.keySet());
        this.deadConfirmationMillis = deadConfirmationMillis;
        this.evictionMillis = Math.max(8 * deadConfirmationMillis, 5 * 60_000L);
        this.clock = clock;
    }

    @Override
    public PositionFacts gather(Set<ChannelId> receivedChannels, Set<ChannelId> frontierChannels)
            throws Exception {
        roundLock.lockInterruptibly();
        try {
            return gatherRound(receivedChannels, frontierChannels, false);
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Waits a bounded five seconds for the source; a task initialising while another
     * round grinds against a slow broker starts unseeded rather than stalling the stream
     * thread. An unseeded task has no committed-position baseline, so a channel it has not
     * received from yet settles on its first receipt there instead.
     */
    @Override
    public PositionFacts gatherForSeed(Set<ChannelId> receivedChannels, Set<ChannelId> frontierChannels)
            throws Exception {
        if (!roundLock.tryLock(SEED_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
            LOG.warn("{}: facts source busy; starting unseeded, the baseline waits for each channel's first"
                    + " receipt", groupId);
            return PositionFacts.EMPTY;
        }
        try {
            return gatherRound(receivedChannels, frontierChannels, true);
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * One round. {@code withCommittedPositions} is true for the seed only: the group's
     * committed offsets are the baseline a task starts from, and after that they are never
     * needed, since receipt settles every cause (D114).
     */
    private PositionFacts gatherRound(Set<ChannelId> receivedChannels, Set<ChannelId> frontierChannels,
                                      boolean withCommittedPositions) throws Exception {
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
        // The observation stage: if the round aborts before this round's name answers have
        // landed, every open confirmation window loses its continuity — the anchor must not
        // survive an outage and let a stale name-gone answer after recovery confirm death
        // from two isolated observations. A failure in the later position queries does not
        // clear the windows: the observations this round were real and affirmative, and
        // erasing them would let a recurring late-stage failure keep a genuinely dead
        // channel unconfirmable forever.
        Map<UUID, String> liveNames;
        try {
            liveNames = describeByIds(undescribed);
        } catch (Exception e) {
            deadWindows.clear();
            recreatedWindows.clear();
            throw e;
        }

        Set<UUID> unknownIds = new HashSet<>(undescribed);
        unknownIds.removeAll(liveNames.keySet());
        Set<String> namesToCheck = new HashSet<>();
        for (UUID id : unknownIds) {
            String lastKnown = topicNamesById.get(id);
            if (lastKnown != null) {
                namesToCheck.add(lastKnown);
            }
        }

        // Confirmed verdicts are re-checked by name every round the id is asked about: the
        // topic reappearing under the same name with a different id upgrades dead to
        // recreated, and the name resolving to the very id a verdict condemned is
        // affirmative proof the confirming answers were stale — the substrate never
        // reuses a topic id — so the verdict is rescinded rather than held against the
        // evidence (D85).
        Set<UUID> confirmedIdsToRecheck = new HashSet<>();
        for (UUID id : topicIds) {
            String lastKnown = topicNamesById.get(id);
            if ((confirmedDead.contains(id) || confirmedRecreated.contains(id)) && lastKnown != null) {
                confirmedIdsToRecheck.add(id);
                namesToCheck.add(lastKnown);
            }
        }
        Map<String, Object> byNameOutcome = describeByNames(namesToCheck);

        long now = clock.getAsLong();
        // Every window this round observed is stamped again when the round ends (D107): the
        // offset wait, the seed's committed fetch and the confirming describe all run after
        // this sample, and D88's rule counts that as watched time, not blind time.
        Set<UUID> observedThisRound = new HashSet<>();
        for (UUID id : unknownIds) {
            String lastKnown = topicNamesById.get(id);
            NameVerdict verdict = classifyName(byNameOutcome, lastKnown, id);
            switch (verdict) {
                case SAME_ID -> {
                    deadWindows.remove(id);
                    recreatedWindows.remove(id);
                }
                // A recreated answer is served from one broker's metadata view, which can
                // lag this process's own resolution and serve the previous incarnation's
                // binding — indistinguishable from a genuine recreation — so conviction
                // takes the same continuously-corroborated window as death (D85).
                case RECREATED -> {
                    deadWindows.remove(id);
                    observedThisRound.add(id);
                    if (observeWindow(recreatedWindows, id, askedAt, now)) {
                        markRecreated(id);
                    }
                }
                case DENIED -> {
                    deadWindows.remove(id);
                    recreatedWindows.remove(id);
                    LOG.warn("{}: describe denied for topic '{}' ({}); treating as denied, not dead",
                            groupId, lastKnown, id);
                }
                case NAME_GONE -> {
                    recreatedWindows.remove(id);
                    observedThisRound.add(id);
                    if (observeWindow(deadWindows, id, askedAt, now)) {
                        markDead(id);
                    }
                }
                // Either the name was asked about but the answer did not arrive — the
                // name-gone observation is no longer continuous and the window restarts —
                // or no name was ever learned for this id, where no corroborating answer
                // is possible at all. Absence of evidence alone never confirms death: a
                // DENY-Describe ACL makes a live topic describe unknown by id, and for a
                // nameless id there is no by-name answer to reveal the denial, so a
                // time-only verdict here would prune a live cause (SPEC Structural 13).
                // The id lingers unconfirmed — costing expression size, never safety —
                // until its name is learned or its topic reappears.
                case UNAVAILABLE -> {
                    deadWindows.remove(id);
                    recreatedWindows.remove(id);
                }
            }
        }
        for (UUID id : confirmedIdsToRecheck) {
            NameVerdict verdict = classifyName(byNameOutcome, topicNamesById.get(id), id);
            if (verdict == NameVerdict.RECREATED && confirmedDead.contains(id)) {
                observedThisRound.add(id);
                if (observeWindow(recreatedWindows, id, askedAt, now)) {
                    markRecreated(id);
                }
            } else if (verdict == NameVerdict.SAME_ID) {
                LOG.warn("{}: {} verdict on topic '{}' ({}) rescinded: the name still resolves to the same"
                                + " id, so the answers that confirmed it were stale",
                        groupId, confirmedDead.contains(id) ? "dead" : "recreated",
                        topicNamesById.get(id), id);
                confirmedDead.remove(id);
                confirmedRecreated.remove(id);
                deadWindows.remove(id);
                recreatedWindows.remove(id);
            } else if (verdict != NameVerdict.RECREATED) {
                // The upgrade window takes the same contrary-observation restarts as the
                // first-classification path: a name-gone, denied or unavailable answer
                // breaks the reappearance run's continuity, so flapping metadata cannot
                // mature an upgrade the unknown-ids path would have kept restarting (D88).
                recreatedWindows.remove(id);
            }
        }

        Set<UUID> deadTopicIds = new HashSet<>(confirmedDead);
        Set<UUID> recreatedTopicIds = new HashSet<>(confirmedRecreated);
        for (UUID id : liveNames.keySet()) {
            deadWindows.remove(id);
            recreatedWindows.remove(id);
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
                    earliestOffsetFutures(offsetQueries);
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

        // Both name-keyed queries — log starts above and the seed's committed offsets here —
        // run between the opening describe and this confirming one, so neither is attributed
        // to a channel whose name-to-id binding did not hold across the query (D22).
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                withCommittedPositions ? committedOffsets() : Map.of();

        Map<UUID, String> confirmedNames = confirmIdentities(liveNames.keySet());

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
        stampObservedWindows(observedThisRound);
        return new PositionFacts(committedNextRead, logStart, deadChannels, recreatedChannels);
    }

    /**
     * Marks the end of this round's watch on every window it observed (D107). Continuity
     * is judged from the previous round's last stamp to the next round's first question;
     * before this stamp the reference was the classification-time sample, so the offset
     * wait and the confirming describe were charged as blind time. Maturity is still
     * judged at classification time: a single long round cannot mature a window it opened.
     */
    private void stampObservedWindows(Set<UUID> observedThisRound) {
        if (observedThisRound.isEmpty()) {
            return;
        }
        long roundEnd = clock.getAsLong();
        for (UUID id : observedThisRound) {
            ConfirmationWindow dead = deadWindows.get(id);
            if (dead != null) {
                dead.lastSeen = Math.max(dead.lastSeen, roundEnd);
            }
            ConfirmationWindow recreated = recreatedWindows.get(id);
            if (recreated != null) {
                recreated.lastSeen = Math.max(recreated.lastSeen, roundEnd);
            }
        }
    }

    /**
     * Extends or (re)opens a confirmation window on an affirmative observation, answering
     * whether the window has matured.
     *
     * <p>Continuity is judged on blind time: the interval that restarts the window runs
     * from the previous answer to the moment this round began asking, during which no
     * round was watching the id at all — a starved facts executor, a task out for a
     * rebalance. A blind interval reaching the window length makes the sightings
     * bracketing it isolated observations, exactly what D44's continuity requirement
     * rejects. Time a round spends inside its own queries is watched time: counting it
     * would let routine in-round latency — a leaderless partition burning the offset
     * deadline — exceed the window every round and make a genuinely dead topic permanently
     * unconfirmable (D88 corrects D85's spelling here).
     */
    private boolean observeWindow(Map<UUID, ConfirmationWindow> windows, UUID id, long askedAt, long now) {
        ConfirmationWindow window = windows.get(id);
        if (window == null || askedAt - window.lastSeen >= deadConfirmationMillis) {
            windows.put(id, new ConfirmationWindow(now));
            return false;
        }
        window.lastSeen = now;
        return now - window.since >= deadConfirmationMillis;
    }

    private void markDead(UUID id) {
        confirmedDead.add(id);
        forget(id);
    }

    private void markRecreated(UUID id) {
        confirmedRecreated.add(id);
        confirmedDead.remove(id);
        forget(id);
    }

    /**
     * Drops an id's confirmation windows, and its name binding unless the id is pinned.
     * Pinned ids keep their name so a confirmed verdict stays recheckable by name — the
     * recheck is what upgrades dead to recreated and rescinds a verdict its own evidence
     * contradicts (D85).
     */
    private void forget(UUID id) {
        deadWindows.remove(id);
        recreatedWindows.remove(id);
        if (!pinnedIds.contains(id)) {
            topicNamesById.remove(id);
        }
    }

    /**
     * Queries the earliest readable offset for each partition, one future per partition so a
     * single unavailable partition fails alone.
     */
    Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetFutures(
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

    /**
     * Stops the source. A round in flight — its queries bounded by their own timeouts, its
     * thread already interrupted by the runtime's executor shutdown — is given a bounded
     * wait; past it the source is marked closed without the lock (D109, Operational 3).
     */
    void close() {
        boolean locked;
        try {
            locked = roundLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            locked = false;
        }
        if (!locked) {
            closed = true;
            LOG.warn("{}: facts source busy at close; abandoning its round", groupId);
            return;
        }
        try {
            closed = true;
        } finally {
            roundLock.unlock();
        }
    }

    /**
     * Sends the by-id describe, one future per id. Its own seam, mirroring
     * {@link #earliestOffsetFutures}, so a scripted describe can fail a future and still
     * run the real tolerate-or-abort classification below — the branch that decides
     * whether one broker failure aborts the whole round — while a completed future runs
     * the real name learning, the one write path for non-declared bindings.
     */
    Map<Uuid, KafkaFuture<TopicDescription>> describeByIdFutures(Set<UUID> topicIds) {
        var uuids = topicIds.stream().map(TopicInfo::toKafkaUuid).toList();
        return admin.describeTopics(TopicCollection.ofTopicIds(uuids)).topicIdValues();
    }

    /**
     * The confirming describe that closes D22's identity window, after the name-keyed
     * queries. Its own seam because it is the round's last stage that can abort the round:
     * a failure here leaves this round's name observations standing, where a failure in
     * the opening describe clears them.
     */
    Map<UUID, String> confirmIdentities(Set<UUID> topicIds) throws Exception {
        return describeByIds(topicIds);
    }

    Map<UUID, String> describeByIds(Set<UUID> topicIds) throws Exception {
        Map<UUID, String> names = new HashMap<>();
        if (topicIds.isEmpty()) {
            return names;
        }
        Map<Uuid, KafkaFuture<TopicDescription>> futures = describeByIdFutures(topicIds);
        for (var entry : futures.entrySet()) {
            UUID id = TopicInfo.toJavaUuid(entry.getKey());
            try {
                TopicDescription description = entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                names.put(id, description.name());
                topicNamesById.put(id, description.name());
            } catch (ExecutionException e) {
                // InvalidTopicException is the admin client's client-side answer for an id
                // it deems unrepresentable (the reserved zero id): tolerated like unknown,
                // not rethrown, so one unanswerable id in the frontier cannot abort every
                // round forever. The decode path refuses such ids at receipt (D83); this
                // guards state persisted before that refusal existed.
                if (e.getCause() instanceof UnknownTopicIdException
                        || e.getCause() instanceof UnknownTopicOrPartitionException
                        || e.getCause() instanceof org.apache.kafka.common.errors.InvalidTopicException) {
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
