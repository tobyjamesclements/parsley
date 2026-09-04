package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Classifies topic identity through the admin client, once per task initialisation.
 *
 * <p>Every id a task's state names is described by id. One that still resolves is alive, and
 * its name is learned. One that does not is not yet dead: a Describe denial masks a live
 * topic as unknown by id, and a broker's metadata view can lag. So an unknown id is judged
 * by its last-known name — declared at start, or learned at an earlier initialisation of
 * this process — and only from three consistent answers half a second apart, the evidence
 * standard the start path applies to the changelog and the declared topics (D84, D113):
 * the name resolving to another id confirms recreation, the name unknown confirms deletion,
 * and a denial, an unavailable answer, or the name resolving to the very id asked about
 * (the by-id answer was stale) keeps the id alive. An id whose name was never learned is
 * never confirmed dead at all (D75); it lingers in the frontier, costing expression size and
 * never safety.
 *
 * <p>One instance serves every task of a process, so a name learned by one task's
 * initialisation serves the next; the map is concurrent because tasks initialise on their
 * own stream threads.
 */
class AdminTopicIdentitySource implements TopicIdentitySource {
    private static final Logger LOG = LoggerFactory.getLogger(AdminTopicIdentitySource.class);
    private static final long TIMEOUT_SECONDS = 10;
    /** How many consistent by-name answers confirm a verdict (D113's standard). */
    static final int CORROBORATING_ANSWERS = 3;

    enum NameVerdict {
        SAME_ID,
        RECREATED,
        NAME_GONE,
        DENIED,
        UNAVAILABLE
    }

    private final Admin admin;
    private final String applicationId;
    private final Map<UUID, String> namesById = new ConcurrentHashMap<>();
    private final Duration corroborationBackoff;

    /**
     * @param admin                the admin client, shared with the runtime
     * @param applicationId        the process's application id, for diagnostics
     * @param declaredNames        the ids resolved at start, mapped to their declared names
     * @param corroborationBackoff how long to wait between corroborating answers: half a
     *                             second in production, ~zero in tests
     */
    AdminTopicIdentitySource(Admin admin, String applicationId, Map<UUID, String> declaredNames,
                             Duration corroborationBackoff) {
        this.admin = admin;
        this.applicationId = applicationId;
        this.namesById.putAll(declaredNames);
        this.corroborationBackoff = corroborationBackoff;
    }

    @Override
    public TopicIdentityVerdicts resolve(Set<UUID> topicIds) throws Exception {
        if (topicIds.isEmpty()) {
            return TopicIdentityVerdicts.NONE;
        }
        Set<UUID> alive = describeByIds(topicIds);
        Set<UUID> unknown = new HashSet<>(topicIds);
        unknown.removeAll(alive);
        if (unknown.isEmpty()) {
            return TopicIdentityVerdicts.NONE;
        }

        Map<UUID, String> nameOf = new HashMap<>();
        for (UUID id : unknown) {
            String name = namesById.get(id);
            if (name == null) {
                // No name to corroborate against: a denial would look exactly like this,
                // and absence of evidence is never evidence of deletion (D75).
                LOG.debug("{}: topic id {} unknown by id and never named; keeping its causes", applicationId, id);
            } else {
                nameOf.put(id, name);
            }
        }
        if (nameOf.isEmpty()) {
            return TopicIdentityVerdicts.NONE;
        }

        Set<UUID> deleted = new HashSet<>(nameOf.keySet());
        Set<UUID> recreated = new HashSet<>(nameOf.keySet());
        for (int answer = 0; answer < CORROBORATING_ANSWERS; answer++) {
            if (answer > 0) {
                Thread.sleep(corroborationBackoff.toMillis());
            }
            Map<String, Object> byName = describeByNames(new HashSet<>(nameOf.values()));
            for (var entry : nameOf.entrySet()) {
                NameVerdict verdict = classifyName(byName, entry.getValue(), entry.getKey());
                if (verdict != NameVerdict.NAME_GONE) {
                    deleted.remove(entry.getKey());
                }
                if (verdict != NameVerdict.RECREATED) {
                    recreated.remove(entry.getKey());
                }
                if (verdict == NameVerdict.DENIED) {
                    LOG.warn("{}: describe denied for topic '{}' ({}); treating as denied, not dead",
                            applicationId, entry.getValue(), entry.getKey());
                }
            }
            if (deleted.isEmpty() && recreated.isEmpty()) {
                break;
            }
        }
        for (UUID id : deleted) {
            LOG.info("{}: topic '{}' ({}) no longer exists; its causes can no longer matter",
                    applicationId, nameOf.get(id), id);
        }
        for (UUID id : recreated) {
            LOG.warn("{}: topic '{}' was recreated; {} is a dead incarnation", applicationId, nameOf.get(id), id);
        }
        return new TopicIdentityVerdicts(deleted, recreated);
    }

    /**
     * Sends the by-id describe, one future per id: its own seam, so a scripted answer can
     * fail one future and still run the real tolerate-or-abort classification.
     */
    Map<Uuid, KafkaFuture<TopicDescription>> describeByIdFutures(Set<UUID> topicIds) {
        var uuids = topicIds.stream().map(TopicInfo::toKafkaUuid).toList();
        return admin.describeTopics(TopicCollection.ofTopicIds(uuids)).topicIdValues();
    }

    /**
     * Describes ids, learning the name of each one that resolves.
     *
     * @return the ids that resolved
     * @throws Exception on any failure that is not the substrate's unknown-id answer: a
     *         timeout or an outage is not evidence about any id, and the whole check is
     *         skipped for this initialisation rather than concluded from it
     */
    Set<UUID> describeByIds(Set<UUID> topicIds) throws Exception {
        Set<UUID> alive = new HashSet<>();
        for (var entry : describeByIdFutures(topicIds).entrySet()) {
            UUID id = TopicInfo.toJavaUuid(entry.getKey());
            try {
                TopicDescription description = entry.getValue().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                alive.add(id);
                namesById.put(id, description.name());
            } catch (ExecutionException e) {
                // InvalidTopicException is the client's own answer for an id it deems
                // unrepresentable (the reserved zero id), tolerated like unknown (D83).
                if (e.getCause() instanceof UnknownTopicIdException
                        || e.getCause() instanceof UnknownTopicOrPartitionException
                        || e.getCause() instanceof InvalidTopicException) {
                    continue;
                }
                throw e;
            }
        }
        return alive;
    }

    /** The by-name describe: per name, the id it resolves to, or the {@link NameVerdict} it failed with. */
    Map<String, Object> describeByNames(Set<String> names) throws InterruptedException {
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
                } else if (e.getCause() instanceof TopicAuthorizationException) {
                    outcome.put(entry.getKey(), NameVerdict.DENIED);
                } else {
                    outcome.put(entry.getKey(), NameVerdict.UNAVAILABLE);
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                outcome.put(entry.getKey(), NameVerdict.UNAVAILABLE);
            }
        }
        return outcome;
    }

    private static NameVerdict classifyName(Map<String, Object> byNameOutcome, String name, UUID id) {
        Object result = byNameOutcome.get(name);
        if (result == null) {
            return NameVerdict.UNAVAILABLE;
        }
        if (result instanceof UUID resolved) {
            return resolved.equals(id) ? NameVerdict.SAME_ID : NameVerdict.RECREATED;
        }
        return (NameVerdict) result;
    }
}
