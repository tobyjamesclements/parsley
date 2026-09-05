package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Establishes the identity classification a task's initialisation acts on (D115), over
 * scripted admin answers: which answers confirm a topic deleted or recreated, and which —
 * a denial, an unavailable answer, a name the source never learned, a stale by-id answer,
 * or an answer that flips mid-corroboration — keep its causes expressed.
 */
class AdminTopicIdentitySourceTest {
    private static final UUID DECLARED_ID = new UUID(500, 1);
    private static final UUID UPSTREAM_ID = new UUID(500, 2);
    private static final UUID NAMELESS_ID = new UUID(500, 3);
    private static final UUID NEW_ID = new UUID(500, 9);

    /** Scripted describes: by id from a fixed map, by name from a queue of per-call answers. */
    static final class Scripted extends AdminTopicIdentitySource {
        final Map<UUID, Object> byId = new HashMap<>();
        final Queue<Map<String, Object>> byNameAnswers = new ArrayDeque<>();
        final List<Set<String>> namesAsked = new ArrayList<>();

        Scripted(Map<UUID, String> declared) {
            super(null, "app-p", declared, Duration.ZERO);
        }

        @Override
        Map<Uuid, KafkaFuture<TopicDescription>> describeByIdFutures(Set<UUID> topicIds) {
            Map<Uuid, KafkaFuture<TopicDescription>> futures = new HashMap<>();
            for (UUID id : topicIds) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                Object answer = byId.get(id);
                if (answer instanceof String name) {
                    future.complete(StartPathFixtures.describedTopic(name, TopicInfo.toKafkaUuid(id), 1));
                } else if (answer instanceof Exception e) {
                    future.completeExceptionally(e);
                } else {
                    future.completeExceptionally(new UnknownTopicIdException("unknown id " + id));
                }
                futures.put(TopicInfo.toKafkaUuid(id), future);
            }
            return futures;
        }

        @Override
        Map<String, Object> describeByNames(Set<String> names, long deadline) {
            namesAsked.add(Set.copyOf(names));
            Map<String, Object> scripted = byNameAnswers.poll();
            return scripted == null ? Map.of() : scripted;
        }
    }

    private static Map<String, Object> nameGone(String name) {
        return Map.of(name, NameVerdictOf.NAME_GONE);
    }

    /** Spells the by-name answers the way {@code describeByNames} returns them. */
    private static final class NameVerdictOf {
        static final Object NAME_GONE = AdminTopicIdentitySource.NameVerdict.NAME_GONE;
        static final Object DENIED = AdminTopicIdentitySource.NameVerdict.DENIED;
        static final Object UNAVAILABLE = AdminTopicIdentitySource.NameVerdict.UNAVAILABLE;
    }

    /** Ids that still describe are alive, and nothing is asked by name for them. */
    @Test
    void liveIdsAreNeverAskedAboutByName() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byId.put(DECLARED_ID, "orders");
        source.byId.put(UPSTREAM_ID, "upstream");
        assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(DECLARED_ID, UPSTREAM_ID)),
                "ids that describe are alive");
        assertEquals(List.of(), source.namesAsked, "a live id needs no corroboration");
    }

    /**
     * An id unknown by id whose name is unknown too, three consistent answers running, is
     * deleted; one whose name resolves to another id is recreated (its incarnation dead).
     */
    @Test
    void consistentNameGoneAnswersConfirmDeletionAndConsistentOtherIdAnswersConfirmRecreation() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byId.put(UPSTREAM_ID, "upstream");
        assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(UPSTREAM_ID)), "staging: learns 'upstream'");

        source.byId.clear();
        for (int answer = 0; answer < AdminTopicIdentitySource.CORROBORATING_ANSWERS; answer++) {
            Map<String, Object> answers = new HashMap<>(nameGone("upstream"));
            answers.put("orders", NEW_ID);
            source.byNameAnswers.add(answers);
        }
        TopicIdentityVerdicts verdicts = source.resolve(Set.of(DECLARED_ID, UPSTREAM_ID));
        assertEquals(Set.of(UPSTREAM_ID), verdicts.deleted(), "the learned name is gone: deleted");
        assertEquals(Set.of(DECLARED_ID), verdicts.recreated(), "the declared name resolves elsewhere: recreated");
        assertEquals(AdminTopicIdentitySource.CORROBORATING_ANSWERS, source.namesAsked.size(),
                "a verdict takes every corroborating answer, none fewer");
    }

    /**
     * An id whose name the source never learned is never confirmed dead (D75): a Describe
     * denial masks a live topic as unknown by id, and with no name there is no answer that
     * could tell the two apart. Its causes stay expressed, at the cost of expression size.
     */
    @Test
    void aNamelessUnknownIdIsNeverConfirmedDead() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byId.put(DECLARED_ID, "orders");
        assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(DECLARED_ID, NAMELESS_ID)),
                "a nameless unknown id is never confirmed dead");
        assertEquals(List.of(), source.namesAsked, "nothing to corroborate against, nothing asked");
    }

    /**
     * A denied or a stale by-id answer keeps the id alive: the name resolving to the very id
     * asked about proves the by-id answer stale, and absence of evidence never confirms
     * death (D44). Both are answers, so nothing is left to ask again.
     */
    @Test
    void deniedAndSameIdAnswersKeepTheIdAliveAndCountAsAnswered() throws Exception {
        for (Object answer : List.of(NameVerdictOf.DENIED, DECLARED_ID)) {
            Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
            for (int i = 0; i < AdminTopicIdentitySource.CORROBORATING_ANSWERS; i++) {
                source.byNameAnswers.add(Map.of("orders", answer));
            }
            assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(DECLARED_ID)),
                    answer + " keeps the id alive and answers the question");
        }
    }

    /**
     * A by-name describe that times out or fails is no answer: the id is neither convicted
     * nor acquitted but reported unanswered, so the asker keeps its question pending and
     * asks again — exactly as it does when the by-id describe fails (D115). Reading a
     * timeout as "alive" would let one slow corroboration silence a recreation for the
     * life of the task.
     */
    @Test
    void anUnavailableAnswerLeavesTheIdUnansweredRatherThanAlive() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byNameAnswers.add(nameGone("orders"));
        source.byNameAnswers.add(Map.of("orders", NameVerdictOf.UNAVAILABLE));
        TopicIdentityVerdicts verdicts = source.resolve(Set.of(DECLARED_ID));
        assertEquals(Set.of(), verdicts.deleted(), "no verdict either way");
        assertEquals(Set.of(), verdicts.recreated(), "no verdict either way");
        assertEquals(Set.of(DECLARED_ID), verdicts.unanswered(), "the timed-out corroboration leaves the id unanswered");
        assertEquals(2, source.namesAsked.size(), "the corroboration stops at the answer it could not get");
    }

    /**
     * A recreation completing mid-corroboration is still a recreation: the name unknown and
     * the name resolving to another id both say the id asked about is dead, so a run mixing
     * them confirms death, and any answer that resolved the name elsewhere makes the verdict
     * recreated rather than deleted — whichever order the answers arrive in. Reported alive,
     * the task would run on under the dead incarnation, which is the one thing
     * CHANNEL_IDENTITY_CHANGED exists to refuse.
     */
    @Test
    void mixedNameGoneAndOtherIdAnswersConfirmRecreationInEitherOrder() throws Exception {
        Map<String, Object> resolvesElsewhere = Map.of("orders", NEW_ID);
        List<List<Map<String, Object>>> runs = List.of(
                List.of(nameGone("orders"), nameGone("orders"), resolvesElsewhere),
                List.of(resolvesElsewhere, nameGone("orders"), nameGone("orders")),
                List.of(nameGone("orders"), resolvesElsewhere, nameGone("orders")));
        for (List<Map<String, Object>> run : runs) {
            Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
            run.forEach(source.byNameAnswers::add);
            TopicIdentityVerdicts verdicts = source.resolve(Set.of(DECLARED_ID));
            assertEquals(Set.of(DECLARED_ID), verdicts.recreated(),
                    "a name that ever resolved to another id was recreated: " + run);
            assertEquals(Set.of(), verdicts.deleted(), "recreated, not merely deleted: " + run);
            assertEquals(3, source.namesAsked.size(), "every one of the three answers was taken: " + run);
        }
    }

    /**
     * A verdict needs an unbroken run of consistent answers: one contrary answer among the
     * corroborating ones — a metadata view flapping, a topic reappearing — keeps the id
     * alive, and the source stops asking once nothing is left to confirm.
     */
    @Test
    void aContraryAnswerMidCorroborationKeepsTheIdAlive() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byNameAnswers.add(nameGone("orders"));
        source.byNameAnswers.add(Map.of("orders", DECLARED_ID));
        source.byNameAnswers.add(nameGone("orders"));
        assertEquals(TopicIdentityVerdicts.NONE, source.resolve(Set.of(DECLARED_ID)),
                "one contrary answer among the corroborating ones keeps the id alive");
        assertEquals(2, source.namesAsked.size(), "the contrary second answer ends the corroboration early");
    }

    /**
     * A by-id describe failing for any reason other than the substrate's unknown-id answer
     * is not evidence about any id: the whole check is abandoned for this initialisation,
     * and the caller keeps every cause.
     */
    @Test
    void aFailingByIdDescribeAbortsTheCheckRatherThanConcludingAnything() {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders"));
        source.byId.put(DECLARED_ID, new org.apache.kafka.common.errors.TimeoutException("slow broker"));
        source.byId.put(UPSTREAM_ID, "upstream");
        assertThrows(Exception.class, () -> source.resolve(Set.of(DECLARED_ID, UPSTREAM_ID)),
                "a timeout is not an unknown-id answer and must not be read as one");
    }

    /** The substrate's own unknown-topic answers by id, in either spelling, are the ones tolerated. */
    @Test
    void unknownTopicAnswersByIdInEitherSpellingAreToleratedAndCorroboratedByName() throws Exception {
        Scripted source = new Scripted(Map.of(DECLARED_ID, "orders", UPSTREAM_ID, "upstream"));
        source.byId.put(DECLARED_ID, new UnknownTopicOrPartitionException("gone"));
        source.byId.put(UPSTREAM_ID, new UnknownTopicIdException("gone"));
        for (int i = 0; i < AdminTopicIdentitySource.CORROBORATING_ANSWERS; i++) {
            Map<String, Object> answers = new HashMap<>();
            answers.put("orders", NameVerdictOf.NAME_GONE);
            answers.put("upstream", NameVerdictOf.NAME_GONE);
            source.byNameAnswers.add(answers);
        }
        TopicIdentityVerdicts verdicts = source.resolve(Set.of(DECLARED_ID, UPSTREAM_ID));
        assertEquals(Set.of(DECLARED_ID, UPSTREAM_ID), verdicts.deleted(),
                "both unknown-topic spellings by id are corroborated by name and confirmed deleted");
        assertEquals(Set.of(), verdicts.recreated(), "a name that is gone is a deletion, not a recreation");
    }
}
