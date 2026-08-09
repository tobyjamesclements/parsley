package io.github.tobyjamesclements.parsley.sim;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.sim.SimWorld.SimChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Deterministic scenarios pinning each criterion's behaviour. The companion {@link SabotageMetaTest} re-runs the
 * shared scenario builders against broken engines to prove each scenario catches its violation.
 */
class TargetedScenarioTest {

    /** A deterministic world plus named processes, wired for one sabotage mode so meta-tests can re-run scenarios broken. */
    static final class Rig {
        final SimWorld world = new SimWorld(7);
        final Oracle oracle = new Oracle();
        final SabotageMode mode;
        final Map<String, SimProcess> procs = new LinkedHashMap<>();
        final Map<String, SimChannel> chans = new LinkedHashMap<>();

        Rig(SabotageMode mode) {
            this.mode = mode;
        }

        SimChannel channel(String name) {
            SimChannel c = world.createChannel(name);
            chans.put(name, c);
            return c;
        }

        SimProcess process(String name, List<SimChannel> received, List<SimChannel> sends, SimProcess.SimLogic logic) {
            SimProcess p = new SimProcess(name, world, oracle, received, sends, logic, mode);
            procs.put(name, p);
            p.start();
            return p;
        }

        SimProcess proc(String name) {
            return procs.get(name);
        }

        Instance external(SimChannel target, String uid) {
            return world.appendExternal(target, (channel, pos) -> new Instance(
                    channel, pos, uid, uid.getBytes(), uid.getBytes(), List.of(), Causes.none(), Set.of()));
        }

        /** A compliant foreign sender that delivered {@code observed} and expresses it, rounded up to {@code expressedAt}. */
        Instance externalCausedBy(SimChannel target, String uid, Instance observed, long expressedAt) {
            Map<ChannelId, Long> meta = new TreeMap<>(observed.meta.byChannel());
            meta.merge(observed.channel, expressedAt, Math::max);
            Set<Instance> causes = new java.util.HashSet<>(observed.trueCauses);
            causes.add(observed);
            byte[] header = CausesCodec.encode(Causes.of(meta));
            return world.appendExternal(target, (channel, pos) -> new Instance(
                    channel, pos, uid, uid.getBytes(), uid.getBytes(),
                    List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)), Causes.of(meta), causes));
        }

        List<String> uidsDelivered(String process) {
            return oracle.committedDeliveries(process).stream().map(i -> i.uid).toList();
        }

        List<String> violationsAfterFinalChecks() {
            oracle.finalChecks();
            oracle.checkAllReceivedDelivered();
            return oracle.violations();
        }

        void assertClean() {
            assertEquals(List.of(), violationsAfterFinalChecks());
        }
    }

    // ---------------------------------------------------------------- Safety 1: the causal diamond across channels

    /**
     * E delivered at p1 causes A; A delivered at p2 causes B. p3 receives both A's and B's channels but gets the
     * effect B first. A correct engine holds B until A is delivered; a broken one delivers them causally inverted,
     * which the oracle flags at the final check.
     */
    static Rig diamond(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c0 = rig.channel("c0");
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p1 = rig.process("p1", List.of(c0), List.of(c1), d -> List.of(c1));
        SimProcess p2 = rig.process("p2", List.of(c1), List.of(c2), d -> List.of(c2));
        SimProcess p3 = rig.process("p3", List.of(c1, c2), List.of(), d -> List.of());

        rig.external(c0, "E");
        p1.feedOne(c0);
        p1.drain();
        p1.commitStep();

        p2.feedOne(c1);
        p2.drain();
        p2.commitStep();

        p3.feedOne(c2);   // the effect arrives first
        p3.drain();
        p3.feedOne(c1);   // then its cause
        p3.drain();
        p3.commitStep();
        return rig;
    }

    @Test
    void effectIsHeldUntilItsCauseIsDelivered() {
        Rig rig = diamond(SabotageMode.NONE);
        assertEquals(List.of("E>p1>c1", "E>p1>c1>p2>c2"), rig.uidsDelivered("p3"),
                "cause must deliver before effect even though the effect arrived first");
        rig.assertClean();
    }

    @Test
    void effectStaysHeldWhileCauseIsMissing() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c0 = rig.channel("c0");
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p1 = rig.process("p1", List.of(c0), List.of(c1), d -> List.of(c1));
        SimProcess p2 = rig.process("p2", List.of(c1), List.of(c2), d -> List.of(c2));
        SimProcess p3 = rig.process("p3", List.of(c1, c2), List.of(), d -> List.of());

        rig.external(c0, "E");
        p1.feedOne(c0);
        p1.drain();
        p1.commitStep();
        p2.feedOne(c1);
        p2.drain();
        p2.commitStep();

        p3.feedOne(c2);
        assertEquals(0, p3.drain(), "the effect must be held while its cause is undelivered");
        p3.commitStep();
        assertEquals(List.of(), rig.uidsDelivered("p3"));
    }

    // ---------------------------------------------------------------- Safety 3: FIFO hold behind a blocked head

    static Rig fifoHold(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c2, "A");                 // the cause, on c2
        rig.externalCausedBy(c1, "M1", a, a.position);      // M1 depends on A
        rig.external(c1, "M2");                             // M2 has no causes but sits behind M1

        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();      // honest engine: nothing deliverable — M1 blocked, M2 held behind it
        p.feedOne(c2);  // the cause arrives
        p.drain();
        p.commitStep();
        return rig;
    }

    @Test
    void laterMessageOnSameChannelIsHeldBehindBlockedHead() {
        Rig rig = fifoHold(SabotageMode.NONE);
        assertEquals(List.of("A", "M1", "M2"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Safety 2: operator rewind must not redeliver

    static Rig rewindDedupe(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        rig.external(c1, "M1");
        rig.external(c1, "M2");
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.stopCleanly();
        p.rewindCommitted(c1, 2); // operator resets offsets; the host will re-feed both messages
        p.start();
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        return rig;
    }

    @Test
    void rewoundFeedIsDroppedNotRedelivered() {
        Rig rig = rewindDedupe(SabotageMode.NONE);
        assertEquals(List.of("M1", "M2"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations());
    }

    // ---------------------------------------------------------------- Liveness 3: cause position that never yields

    @Test
    void causeOnPositionThatNeverYieldsResolvesFromReadPositionReport() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance e1 = rig.external(c1, "E1");     // c1@0
        rig.world.appendDead(c1);                  // c1@1: aborted transaction, never yields
        rig.world.appendDead(c1);                  // c1@2: aborted transaction, never yields
        rig.externalCausedBy(c2, "B", e1, 2);      // B truthfully expresses its cause rounded up to c1@2

        p.feedOne(c1);
        p.drain();                                  // E1 delivered; fedUpTo(c1)=0
        p.feedOne(c2);
        assertEquals(0, p.drain(), "B must wait: positions 1..2 are not yet known to be empty");

        // No further message ever arrives on c1. The host's read position advances past the trailing dead run and is
        // reported; that report — not elapsed time — is what frees B.
        p.feedOne(c1);                              // advances the read position, feeds nothing
        p.commitStep();                             // the advance is committed, hence reported
        p.ingestFacts();
        assertEquals(1, p.drain(), "B must deliver once the report covers the dead positions");
        p.commitStep();
        assertEquals(List.of("E1", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Liveness 4: cause on a channel not received

    @Test
    void causeOutsideReceivedChannelSetDoesNotBlock() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel other = rig.channel("other");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());

        Instance a = rig.external(other, "A");
        rig.externalCausedBy(c1, "B", a, a.position);
        p.feedOne(c1);
        assertEquals(1, p.drain(), "a cause on a channel this process does not receive must not block");
        p.commitStep();
        assertEquals(List.of("B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Safety 6: no metadata means no causes

    @Test
    void messageWithoutMetadataDeliversImmediately() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        rig.external(c1, "plain");
        p.feedOne(c1);
        assertEquals(1, p.drain());
        p.commitStep();
        assertEquals(List.of("plain"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Safety 7: undecodable metadata fails closed

    static Rig undecodableMetadata(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        rig.process("p", List.of(c1), List.of(), d -> List.of());
        rig.world.appendExternal(c1, (channel, pos) -> new Instance(
                channel, pos, "garbage", null, "v".getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, new byte[] {99, 1, 2, 3})), Causes.none(), Set.of()));
        rig.external(c1, "after");
        return rig;
    }

    @Test
    void undecodableMetadataFailsClosedAndNothingDeliversPastIt() {
        Rig rig = undecodableMetadata(SabotageMode.NONE);
        SimProcess p = rig.proc("p");
        SimChannel c1 = rig.chans.get("c1");
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> p.feedOne(c1));
        assertEquals(ParsleyFailClosedException.Reason.UNDECODABLE_METADATA, e.reason());
        assertEquals(List.of(), rig.uidsDelivered("p"), "a failure is never converted into a delivery");
    }

    // ---------------------------------------------------------------- Safety 8: truncation past the read position

    /** Runs up to the truncation; the honest engine must refuse at the next fact ingestion. */
    static Rig truncation(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        for (int i = 0; i < 5; i++) {
            rig.external(c1, "m" + i);
        }
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.commitStep();                 // read position committed at 2
        rig.world.truncate(c1, 4);      // messages m2, m3 discarded before ever being read
        return rig;
    }

    @Test
    void truncationBeyondReadPositionFailsClosed() {
        Rig rig = truncation(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, e.reason());
    }

    // ---------------------------------------------------------------- Liveness 5 / restart: held survives restart

    static Rig heldSurvivesRestart(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c1, "A");                 // will arrive late
        rig.externalCausedBy(c2, "B", a, a.position);       // depends on A

        p.feedOne(c2);      // B received, held (A not yet delivered)
        p.drain();
        p.commitStep();     // the read position over B commits; B lives only in the ordering store now
        p.stopCleanly();
        p.start();          // full re-initialisation from committed state
        p.feedOne(c1);      // A arrives
        p.drain();
        p.commitStep();
        return rig;
    }

    @Test
    void heldMessageIsDeliveredAfterRestart() {
        Rig rig = heldSurvivesRestart(SabotageMode.NONE);
        assertEquals(List.of("A", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Structural 16: removing a channel with held messages

    /** Runs up to the redeclaration; the honest engine must refuse the restart. */
    static Rig removeChannelWithHeld(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());
        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);
        p.feedOne(c2);      // B held on c2
        p.commitStep();
        p.stopCleanly();
        p.redeclare(List.of(c1)); // the new execution's declaration removes c2, which still holds B
        return rig;
    }

    @Test
    void executionRemovingChannelWithHeldMessagesIsRefused() {
        Rig rig = removeChannelWithHeld(SabotageMode.NONE);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").start());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES, e.reason());
    }

    // ---------------------------------------------------------------- Structural 16: leave and rejoin never re-enters past

    @Test
    void channelLeavingAndRejoiningDoesNotRedeliver() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());
        rig.external(c1, "M1");
        p.feedOne(c1);
        p.drain();
        p.stopCleanly();

        p.redeclare(List.of(c2));   // c1 leaves; no held messages on it, so the execution is admissible
        p.start();
        p.commitStep();
        p.stopCleanly();

        p.redeclare(List.of(c1, c2)); // c1 rejoins; its committed offsets meanwhile expired
        p.rewindCommitted(c1, 10);
        p.start();
        p.feedOne(c1);                // the host re-feeds M1
        p.drain();
        p.commitStep();

        assertEquals(List.of("M1"), rig.uidsDelivered("p"), "delivered past must not be re-entered on rejoin");
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Structural 16 / Safety 1: joining never inverts

    @Test
    void channelJoiningDoesNotDeliverCausesBehindDeliveredEffects() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c2), List.of(), d -> List.of()); // c1 not received yet

        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);

        p.feedOne(c2);
        assertEquals(1, p.drain(), "a cause on a channel outside the received set does not block");
        p.stopCleanly();

        p.redeclare(List.of(c1, c2)); // the cause's channel joins the received set
        p.start();
        p.feedOne(c1);                // the host feeds A
        p.drain();
        p.commitStep();
        Scenario.quiesce(List.of(p));

        // Delivering A now would put the cause after its already-delivered effect over the process's lifetime
        // (SPEC Safety 1); the delivered causal past must not be re-entered (SPEC Structural 16).
        assertEquals(List.of("B"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations());
    }

    // ---------------------------------------------------------------- deleted topic: deps resolve, holds still deliver

    @Test
    void depsOnDeletedChannelResolveOnceItsFeedIsExhausted() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance e1 = rig.external(c1, "E1");
        rig.world.appendDead(c1);
        rig.externalCausedBy(c2, "B", e1, 1);   // expressed rounded up to c1@1, which will never yield

        p.feedOne(c1);
        p.drain();                               // E1 delivered
        p.feedOne(c2);
        assertEquals(0, p.drain(), "B waits on c1@1");

        rig.world.killChannel(c1);               // topic deleted: no position on c1 will ever yield again
        p.ingestFacts();
        assertEquals(1, p.drain(), "channel death settles every remaining position");
        p.commitStep();
        assertEquals(List.of("E1", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Safety 9: dead channel with undelivered holds

    /** Runs up to the deletion of a received topic from which a message is still held undelivered; the honest
     * engine must refuse at the next fact ingestion — senders that delivered from the dead channel may already
     * have discarded its causes from their metadata (SPEC Structural 13 allows it), so arriving effects can no
     * longer be ordered against the held message. */
    static Rig deadChannelWithHeldMessages(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());
        Instance a = rig.external(c1, "A");                  // the cause, arrives later
        rig.externalCausedBy(c2, "B", a, a.position);        // B (on c2) depends on A: held until A delivers
        p.feedOne(c2);
        p.drain();
        p.commitStep();
        rig.world.killChannel(c2);                           // c2 deleted while B is still held from it
        return rig;
    }

    @Test
    void deletingAChannelWithUndeliveredHeldMessagesFailsClosed() {
        Rig rig = deadChannelWithHeldMessages(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());
        assertEquals(List.of(), rig.uidsDelivered("p"), "nothing may deliver past the held message");
    }

    /**
     * The full shape of the hazard (ASSESSMENT 1.4, found live by the property generator): upstream q delivers from
     * a channel and, after the channel dies, legally prunes it from its metadata — its emissions then express
     * nothing about it. A downstream process still holding an undelivered message from that channel would deliver
     * the effect past its held cause if it kept running; Safety 9 obliges it to stop instead.
     */
    @Test
    void effectFromPrunedDeadChannelCannotDeliverPastItsHeldCause() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel cx = rig.channel("cx");   // will die
        SimChannel c9 = rig.channel("c9");   // never-satisfied cause keeps p's hold in place
        SimChannel cq = rig.channel("cq");   // q's output
        SimProcess q = rig.process("q", List.of(cx), List.of(cq), d -> List.of(cq));
        SimProcess p = rig.process("p", List.of(cx, c9, cq), List.of(), d -> List.of());

        Instance x0 = rig.external(cx, "X0");
        rig.external(c9, "N");                                   // exists so c9 stays alive, never fed far enough
        q.feedOne(cx);
        q.drain();                                                // q delivers X0, emits E expressing {cx@0}
        q.commitStep();

        p.feedOne(cx);                                            // p delivers X0 too
        p.drain();
        Map<ChannelId, Long> blocked = Map.of(c9.id(), 9L);
        byte[] header = CausesCodec.encode(Causes.of(blocked));
        rig.world.appendExternal(cx, (channel, pos) -> new Instance(
                channel, pos, "X1", "X1".getBytes(), "X1".getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)), Causes.of(blocked), Set.of()));
        p.feedOne(cx);                                            // X1 held: blocked on c9@9
        p.commitStep();

        rig.world.killChannel(cx);
        q.ingestFacts();                                          // q: nothing held from cx — prunes it, legally
        q.drain();
        q.commitStep();

        // p still holds X1 from the dead channel: it must refuse, not deliver q's future effects past X1.
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());
        assertEquals(List.of("X0"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations(), "no safety violation: the refusal preserved causal order");
    }

    // ---------------------------------------------------------------- recreated topic: mid-run identity change

    /** Runs up to the recreation of a received topic; the honest engine must refuse at the next fact ingestion —
     * the feed path subscribes by name, so from this moment it may carry the new channel under the old identity. */
    static Rig recreatedTopic(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        rig.external(c1, "m0");
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        rig.world.recreateTopic(c1);
        return rig;
    }

    @Test
    void recreatedReceivedTopicFailsClosedMidRun() {
        Rig rig = recreatedTopic(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason());
        assertEquals(List.of("m0"), rig.uidsDelivered("p"), "delivery stops at the recreation, nothing is lost");
    }

    // ---------------------------------------------------------------- recreated topic: dep on the dead incarnation

    @Test
    void depOnDeadIncarnationOfRecreatedTopicDoesNotBlock() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel oldC1 = rig.channel("c1-old");
        for (int i = 0; i < 6; i++) {
            rig.world.appendDead(oldC1); // positions 0..5 assigned on the old incarnation
        }
        rig.world.killChannel(oldC1);
        SimChannel newC1 = rig.channel("c1"); // same topic name in Kafka terms; a different channel
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(newC1, c2), List.of(), d -> List.of());

        // A message expressing a cause on the old incarnation: not in the received-channel set (different topic ID).
        Map<ChannelId, Long> meta = Map.of(oldC1.id(), 5L);
        byte[] header = CausesCodec.encode(Causes.of(meta));
        rig.world.appendExternal(c2, (channel, pos) -> new Instance(
                channel, pos, "B", "B".getBytes(), "B".getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)), Causes.of(meta), Set.of()));

        p.feedOne(c2);
        assertEquals(1, p.drain(), "the old incarnation is a different channel outside the received set");
        p.commitStep();
        assertEquals(List.of("B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- self-channel: cycles to self deliver, never self-depend

    @Test
    void selfChannelDeliversAndNeverDependsOnItself() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c0 = rig.channel("c0");
        SimChannel cs = rig.channel("cs");
        // p receives its own send channel; emits one hop per delivery up to depth 2.
        SimProcess p = rig.process("p", List.of(c0, cs), List.of(cs),
                d -> d.uid.chars().filter(ch -> ch == '>').count() / 2 < 2 ? List.of(cs) : List.of());

        rig.external(c0, "E");
        p.feedOne(c0);
        p.drain();          // delivers E, emits E>p>cs (pending)
        p.commitStep();     // becomes visible on cs
        p.feedOne(cs);
        p.drain();          // delivers E>p>cs, emits E>p>cs>p>cs
        p.commitStep();
        p.feedOne(cs);
        p.drain();
        p.commitStep();

        assertEquals(List.of("E", "E>p>cs", "E>p>cs>p>cs"), rig.uidsDelivered("p"));
        rig.assertClean(); // includes the Structural 14 check on every committed emission
    }

    // ---------------------------------------------------------------- Structural 15: causes of held messages are re-expressed

    static Rig receiptCausesReexpressed(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimChannel c3 = rig.channel("c3");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(c3), d -> d.uid.equals("E") ? List.of(c3) : List.of());
        SimProcess q = rig.process("q", List.of(c1, c3), List.of(), d -> List.of());

        rig.external(c1, "E");                       // c1@0, no causes
        Instance a1 = rig.external(c1, "A1");        // c1@1
        rig.externalCausedBy(c2, "B", a1, 1);        // B depends on A1 = c1@1

        p.feedOne(c2);   // B received and HELD (A1 not delivered at p): its causes now bind p's sends
        p.feedOne(c1);   // E received
        p.drain();       // E delivered -> p emits S to c3; S must express (c1,1) learned from held B
        p.commitStep();

        // q receives c1 and c3 and gets S before A1; if S under-expressed its causes, q delivers S before A1.
        q.feedOne(c3);
        q.drain();
        q.feedOne(c1);   // E
        q.feedOne(c1);   // A1
        q.drain();
        q.commitStep();
        Scenario.quiesce(List.of(rig.proc("p"), rig.proc("q")));
        return rig;
    }

    @Test
    void causesKnownOnlyFromHeldMetadataAreExpressedOnSends() {
        Rig rig = receiptCausesReexpressed(SabotageMode.NONE);
        assertEquals(List.of("E", "A1", "E>p>c3"), rig.uidsDelivered("q"),
                "q must deliver A1 before p's emission, whose held-metadata cause binds it");
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Liveness 1: every fed message is owed

    /** Four plain messages on one channel: every one fed in a committed step must be delivered. An engine that
     * consumes a position and silently discards its message (the drop the oracle could not see while it observed
     * only accepted receipts) leaves an owed delivery missing. */
    static Rig fourOnOneChannel(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        for (int i = 0; i < 4; i++) {
            rig.external(c1, "m" + i);
        }
        for (int i = 0; i < 4; i++) {
            p.feedOne(c1);
        }
        p.drain();
        p.commitStep();
        return rig;
    }

    @Test
    void everyFedMessageIsDelivered() {
        Rig rig = fourOnOneChannel(SabotageMode.NONE);
        assertEquals(List.of("m0", "m1", "m2", "m3"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- expression stays within what the sender knew

    /** A process whose fed-or-never frontier runs past its causal knowledge (dead positions beyond the last
     * delivery): emissions must express only what was delivered or seen expressed, never the fed frontier. */
    static Rig expressionUpperBound(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimChannel c3 = rig.channel("c3");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(c3),
                d -> d.uid.equals("F") ? List.of(c3) : List.of());

        rig.external(c1, "E");      // c1@0
        rig.world.appendDead(c1);   // c1@1: never a message
        rig.world.appendDead(c1);   // c1@2: never a message
        rig.external(c2, "F");      // c2@0

        p.feedOne(c1);
        p.drain();                  // E delivered
        p.feedOne(c1);              // advances the read position past the trailing dead run, feeds nothing
        p.commitStep();
        p.ingestFacts();            // fedUpTo(c1) now covers the dead run: fed-or-never, but no cause
        p.feedOne(c2);
        p.drain();                  // F delivered; the emission must express {c1@0, c2@0}, not c1@2
        p.commitStep();
        return rig;
    }

    @Test
    void emissionsExpressOnlyDeliveredAndReceivedCauses() {
        Rig rig = expressionUpperBound(SabotageMode.NONE);
        assertEquals(List.of("E", "F"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Safety 8: truncation at the exact boundaries

    @Test
    void truncationDiscardingExactlyOneUnreadPositionFailsClosed() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        for (int i = 0; i < 5; i++) {
            rig.external(c1, "m" + i);
        }
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.commitStep();                 // covered up to position 1; read position committed at 2
        rig.world.truncate(c1, 3);      // exactly one unread position (2) discarded: the smallest violation
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, p::ingestFacts);
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, e.reason());
    }

    @Test
    void truncationUpToExactlyTheCoveredPositionIsRetention() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimProcess p = rig.process("p", List.of(c1), List.of(), d -> List.of());
        for (int i = 0; i < 5; i++) {
            rig.external(c1, "m" + i);
        }
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.commitStep();                 // covered up to position 1; read position committed at 2
        rig.world.truncate(c1, 2);      // nothing unread discarded: retention, not a violation
        p.ingestFacts();                // must not refuse
        Scenario.quiesce(List.of(p));
        assertEquals(List.of("m0", "m1", "m2", "m3", "m4"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- Structural 16: join clamp survives retention

    /**
     * The delivered-past clamp entry sits exactly at the earliest retained position: retention up to it discards
     * only positions below it, so the entry still matters — position k itself remains readable, and re-entering it
     * on a joining channel would deliver a cause behind its already-delivered effect (SPEC Safety 1 lifetime order).
     * Over-pruning by one position here is invisible to everything but this shape.
     */
    @Test
    void joinClampSurvivesRetentionAtItsExactPosition() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess q = rig.process("q", List.of(c1), List.of(c2), d -> d.uid.equals("A2") ? List.of(c2) : List.of());
        SimProcess p = rig.process("p", List.of(c2), List.of(), d -> List.of()); // c1 not received yet

        for (int i = 0; i < 3; i++) {
            rig.external(c1, "A" + i);  // c1@0..2
        }
        q.feedOne(c1);
        q.feedOne(c1);
        q.feedOne(c1);
        q.drain();                       // delivers A0..A2, emits E to c2 expressing {c1@2}
        q.commitStep();

        p.feedOne(c2);
        p.drain();                       // E delivered; its cause c1@2 is vacuous here (c1 not received)
        p.commitStep();

        rig.world.truncate(c1, 2);       // retention up to exactly the clamp's position
        p.ingestFacts();                 // the delivered-past entry c1@2 must survive: 2 is still readable
        p.commitStep();
        p.stopCleanly();

        p.redeclare(List.of(c1, c2));    // the cause's channel joins
        p.start();
        Scenario.quiesce(List.of(p, q));

        // A2 (= c1@2) must not be delivered at p: it is the already-delivered effect E's cause.
        assertEquals(List.of("A2>q>c2"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- restart fidelity: held content is exact

    /** A held message with an application header, persisted across a restart, must reach application logic with
     * key, value, headers and timestamp exactly as received ({@code SimProcess.drain} asserts this on every
     * delivery; this shape forces the restored-from-store path at a position whose timestamp is non-zero). */
    @Test
    void restoredHeldMessageReachesLogicWithContentIntact() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c1, "A");                       // the cause, arrives late
        rig.external(c2, "pad");                                  // c2@0, so B sits at a non-zero position
        Map<ChannelId, Long> meta = Map.of(c1.id(), a.position);
        byte[] header = CausesCodec.encode(Causes.of(meta));
        rig.world.appendExternal(c2, (channel, pos) -> new Instance(
                channel, pos, "B", "Bk".getBytes(), "Bv".getBytes(),
                List.of(new HeaderKV("app.header", new byte[] {7}),
                        new HeaderKV(CausesCodec.HEADER_KEY, header)),
                Causes.of(meta), Set.of(a)));                     // B@1 with an application header

        p.feedOne(c2);      // pad
        p.feedOne(c2);      // B, held (A undelivered)
        p.drain();
        p.commitStep();     // B lives only in the ordering store now
        p.stopCleanly();
        p.start();          // full re-initialisation: B restores from the store
        p.feedOne(c1);      // A arrives
        p.drain();          // delivers A, then B — drain asserts B's key/value/headers/timestamp are exact
        p.commitStep();

        assertEquals(List.of("pad", "A", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    // ---------------------------------------------------------------- 2-safety 2: restart is not observable

    private enum RestartMode { NONE, AFTER_COMMIT, MID_STEP }

    @Test
    void restartDoesNotChangeWhatIsDeliveredOrItsOrder() {
        List<String> without = deliveriesWithRestart(RestartMode.NONE);
        assertEquals(without, deliveriesWithRestart(RestartMode.AFTER_COMMIT),
                "a restart between steps must not be observable in deliveries or their order");
        assertEquals(without, deliveriesWithRestart(RestartMode.MID_STEP),
                "a crash that discards uncommitted receipts, deliveries and emissions must not be observable either");
    }

    private static List<String> deliveriesWithRestart(RestartMode mode) {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimChannel c3 = rig.channel("c3");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(c3),
                d -> d.uid.equals("A") ? List.of(c3) : List.of());

        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);
        rig.external(c2, "C");
        rig.external(c1, "D");

        p.feedOne(c2);      // B held on A
        p.feedOne(c2);      // C held behind B
        if (mode != RestartMode.MID_STEP) {
            p.commitStep();
        }
        if (mode == RestartMode.AFTER_COMMIT) {
            p.crash();      // abort (nothing pending) and full re-initialisation
            p.start();
        }
        p.feedOne(c1);      // A
        p.drain();          // delivers A, emits to c3 — all uncommitted in MID_STEP mode
        if (mode == RestartMode.MID_STEP) {
            p.crash();      // discards the uncommitted receipts, deliveries and the emission
            p.start();
            p.feedOne(c2);  // the host re-feeds everything whose read position was not committed
            p.feedOne(c2);
            p.feedOne(c1);
        }
        p.feedOne(c1);      // D
        p.drain();
        p.commitStep();
        Scenario.quiesce(List.of(p));
        rig.assertClean();
        return rig.uidsDelivered("p");
    }
}
