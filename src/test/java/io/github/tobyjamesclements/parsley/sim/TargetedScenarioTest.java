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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins individual scenarios the random sweep reaches only rarely.
 *
 * <p>Each case names a specific ordering hazard and fails if it returns.
 */
class TargetedScenarioTest {
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

        p3.feedOne(c2);
        p3.drain();
        p3.feedOne(c1);
        p3.drain();
        p3.commitStep();
        return rig;
    }

    /** Effect is held until its cause is delivered. */
    @Test
    void effectIsHeldUntilItsCauseIsDelivered() {
        Rig rig = diamond(SabotageMode.NONE);
        assertEquals(List.of("E>p1>c1", "E>p1>c1>p2>c2"), rig.uidsDelivered("p3"),
                "cause must deliver before effect even though the effect arrived first");
        rig.assertClean();
    }

    /** Effect stays held while cause is missing. */
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

    static Rig fifoHold(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c2, "A");
        rig.externalCausedBy(c1, "M1", a, a.position);
        rig.external(c1, "M2");

        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.feedOne(c2);
        p.drain();
        p.commitStep();
        return rig;
    }

    /** Later message on same channel is held behind blocked head. */
    @Test
    void laterMessageOnSameChannelIsHeldBehindBlockedHead() {
        Rig rig = fifoHold(SabotageMode.NONE);
        assertEquals(List.of("A", "M1", "M2"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

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
        p.rewindCommitted(c1, 2);
        p.start();
        p.feedOne(c1);
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        return rig;
    }

    /** Rewound feed is dropped not redelivered. */
    @Test
    void rewoundFeedIsDroppedNotRedelivered() {
        Rig rig = rewindDedupe(SabotageMode.NONE);
        assertEquals(List.of("M1", "M2"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations());
    }

    /** Cause on position that never yields resolves from read position report. */
    @Test
    void causeOnPositionThatNeverYieldsResolvesFromReadPositionReport() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance e1 = rig.external(c1, "E1");
        rig.world.appendDead(c1);
        rig.world.appendDead(c1);
        rig.externalCausedBy(c2, "B", e1, 2);

        p.feedOne(c1);
        p.drain();
        p.feedOne(c2);
        assertEquals(0, p.drain(), "B must wait: positions 1..2 are not yet known to be empty");

        p.feedOne(c1);
        p.commitStep();
        p.ingestFacts();
        assertEquals(1, p.drain(), "B must deliver once the report covers the dead positions");
        p.commitStep();
        assertEquals(List.of("E1", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    /** Cause outside received channel set does not block. */
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

    /** Message without metadata delivers immediately. */
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

    /** Undecodable metadata fails closed and nothing delivers past it. */
    @Test
    void undecodableMetadataFailsClosedAndNothingDeliversPastIt() {
        Rig rig = undecodableMetadata(SabotageMode.NONE);
        SimProcess p = rig.proc("p");
        SimChannel c1 = rig.chans.get("c1");
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> p.feedOne(c1));
        assertEquals(ParsleyFailClosedException.Reason.UNDECODABLE_METADATA, e.reason());
        assertEquals(List.of(), rig.uidsDelivered("p"), "a failure is never converted into a delivery");
    }

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
        p.commitStep();
        rig.world.truncate(c1, 4);
        return rig;
    }

    /** Truncation beyond read position fails closed. */
    @Test
    void truncationBeyondReadPositionFailsClosed() {
        Rig rig = truncation(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, e.reason());
    }

    static Rig heldSurvivesRestart(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);

        p.feedOne(c2);
        p.drain();
        p.commitStep();
        p.stopCleanly();
        p.start();
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        return rig;
    }

    /** Held message is delivered after restart. */
    @Test
    void heldMessageIsDeliveredAfterRestart() {
        Rig rig = heldSurvivesRestart(SabotageMode.NONE);
        assertEquals(List.of("A", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    static Rig removeChannelWithHeld(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());
        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);
        p.feedOne(c2);
        p.commitStep();
        p.stopCleanly();
        p.redeclare(List.of(c1));
        return rig;
    }

    /** Execution removing channel with held messages is refused. */
    @Test
    void executionRemovingChannelWithHeldMessagesIsRefused() {
        Rig rig = removeChannelWithHeld(SabotageMode.NONE);
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").start());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES, e.reason());
    }

    /** Channel leaving and rejoining does not redeliver. */
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

        p.redeclare(List.of(c2));
        p.start();
        p.commitStep();
        p.stopCleanly();

        p.redeclare(List.of(c1, c2));
        p.rewindCommitted(c1, 10);
        p.start();
        p.feedOne(c1);
        p.drain();
        p.commitStep();

        assertEquals(List.of("M1"), rig.uidsDelivered("p"), "delivered past must not be re-entered on rejoin");
        rig.assertClean();
    }

    /** Channel joining does not deliver causes behind delivered effects. */
    @Test
    void channelJoiningDoesNotDeliverCausesBehindDeliveredEffects() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c2), List.of(), d -> List.of());

        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);

        p.feedOne(c2);
        assertEquals(1, p.drain(), "a cause on a channel outside the received set does not block");
        p.stopCleanly();

        p.redeclare(List.of(c1, c2));
        p.start();
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        Scenario.quiesce(List.of(p));

        assertEquals(List.of("B"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations());
    }

    /** Deps on deleted channel resolve once its feed is exhausted. */
    @Test
    void depsOnDeletedChannelResolveOnceItsFeedIsExhausted() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance e1 = rig.external(c1, "E1");
        rig.world.appendDead(c1);
        rig.externalCausedBy(c2, "B", e1, 1);

        p.feedOne(c1);
        p.drain();
        p.feedOne(c2);
        assertEquals(0, p.drain(), "B waits on c1@1");

        rig.world.killChannel(c1);
        p.ingestFacts();
        assertEquals(1, p.drain(), "channel death settles every remaining position");
        p.commitStep();
        assertEquals(List.of("E1", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    static Rig deadChannelWithHeldMessages(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());
        Instance a = rig.external(c1, "A");
        rig.externalCausedBy(c2, "B", a, a.position);
        p.feedOne(c2);
        p.drain();
        p.commitStep();
        rig.world.killChannel(c2);
        return rig;
    }

    /** Deleting a channel with undelivered held messages fails closed. */
    @Test
    void deletingAChannelWithUndeliveredHeldMessagesFailsClosed() {
        Rig rig = deadChannelWithHeldMessages(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());
        assertEquals(List.of(), rig.uidsDelivered("p"), "nothing may deliver past the held message");
    }

    /** Effect from pruned dead channel cannot deliver past its held cause. */
    @Test
    void effectFromPrunedDeadChannelCannotDeliverPastItsHeldCause() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel cx = rig.channel("cx");
        SimChannel c9 = rig.channel("c9");
        SimChannel cq = rig.channel("cq");
        SimProcess q = rig.process("q", List.of(cx), List.of(cq), d -> List.of(cq));
        SimProcess p = rig.process("p", List.of(cx, c9, cq), List.of(), d -> List.of());

        Instance x0 = rig.external(cx, "X0");
        rig.external(c9, "N");
        q.feedOne(cx);
        q.drain();
        q.commitStep();

        p.feedOne(cx);
        p.drain();
        Map<ChannelId, Long> blocked = Map.of(c9.id(), 9L);
        byte[] header = CausesCodec.encode(Causes.of(blocked));
        rig.world.appendExternal(cx, (channel, pos) -> new Instance(
                channel, pos, "X1", "X1".getBytes(), "X1".getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)), Causes.of(blocked), Set.of()));
        p.feedOne(cx);
        p.commitStep();

        rig.world.killChannel(cx);
        q.ingestFacts();
        q.drain();
        q.commitStep();

        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES, e.reason());
        assertEquals(List.of("X0"), rig.uidsDelivered("p"));
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations(), "no safety violation: the refusal preserved causal order");
    }

    /**
     * Retention crosses a message a process still holds (D104). P receives {b, w, x} and holds
     * X (x@0, expressing W on w, unfed at P); Q receives {x, t}, delivers X at once (it does
     * not receive w) and sends to b. Retention then discards x@0 — exactly up to both
     * readers' committed read position, which the fed-based check could not see — and Q's
     * facts round prunes (x, 0) from its frontier as Structural 13 permits. Q delivers T and
     * sends B, which expresses nothing about x. The rig stops before P's facts round: an
     * honest engine refuses there; an engine ignoring truncation goes on to deliver B past
     * held X.
     */
    static Rig retentionCrossesAHeldMessage(SabotageMode mode) {
        Rig rig = new Rig(mode);
        // The drain scans received channels in ChannelId order; the inversion needs b < x.
        List<SimChannel> made = new java.util.ArrayList<>(List.of(
                rig.channel("k0"), rig.channel("k1"), rig.channel("k2"), rig.channel("k3")));
        made.sort(java.util.Comparator.comparing(SimChannel::id));
        SimChannel b = made.get(0);
        SimChannel w = made.get(1);
        SimChannel x = made.get(2);
        SimChannel t = made.get(3);
        rig.chans.put("b", b);
        rig.chans.put("w", w);
        rig.chans.put("x", x);
        rig.chans.put("t", t);

        SimProcess q = rig.process("q", List.of(x, t), List.of(b), d -> d.uid.equals("T") ? List.of(b) : List.of());
        SimProcess p = rig.process("p", List.of(b, w, x), List.of(), d -> List.of());

        Instance wMsg = rig.external(w, "W");
        rig.externalCausedBy(x, "X", wMsg, wMsg.position);

        p.feedOne(x);
        assertEquals(0, p.drain(), "staging: X waits on W at P");
        p.commitStep();

        q.feedOne(x);
        assertEquals(1, q.drain(), "staging: Q does not receive w, so X delivers there at once");
        q.commitStep();

        rig.world.truncate(x, 1);
        q.ingestFacts();
        rig.external(t, "T");
        q.feedOne(t);
        assertEquals(1, q.drain(), "staging: T delivers and B is sent, expressing nothing about x");
        q.commitStep();
        return rig;
    }

    /**
     * Retention discarding a held message fails the holder closed (D104): the facts round
     * that shows the earliest retained position past the head of the hold-back buffer
     * refuses with POSITIONS_DISCARDED_UNREAD, naming the held position, before any effect
     * can be delivered past it. The fed-based check this replaces passed here — the held
     * message had advanced fedUpTo past itself — and the effect B was then delivered before
     * its cause X: the retention dual of D46's deleted channel.
     */
    @Test
    void retentionDiscardingAHeldMessageFailsTheHolderClosed() {
        Rig rig = retentionCrossesAHeldMessage(SabotageMode.NONE);
        SimProcess p = rig.proc("p");
        ParsleyFailClosedException e = assertThrows(ParsleyFailClosedException.class, p::ingestFacts,
                "retention past a held message must stop the holder");
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, e.reason());
        assertTrue(e.getMessage().contains("held message at position 0"),
                "the diagnosis names the held position, got: " + e.getMessage());
        assertEquals(List.of(), rig.uidsDelivered("p"), "nothing was delivered past the hold");
        rig.oracle.finalChecks();
        assertEquals(List.of(), rig.oracle.violations(), "no safety violation: the refusal preserved causal order");
    }

    /**
     * Retention up to exactly the held message's position is ordinary retention: the hold
     * survives and delivers once its cause arrives. Pins the boundary of the D104 check so
     * it cannot loosen into refusing a retained hold.
     */
    @Test
    void retentionUpToExactlyTheHeldMessageIsRetention() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel w = rig.channel("w");
        SimChannel x = rig.channel("x");
        SimProcess p = rig.process("p", List.of(w, x), List.of(), d -> List.of());
        rig.external(x, "X0");
        Instance wMsg = rig.external(w, "W");
        rig.externalCausedBy(x, "X1", wMsg, wMsg.position);
        p.feedOne(x);
        p.drain();
        p.feedOne(x);
        assertEquals(0, p.drain(), "staging: X1 waits on W");
        p.commitStep();

        rig.world.truncate(x, 1);
        assertDoesNotThrow(p::ingestFacts, "retention up to the held message itself discards nothing owed");
        p.feedOne(w);
        p.drain();
        p.commitStep();
        assertEquals(List.of("X0", "W", "X1"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

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

    /** Recreated received topic fails closed mid run. */
    @Test
    void recreatedReceivedTopicFailsClosedMidRun() {
        Rig rig = recreatedTopic(SabotageMode.NONE);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, () -> rig.proc("p").ingestFacts());
        assertEquals(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED, e.reason());
        assertEquals(List.of("m0"), rig.uidsDelivered("p"), "delivery stops at the recreation, nothing is lost");
    }

    /** Dep on dead incarnation of recreated topic does not block. */
    @Test
    void depOnDeadIncarnationOfRecreatedTopicDoesNotBlock() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel oldC1 = rig.channel("c1-old");
        for (int i = 0; i < 6; i++) {
            rig.world.appendDead(oldC1);
        }
        rig.world.killChannel(oldC1);
        SimChannel newC1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(newC1, c2), List.of(), d -> List.of());

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

    /** Self channel delivers and never depends on itself. */
    @Test
    void selfChannelDeliversAndNeverDependsOnItself() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c0 = rig.channel("c0");
        SimChannel cs = rig.channel("cs");

        SimProcess p = rig.process("p", List.of(c0, cs), List.of(cs),
                d -> d.uid.chars().filter(ch -> ch == '>').count() / 2 < 2 ? List.of(cs) : List.of());

        rig.external(c0, "E");
        p.feedOne(c0);
        p.drain();
        p.commitStep();
        p.feedOne(cs);
        p.drain();
        p.commitStep();
        p.feedOne(cs);
        p.drain();
        p.commitStep();

        assertEquals(List.of("E", "E>p>cs", "E>p>cs>p>cs"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    static Rig receiptCausesReexpressed(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimChannel c3 = rig.channel("c3");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(c3), d -> d.uid.equals("E") ? List.of(c3) : List.of());
        SimProcess q = rig.process("q", List.of(c1, c3), List.of(), d -> List.of());

        rig.external(c1, "E");
        Instance a1 = rig.external(c1, "A1");
        rig.externalCausedBy(c2, "B", a1, 1);

        p.feedOne(c2);
        p.feedOne(c1);
        p.drain();
        p.commitStep();

        q.feedOne(c3);
        q.drain();
        q.feedOne(c1);
        q.feedOne(c1);
        q.drain();
        q.commitStep();
        Scenario.quiesce(List.of(rig.proc("p"), rig.proc("q")));
        return rig;
    }

    /** Causes known only from held metadata are expressed on sends. */
    @Test
    void causesKnownOnlyFromHeldMetadataAreExpressedOnSends() {
        Rig rig = receiptCausesReexpressed(SabotageMode.NONE);
        assertEquals(List.of("E", "A1", "E>p>c3"), rig.uidsDelivered("q"),
                "q must deliver A1 before p's emission, whose held-metadata cause binds it");
        rig.assertClean();
    }

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

    /** Every fed message is delivered. */
    @Test
    void everyFedMessageIsDelivered() {
        Rig rig = fourOnOneChannel(SabotageMode.NONE);
        assertEquals(List.of("m0", "m1", "m2", "m3"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    static Rig expressionUpperBound(SabotageMode mode) {
        Rig rig = new Rig(mode);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimChannel c3 = rig.channel("c3");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(c3),
                d -> d.uid.equals("F") ? List.of(c3) : List.of());

        rig.external(c1, "E");
        rig.world.appendDead(c1);
        rig.world.appendDead(c1);
        rig.external(c2, "F");

        p.feedOne(c1);
        p.drain();
        p.feedOne(c1);
        p.commitStep();
        p.ingestFacts();
        p.feedOne(c2);
        p.drain();
        p.commitStep();
        return rig;
    }

    /** Emissions express only delivered and received causes. */
    @Test
    void emissionsExpressOnlyDeliveredAndReceivedCauses() {
        Rig rig = expressionUpperBound(SabotageMode.NONE);
        assertEquals(List.of("E", "F"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    /** Truncation discarding exactly one unread position fails closed. */
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
        p.commitStep();
        rig.world.truncate(c1, 3);
        ParsleyFailClosedException e =
                assertThrows(ParsleyFailClosedException.class, p::ingestFacts);
        assertEquals(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD, e.reason());
    }

    /** Truncation up to exactly the covered position is retention. */
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
        p.commitStep();
        rig.world.truncate(c1, 2);
        p.ingestFacts();
        Scenario.quiesce(List.of(p));
        assertEquals(List.of("m0", "m1", "m2", "m3", "m4"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    /** Join clamp survives retention at its exact position. */
    @Test
    void joinClampSurvivesRetentionAtItsExactPosition() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess q = rig.process("q", List.of(c1), List.of(c2), d -> d.uid.equals("A2") ? List.of(c2) : List.of());
        SimProcess p = rig.process("p", List.of(c2), List.of(), d -> List.of());

        for (int i = 0; i < 3; i++) {
            rig.external(c1, "A" + i);
        }
        q.feedOne(c1);
        q.feedOne(c1);
        q.feedOne(c1);
        q.drain();
        q.commitStep();

        p.feedOne(c2);
        p.drain();
        p.commitStep();

        rig.world.truncate(c1, 2);
        p.ingestFacts();
        p.commitStep();
        p.stopCleanly();

        p.redeclare(List.of(c1, c2));
        p.start();
        Scenario.quiesce(List.of(p, q));

        assertEquals(List.of("A2>q>c2"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    /** Restored held message reaches logic with content intact. */
    @Test
    void restoredHeldMessageReachesLogicWithContentIntact() {
        Rig rig = new Rig(SabotageMode.NONE);
        SimChannel c1 = rig.channel("c1");
        SimChannel c2 = rig.channel("c2");
        SimProcess p = rig.process("p", List.of(c1, c2), List.of(), d -> List.of());

        Instance a = rig.external(c1, "A");
        rig.external(c2, "pad");
        Map<ChannelId, Long> meta = Map.of(c1.id(), a.position);
        byte[] header = CausesCodec.encode(Causes.of(meta));
        rig.world.appendExternal(c2, (channel, pos) -> new Instance(
                channel, pos, "B", "Bk".getBytes(), "Bv".getBytes(),
                List.of(new HeaderKV("app.header", new byte[] {7}),
                        new HeaderKV(CausesCodec.HEADER_KEY, header)),
                Causes.of(meta), Set.of(a)));

        p.feedOne(c2);
        p.feedOne(c2);
        p.drain();
        p.commitStep();
        p.stopCleanly();
        p.start();
        p.feedOne(c1);
        p.drain();
        p.commitStep();

        assertEquals(List.of("pad", "A", "B"), rig.uidsDelivered("p"));
        rig.assertClean();
    }

    private enum RestartMode { NONE, AFTER_COMMIT, MID_STEP }

    /** Restart does not change what is delivered or its order. */
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

        p.feedOne(c2);
        p.feedOne(c2);
        if (mode != RestartMode.MID_STEP) {
            p.commitStep();
        }
        if (mode == RestartMode.AFTER_COMMIT) {
            p.crash();
            p.start();
        }
        p.feedOne(c1);
        p.drain();
        if (mode == RestartMode.MID_STEP) {
            p.crash();
            p.start();
            p.feedOne(c2);
            p.feedOne(c2);
            p.feedOne(c1);
        }
        p.feedOne(c1);
        p.drain();
        p.commitStep();
        Scenario.quiesce(List.of(p));
        rig.assertClean();
        return rig.uidsDelivered("p");
    }
}
