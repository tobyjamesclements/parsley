package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link ParsleyQuiesceTracker}'s registration/drain bookkeeping directly. Unlike
 * {@link ParsleyQuiesce}, the tracker has no shutdown-arming step: {@link ParsleyQuiesceTracker#allDrained()}
 * reflects drain state alone, since an epoch leave always cares about drain state unconditionally.
 */
class ParsleyQuiesceTrackerTest {

    private static final String MEMBER_0 = "app/0_0";
    private static final String MEMBER_1 = "app/0_1";

    /**
     * With no member ever registered, {@code allDrained} is trivially true — there is nothing whose
     * buffer could still be holding a record, so a member with no local peers may leave at once.
     *
     * Asserts {@code allDrained} is true on a fresh tracker with no registered members.
     */
    @Test
    void allDrainedIsTrueWhenNoMemberIsRegistered() {
        ParsleyQuiesceTracker tracker = new ParsleyQuiesceTracker();
        assertTrue(tracker.allDrained(), "no registered member can be holding anything");
    }

    /**
     * A newly registered member is undrained until it reports an empty buffer — a member must not be
     * considered drained the instant it registers, or a leave would proceed while it still holds records.
     *
     * Asserts {@code allDrained} is false after registration and true once the member reports drained.
     */
    @Test
    void allDrainedWaitsForARegisteredMemberToReportDrained() {
        ParsleyQuiesceTracker tracker = new ParsleyQuiesceTracker();
        tracker.register(MEMBER_0);
        assertFalse(tracker.allDrained(), "the registered member has not reported a drained buffer yet");

        tracker.setDrained(MEMBER_0, true);
        assertTrue(tracker.allDrained(), "the only registered member is drained");
    }

    /**
     * With two registered members, {@code allDrained} must wait for BOTH — one drained member must not
     * clear the leave gate while another still holds buffered records.
     *
     * Asserts {@code allDrained} is false while member 1 is undrained, true once it drains too.
     */
    @Test
    void allDrainedWaitsForEveryRegisteredMember() {
        ParsleyQuiesceTracker tracker = new ParsleyQuiesceTracker();
        tracker.register(MEMBER_0);
        tracker.register(MEMBER_1);
        tracker.setDrained(MEMBER_0, true);

        assertFalse(tracker.allDrained(), "member 1 has not drained yet");

        tracker.setDrained(MEMBER_1, true);
        assertTrue(tracker.allDrained(), "both registered members are now drained");
    }

    /**
     * A member's drained state can toggle back to false — a new record arrives and is held again after
     * previously draining to zero — and {@code allDrained} must reflect that immediately.
     *
     * Asserts {@code allDrained} flips back to false when a previously-drained member un-drains.
     */
    @Test
    void allDrainedFlipsBackWhenAMemberUnDrains() {
        ParsleyQuiesceTracker tracker = new ParsleyQuiesceTracker();
        tracker.register(MEMBER_0);
        tracker.setDrained(MEMBER_0, true);
        assertTrue(tracker.allDrained(), "the only registered member is drained");

        tracker.setDrained(MEMBER_0, false);
        assertFalse(tracker.allDrained(), "the member is holding a record again");
    }

    /**
     * Unregistering a member removes it from consideration — a member that has left must not
     * permanently block {@code allDrained} for the members that remain.
     *
     * Asserts {@code allDrained} becomes true for the remaining drained member once the undrained one
     * unregisters.
     */
    @Test
    void unregisteringAMemberRemovesItFromConsideration() {
        ParsleyQuiesceTracker tracker = new ParsleyQuiesceTracker();
        tracker.register(MEMBER_0);
        tracker.register(MEMBER_1);
        tracker.setDrained(MEMBER_0, true);

        assertFalse(tracker.allDrained(), "member 1 is still registered and undrained");

        tracker.unregister(MEMBER_1);
        assertTrue(tracker.allDrained(), "only member 0 remains registered, and it is drained");
    }
}
