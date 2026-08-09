package io.github.tobyjamesclements.parsley.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.tobyjamesclements.parsley.core.ChannelId;

/**
 * Ground-truth causality bookkeeping, entirely outside the engine. The simulator reports executions starting, feeds,
 * deliveries, sends, commits and rollbacks; the oracle maintains each process's true causal past (delivery adds the
 * message and its causes; a feed adds only its causes, since happened-before passes through receipt), snapshots it as
 * the true cause set of every sent message, and checks the safety criteria over committed history alone — an aborted
 * step's events have not occurred.
 *
 * <p>The oracle observes the <em>feed</em>, not the engine's acceptance: every message fed to the engine is a
 * delivery owed, whether or not the engine acknowledged it, unless the oracle's own bookkeeping shows its delivery is
 * no longer permitted — it was already delivered in a committed step, or it lay in the delivered causal past of the
 * execution that fed it (SPEC Structural 16 forbids re-entering that past; the exemption is computed from true
 * causes, never from expressed metadata, so an over-expressing engine cannot inflate its own excuse).</p>
 */
public final class Oracle {

    /** A committed send together with the world state snapshotted at the moment of sending: the expression upper
     * bound (max position per channel among everything this process had delivered or seen expressed on fed
     * messages), the highest assigned position per expressed channel, and which true causes were already
     * discardable then (below their channel's earliest retained position, or on a dead channel). All are
     * send-time snapshots: appends, kills and truncations between send and commit must neither excuse a
     * genuinely-future or baseless expression nor retroactively excuse an under-expression that was a violation
     * when the stamp was made. */
    record Sent(Instance instance, Map<ChannelId, Long> upperBoundAtSend, Map<ChannelId, Long> lastAssignedAtSend,
                java.util.Set<Instance> excusedAtSend) {
    }

    static final class ProcState {
        final List<Instance> committedDeliveries = new ArrayList<>();
        final Set<Instance> committedPast = new HashSet<>();
        final Set<Instance> committedFedOwed = new HashSet<>();
        final Map<ChannelId, Long> committedExpressible = new HashMap<>();
        /** Per-channel maximum over committed deliveries and their true causes, maintained incrementally at each
         * commit (the max only grows) so restarts do not recompute it from the full history. */
        final Map<ChannelId, Long> committedPastMax = new HashMap<>();
        /** Every instance a committed step actually fed, owed or not: exact ground truth for obligations of the
         * form "a committed step read this very message" (Safety 7) — no read-position proxy, so operator rewinds
         * and repositions onto a truncated log start can neither erase nor fake it. */
        final Set<Instance> committedFed = new HashSet<>();
        final Set<Instance> deltaFed = new HashSet<>();
        final List<Instance> deltaDeliveries = new ArrayList<>();
        final Set<Instance> deltaPast = new HashSet<>();
        final Set<Instance> deltaFedOwed = new HashSet<>();
        final Map<ChannelId, Long> deltaExpressible = new HashMap<>();
        /** The delivered causal past as of this execution's initialisation, summarized per channel as the maximum
         * position over delivered instances and the true causes of delivered instances — the same per-channel-max
         * summary D31 sanctions for the engine's join clamp, but computed from ground truth, never from expressed
         * metadata. Positions at or below it are no longer owed here: delivering them would re-enter delivered
         * causal past (SPEC Structural 16), or they were already delivered. */
        Map<ChannelId, Long> executionExemption = Map.of();
    }

    private final Map<String, ProcState> processes = new LinkedHashMap<>();
    private final List<String> violations = new ArrayList<>();

    private ProcState state(String process) {
        return processes.computeIfAbsent(process, p -> new ProcState());
    }

    /** A full initialisation began: snapshot the delivered causal past that this execution must not re-enter. */
    public void onStart(String process) {
        ProcState st = state(process);
        st.executionExemption = deliveredPastMax(process);
    }

    /** The delivered causal past of a process right now, as the per-channel maximum over its committed deliveries
     * and their true causes — the ground-truth counterpart of the engine's join clamp (D31). A snapshot copy: the
     * underlying max is maintained incrementally at commit and keeps growing. */
    public Map<ChannelId, Long> deliveredPastMax(String process) {
        return new HashMap<>(state(process).committedPastMax);
    }

    /** A message was fed to the engine. Its causes join the causal past regardless of what the engine does with it
     * (happened-before passes through receipt, SPEC Structural 15 — a dropped message was still received), and its
     * delivery is owed unless the delivered causal past at this execution's start already covered its position. */
    public void onFed(String process, Instance instance) {
        ProcState st = state(process);
        st.deltaFed.add(instance);
        st.deltaPast.addAll(instance.trueCauses);
        instance.meta.byChannel().forEach((channel, position) ->
                st.deltaExpressible.merge(channel, position, Math::max));
        if (instance.position > st.executionExemption.getOrDefault(instance.channel, Long.MIN_VALUE)) {
            st.deltaFedOwed.add(instance);
        }
    }

    public void onDelivered(String process, Instance instance) {
        ProcState st = state(process);
        st.deltaDeliveries.add(instance);
        st.deltaPast.add(instance);
        st.deltaPast.addAll(instance.trueCauses);
        st.deltaExpressible.merge(instance.channel, instance.position, Math::max);
    }

    /** The true cause set for a message this process sends right now. */
    public Set<Instance> causalPastSnapshot(String process) {
        ProcState st = state(process);
        Set<Instance> snapshot = new HashSet<>(st.committedPast);
        snapshot.addAll(st.deltaPast);
        return snapshot;
    }

    /** The highest position per channel this process could legitimately express right now: everything it has
     * delivered, and every pair expressed by the metadata of anything fed to it. Anything above is over-expression. */
    public Map<ChannelId, Long> expressionUpperBound(String process) {
        ProcState st = state(process);
        Map<ChannelId, Long> bound = new HashMap<>(st.committedExpressible);
        st.deltaExpressible.forEach((channel, position) -> bound.merge(channel, position, Math::max));
        return bound;
    }

    public void commitStep(String process, List<Sent> appendedNowCommitted) {
        for (Sent sent : appendedNowCommitted) {
            checkExpression(sent);
        }
        ProcState st = state(process);
        for (Instance delivered : st.deltaDeliveries) {
            st.committedPastMax.merge(delivered.channel, delivered.position, Math::max);
            for (Instance cause : delivered.trueCauses) {
                st.committedPastMax.merge(cause.channel, cause.position, Math::max);
            }
        }
        st.committedDeliveries.addAll(st.deltaDeliveries);
        st.committedPast.addAll(st.deltaPast);
        st.committedFedOwed.addAll(st.deltaFedOwed);
        st.committedFed.addAll(st.deltaFed);
        st.deltaExpressible.forEach((channel, position) ->
                st.committedExpressible.merge(channel, position, Math::max));
        rollbackStep(process);
    }

    public void rollbackStep(String process) {
        ProcState st = state(process);
        st.deltaDeliveries.clear();
        st.deltaPast.clear();
        st.deltaFedOwed.clear();
        st.deltaFed.clear();
        st.deltaExpressible.clear();
    }

    /**
     * Checked the moment a send commits: the metadata must express every true cause still at or above its channel's
     * earliest retained position (SPEC Structural 15), name no position that was unassigned at the moment of sending
     * (Structural 12), never depend on the message's own position or above on its own channel (Structural 14), and
     * express nothing above what the sender had delivered or seen expressed at the moment of sending — the upper
     * bound that keeps downstream delivered-past clamps sound (Structural 16 via D31).
     */
    private void checkExpression(Sent sent) {
        Instance instance = sent.instance();
        instance.meta.byChannel().forEach((channel, position) -> {
            if (channel.equals(instance.channel) && position >= instance.position) {
                violations.add("Structural 14: " + instance
                        + " expresses dependency on own channel at or above itself: " + position);
            }
            Long lastAssigned = sent.lastAssignedAtSend().get(channel);
            if (lastAssigned != null && position > lastAssigned) {
                violations.add("Structural 12: " + instance + " expresses position " + channel + "@" + position
                        + " which was unassigned at send time (last assigned: " + lastAssigned + ")");
            }
            Long bound = sent.upperBoundAtSend().get(channel);
            if (bound == null || position > bound) {
                violations.add("Over-expression: " + instance + " expresses " + channel + "@" + position
                        + " above anything its sender had delivered or seen expressed at send time (bound: "
                        + bound + ")");
            }
        });
        for (Instance cause : instance.trueCauses) {
            if (sent.excusedAtSend().contains(cause)) {
                continue; // discardable at the moment of sending (Structural 13) — judged then, never retroactively.
            }
            Long expressed = instance.meta.byChannel().get(cause.channel);
            if (expressed == null || expressed < cause.position) {
                violations.add("Structural 15: " + instance + " fails to express cause " + cause
                        + " (expressed: " + expressed + ")");
            }
        }
    }

    /** Full-history checks, run at quiescence. */
    public void finalChecks() {
        processes.forEach((process, st) -> {
            Map<Instance, Integer> firstIndex = new HashMap<>();
            for (int i = 0; i < st.committedDeliveries.size(); i++) {
                Instance delivered = st.committedDeliveries.get(i);
                Integer previous = firstIndex.putIfAbsent(delivered, i);
                if (previous != null) {
                    violations.add("Safety 2: " + process + " delivered " + delivered + " twice (indexes "
                            + previous + " and " + i + ")");
                }
            }
            Map<ChannelId, Long> lastPerChannel = new HashMap<>();
            for (Instance delivered : st.committedDeliveries) {
                Long last = lastPerChannel.put(delivered.channel, delivered.position);
                if (last != null && delivered.position <= last) {
                    violations.add("Safety 3: " + process + " delivered " + delivered.channel + "@"
                            + delivered.position + " after position " + last + " of the same channel");
                }
            }
            for (int i = 0; i < st.committedDeliveries.size(); i++) {
                Instance effect = st.committedDeliveries.get(i);
                for (Instance cause : effect.trueCauses) {
                    Integer causeIndex = firstIndex.get(cause);
                    if (causeIndex != null && causeIndex > i) {
                        violations.add("Safety 1: " + process + " delivered effect " + effect + " (index " + i
                                + ") before its cause " + cause + " (index " + causeIndex + ")");
                    }
                }
            }
        });
    }

    /** Liveness at quiescence: everything fed in a committed step and owed must have been delivered (SPEC
     * Liveness 1, 5). Processes that failed closed are waived — their remaining holds are the specified trade —
     * but their committed history was still checked for safety above. */
    public void checkAllReceivedDelivered() {
        checkAllReceivedDelivered(Set.of());
    }

    public void checkAllReceivedDelivered(Set<String> failedClosedProcesses) {
        processes.forEach((process, st) -> {
            if (failedClosedProcesses.contains(process)) {
                return;
            }
            Set<Instance> undelivered = new HashSet<>(st.committedFedOwed);
            st.committedDeliveries.forEach(undelivered::remove);
            for (Instance instance : undelivered) {
                violations.add("Liveness: " + process + " was fed " + instance + " but never delivered it");
            }
        });
    }

    public List<Instance> committedDeliveries(String process) {
        return List.copyOf(state(process).committedDeliveries);
    }

    /** Whether a committed step of this process actually fed this exact instance. */
    public boolean committedFeedOf(String process, Instance instance) {
        return state(process).committedFed.contains(instance);
    }

    /** Committed-fed, owed, still-undelivered instances per channel — the custody a wedge never excuses. */
    public Map<ChannelId, List<Instance>> undeliveredOwedByChannel(String process) {
        ProcState st = state(process);
        Set<Instance> undelivered = new HashSet<>(st.committedFedOwed);
        st.committedDeliveries.forEach(undelivered::remove);
        Map<ChannelId, List<Instance>> byChannel = new HashMap<>();
        for (Instance instance : undelivered) {
            byChannel.computeIfAbsent(instance.channel, c -> new ArrayList<>()).add(instance);
        }
        return byChannel;
    }

    public List<String> violations() {
        return List.copyOf(violations);
    }
}
