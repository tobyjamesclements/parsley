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
 * Ground truth for the simulator, maintained outside the engine.
 *
 * <p>Tracks happened-before independently and asserts causal order, absence of duplicates,
 * FIFO per channel, and that everything received is eventually delivered.
 */
public final class Oracle {
    record Sent(Instance instance, Map<ChannelId, Long> upperBoundAtSend, Map<ChannelId, Long> lastAssignedAtSend,
                java.util.Set<Instance> excusedAtSend) {
    }

    static final class ProcState {
        final List<Instance> committedDeliveries = new ArrayList<>();
        final Set<Instance> committedPast = new HashSet<>();
        final Set<Instance> committedFedOwed = new HashSet<>();
        final Map<ChannelId, Long> committedExpressible = new HashMap<>();

        final Map<ChannelId, Long> committedPastMax = new HashMap<>();

        final Set<Instance> committedFed = new HashSet<>();
        final Set<Instance> deltaFed = new HashSet<>();
        final List<Instance> deltaDeliveries = new ArrayList<>();
        final Set<Instance> deltaPast = new HashSet<>();
        final Set<Instance> deltaFedOwed = new HashSet<>();
        final Map<ChannelId, Long> deltaExpressible = new HashMap<>();

        // Mirrors the engine's persisted delivered-past clamp: delivered positions merged
        // with each delivered message's *expressed* frontier — coarser than trueCauses, and
        // deliberately so, because the engine's sanctioned drops are judged by expression.
        // Kept apart from committedPastMax, whose trueCauses semantics D41 depends on.
        final Map<ChannelId, Long> committedEnginePast = new HashMap<>();
        final Map<ChannelId, Long> deltaEnginePast = new HashMap<>();
        final Set<Instance> committedDeliveredSet = new HashSet<>();
        final Set<Instance> deltaDeliveredSet = new HashSet<>();

        Map<ChannelId, Long> executionExemption = Map.of();
    }

    private final Map<String, ProcState> processes = new LinkedHashMap<>();
    private final List<String> violations = new ArrayList<>();

    private ProcState state(String process) {
        return processes.computeIfAbsent(process, p -> new ProcState());
    }

    public void onStart(String process) {
        ProcState st = state(process);
        st.executionExemption = deliveredPastMax(process);
    }

    public Map<ChannelId, Long> deliveredPastMax(String process) {
        return new HashMap<>(state(process).committedPastMax);
    }

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

    /**
     * Records a delivery, first checking it was legal at this very moment: every true cause
     * must already be delivered here, settled by evidence the world corroborates, or lie
     * within the delivered past the engine is entitled to drop behind (D31/D41). The
     * end-of-run Safety 1 check compares delivered pairs only, so a premature delivery whose
     * cause never delivers — dropped later by the clamp this very delivery advanced — is
     * visible only here, at delivery time.
     *
     * @param process    the delivering process
     * @param instance   the delivered instance
     * @param settledNow causes settled by world truth at this moment: on a dead or recreated
     *                   channel, truncated below the log start, or on a channel this process
     *                   does not receive
     */
    public void onDelivered(String process, Instance instance, Set<Instance> settledNow) {
        ProcState st = state(process);
        for (Instance cause : instance.trueCauses) {
            if (st.committedDeliveredSet.contains(cause) || st.deltaDeliveredSet.contains(cause)
                    || settledNow.contains(cause)) {
                continue;
            }
            long bound = Math.max(
                    st.committedEnginePast.getOrDefault(cause.channel, Long.MIN_VALUE),
                    st.deltaEnginePast.getOrDefault(cause.channel, Long.MIN_VALUE));
            if (cause.position <= bound) {
                continue;
            }
            violations.add("Safety 1 (delivery-time): " + process + " delivered " + instance
                    + " while its cause " + cause + " was neither delivered, nor settled by evidence,"
                    + " nor within the delivered past (bound: "
                    + (bound == Long.MIN_VALUE ? "none" : bound) + ")");
        }
        st.deltaDeliveries.add(instance);
        st.deltaDeliveredSet.add(instance);
        st.deltaPast.add(instance);
        st.deltaPast.addAll(instance.trueCauses);
        st.deltaExpressible.merge(instance.channel, instance.position, Math::max);
        st.deltaEnginePast.merge(instance.channel, instance.position, Math::max);
        instance.meta.byChannel().forEach((channel, position) ->
                st.deltaEnginePast.merge(channel, position, Math::max));
    }

    public Set<Instance> causalPastSnapshot(String process) {
        ProcState st = state(process);
        Set<Instance> snapshot = new HashSet<>(st.committedPast);
        snapshot.addAll(st.deltaPast);
        return snapshot;
    }

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
        st.committedDeliveredSet.addAll(st.deltaDeliveredSet);
        st.deltaEnginePast.forEach((channel, position) ->
                st.committedEnginePast.merge(channel, position, Math::max));
        rollbackStep(process);
    }

    public void rollbackStep(String process) {
        ProcState st = state(process);
        st.deltaDeliveries.clear();
        st.deltaPast.clear();
        st.deltaFedOwed.clear();
        st.deltaFed.clear();
        st.deltaExpressible.clear();
        st.deltaDeliveredSet.clear();
        st.deltaEnginePast.clear();
    }

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
                continue;
            }
            Long expressed = instance.meta.byChannel().get(cause.channel);
            if (expressed == null || expressed < cause.position) {
                violations.add("Structural 15: " + instance + " fails to express cause " + cause
                        + " (expressed: " + expressed + ")");
            }
        }
    }

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

    public boolean committedFeedOf(String process, Instance instance) {
        return state(process).committedFed.contains(instance);
    }

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

    /** Records a violation the simulated host observed at the moment it happened. */
    public void flag(String violation) {
        violations.add(violation);
    }
}
