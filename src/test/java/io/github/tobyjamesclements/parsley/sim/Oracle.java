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

    public void onDelivered(String process, Instance instance) {
        ProcState st = state(process);
        st.deltaDeliveries.add(instance);
        st.deltaPast.add(instance);
        st.deltaPast.addAll(instance.trueCauses);
        st.deltaExpressible.merge(instance.channel, instance.position, Math::max);
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
}
