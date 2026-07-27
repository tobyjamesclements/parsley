package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Ground-truth causality, computed entirely outside the protocol. The oracle never reads a
 * vector clock: it tracks real happened-before ancestry (program order, delivery order,
 * transitivity) and checks every delivery against it.
 *
 * <p>Actor state is transactional like everything else in the simulation: a node's knowledge
 * gained while processing an input is staged and rolls back if the step aborts, mirroring EOS.
 *
 * <p>Checks:
 * <ul>
 *   <li><b>V1 causal delivery</b> — when record {@code m} is delivered at node {@code n}, every
 *       ancestor of {@code m} on a channel {@code n} consumes, at or above {@code n}'s baseline
 *       on that channel, has already been delivered at {@code n}.</li>
 *   <li><b>V2 channel FIFO</b> — delivered offsets per (node, channel) strictly increase.</li>
 *   <li><b>Duplicate suppression</b> — no record is delivered twice at one node (committed).</li>
 *   <li><b>Completeness</b> (end of run) — every fetchable business record at or above a node's
 *       baseline on a consumed channel was delivered there.</li>
 * </ul>
 */
final class Oracle {

    record Info(long id, Channel channel, long offset, Set<Long> ancestors) {}

    static final class Actor {
        final String name;
        /** Causal past as record ids: everything delivered here plus own emissions, closed. */
        Set<Long> knowledge = new HashSet<>();
        Set<Long> delivered = new HashSet<>();
        final Map<Channel, Long> lastDelivered = new HashMap<>();
        /** First consumer position per channel; ancestors strictly below it are out of scope. */
        final Map<Channel, Long> baseline = new HashMap<>();

        private Set<Long> snapKnowledge;
        private Set<Long> snapDelivered;
        private Map<Channel, Long> snapLastDelivered;
        private Map<Channel, Long> snapBaseline;

        Actor(String name) {
            this.name = name;
        }
    }

    private final Map<String, Actor> actors = new HashMap<>();
    private final Map<Long, Info> records = new HashMap<>();
    private final Map<Channel, Map<Long, Long>> byCoordinate = new HashMap<>();
    private long nextId;

    Actor actor(String name) {
        return actors.computeIfAbsent(name, Actor::new);
    }

    /** Declares where a node's consumer first points at a channel (its oracle baseline). */
    void subscribed(String actor, Channel c, long firstPosition) {
        actor(actor).baseline.putIfAbsent(c, firstPosition);
    }

    /** Registers a business emission by {@code actor}; ancestors are its causal past now. */
    long emitted(String actorName, Channel target, long offset) {
        Actor a = actor(actorName);
        long id = nextId++;
        Info info = new Info(id, target, offset, Set.copyOf(a.knowledge));
        records.put(id, info);
        byCoordinate.computeIfAbsent(target, k -> new HashMap<>()).put(offset, id);
        a.knowledge.add(id);
        return id;
    }

    /** Explicit observation by an edge producer (a plain client folding a consumed record). */
    void observed(String actorName, long recordId) {
        Actor a = actor(actorName);
        a.knowledge.add(recordId);
        a.knowledge.addAll(records.get(recordId).ancestors);
    }

    /** V1, V2, duplicate check, then knowledge update, for one delivery at a node. */
    void onDelivery(String actorName, Set<Channel> consumed, long recordId) {
        Actor a = actor(actorName);
        Info m = records.get(recordId);
        if (m == null) throw new AssertionError(actorName + ": delivered unknown record id " + recordId);

        if (a.delivered.contains(recordId)) {
            throw new AssertionError(actorName + ": duplicate delivery of " + describe(m));
        }
        Long last = a.lastDelivered.get(m.channel);
        if (last != null && m.offset <= last) {
            throw new AssertionError(actorName + ": FIFO violation on " + m.channel
                    + ": delivered offset " + m.offset + " after " + last);
        }
        for (long ancestorId : m.ancestors) {
            Info anc = records.get(ancestorId);
            if (!consumed.contains(anc.channel)) continue;
            Long base = a.baseline.get(anc.channel);
            if (base == null || anc.offset < base) continue;
            if (!a.delivered.contains(ancestorId)) {
                throw new AssertionError(actorName + ": CAUSAL VIOLATION: delivered " + describe(m)
                        + " before its cause " + describe(anc));
            }
        }

        a.delivered.add(recordId);
        a.lastDelivered.put(m.channel, m.offset);
        a.knowledge.add(recordId);
        a.knowledge.addAll(m.ancestors);
    }

    /** The record id at a coordinate, or null (marker, aborted, null message, or nothing). */
    Long recordIdAt(Channel c, long offset) {
        Map<Long, Long> m = byCoordinate.get(c);
        return m == null ? null : m.get(offset);
    }

    void snapshot(String actorName) {
        Actor a = actor(actorName);
        a.snapKnowledge = new HashSet<>(a.knowledge);
        a.snapDelivered = new HashSet<>(a.delivered);
        a.snapLastDelivered = new HashMap<>(a.lastDelivered);
        a.snapBaseline = new HashMap<>(a.baseline);
    }

    void commit(String actorName) {
        Actor a = actor(actorName);
        a.snapKnowledge = null;
        a.snapDelivered = null;
        a.snapLastDelivered = null;
        a.snapBaseline = null;
    }

    void abort(String actorName) {
        Actor a = actor(actorName);
        if (a.snapKnowledge == null) throw new IllegalStateException("no snapshot for " + actorName);
        a.knowledge = a.snapKnowledge;
        a.delivered = a.snapDelivered;
        a.lastDelivered.clear();
        a.lastDelivered.putAll(a.snapLastDelivered);
        a.baseline.clear();
        a.baseline.putAll(a.snapBaseline);
        commit(actorName);
    }

    /** Completeness at drain: everything above baseline on consumed channels was delivered. */
    void checkCompleteness(String actorName, Set<Channel> consumed, SimBroker broker) {
        Actor a = actor(actorName);
        for (Channel c : consumed) {
            Long base = a.baseline.get(c);
            if (base == null) continue;
            for (long o = base; o < broker.endOffset(c); o++) {
                SimBroker.Entry e = broker.entry(c, o);
                if (e.kind() != SimBroker.Kind.BUSINESS) continue;
                if (!a.delivered.contains(e.recordId())) {
                    throw new AssertionError(actorName + ": LIVENESS: record at " + c + "@" + o
                            + " (id " + e.recordId() + ") was never delivered by drain");
                }
            }
        }
    }

    private String describe(Info i) {
        return "record#" + i.id + "@" + i.channel + ":" + i.offset;
    }
}
