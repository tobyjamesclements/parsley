package io.github.tobyjamesclements.parsley.sim;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.core.Causes;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.EngineTestFactory.SabotageMode;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.sim.SimWorld.SimChannel;

/**
 * Builds and runs one simulated scenario from a seed.
 */
public final class Scenario {
    public record Result(List<String> violations, Oracle oracle, Throwable failure, List<String> journal) {
        public boolean clean() {
            return violations.isEmpty() && failure == null;
        }

        public String journalTail(int lines) {
            return tail(journal, lines);
        }
    }

    private static String tail(List<String> journal, int lines) {
        return String.join("\n    ", journal.subList(Math.max(0, journal.size() - lines), journal.size()));
    }

    private record CorruptSpot(SimChannel channel, long position, Instance instance) {
    }

    private static final class RefusalLedger {
        final java.util.EnumSet<ParsleyFailClosedException.Reason> justifiable =
                java.util.EnumSet.noneOf(ParsleyFailClosedException.Reason.class);
        final List<String> violations = new ArrayList<>();
    }

    public static Result run(long seed, SabotageMode mode) {
        List<String> journal = new ArrayList<>();
        try {
            return runInner(seed, mode, journal);
        } catch (Throwable t) {
            return new Result(List.of("threw: " + t + "\n  journal tail:\n    " + tail(journal, 40)), null, t,
                    List.copyOf(journal));
        }
    }

    private static Result runInner(long seed, SabotageMode mode, List<String> journal) {
        Random rng = new Random(seed);
        SimWorld world = new SimWorld(seed);
        Oracle oracle = new Oracle();
        List<CorruptSpot> corrupted = new ArrayList<>();
        RefusalLedger ledger = new RefusalLedger();

        int topicCount = 3 + rng.nextInt(3);
        List<SimChannel> channels = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            channels.addAll(world.createTopic("c" + i, rng.nextInt(4) == 0 ? 2 : 1));
        }
        int channelCount = channels.size();

        int processCount = 2 + rng.nextInt(3);
        List<SimProcess> processes = new ArrayList<>();
        for (int i = 0; i < processCount; i++) {
            List<SimChannel> received = randomSubset(rng, channels, 1 + rng.nextInt(Math.min(3, channelCount)));
            if (i == 0) {
                while (received.size() < 2) {
                    SimChannel extra = channels.get(rng.nextInt(channelCount));
                    if (!received.contains(extra)) {
                        received.add(extra);
                    }
                }
            }
            List<SimChannel> sends = randomSubset(rng, channels, rng.nextInt(3));
            SimProcess process = new SimProcess("p" + i, world, oracle, received, sends,
                    delivered -> emitTargets(delivered, sends), mode);
            processes.add(process);
            process.start();
        }

        int seedMessages = 4 + rng.nextInt(8);
        for (int i = 0; i < seedMessages; i++) {
            produceExternal(world, channels.get(rng.nextInt(channelCount)), rng, "x" + i);
        }

        int events = 250 + rng.nextInt(250);
        for (int e = 0; e < events; e++) {
            SimProcess p = processes.get(rng.nextInt(processCount));
            int roll = rng.nextInt(100);
            if (!p.isRunning()) {
                if (roll < 70) {
                    journal.add(p.name + " restart attempt");
                    guard(p, p::start, journal, ledger);
                } else {
                    redeclareEvent(p, channels, rng, journal, ledger);
                }
                continue;
            }
            if (roll < 32) {
                List<SimChannel> received = iterate(p.receivedChannels());
                SimChannel channel = received.get(rng.nextInt(received.size()));
                journal.add(p.name + " feed " + channel.name + " at " + p.workingNextRead(channel));
                guard(p, () -> p.feedOne(channel), journal, ledger);
            } else if (roll < 44) {
                journal.add(p.name + " drain");
                guard(p, p::drain, journal, ledger);
            } else if (roll < 54) {
                journal.add(p.name + " facts");
                guard(p, p::ingestFacts, journal, ledger);
            } else if (roll < 62) {
                SimChannel target = channels.get(rng.nextInt(channelCount));
                if (rng.nextInt(5) == 0) {
                    journal.add("external corrupt -> " + target.name);
                    ledger.justifiable.add(ParsleyFailClosedException.Reason.UNDECODABLE_METADATA);
                    corrupted.add(produceCorrupt(world, target, "g" + e));
                } else {
                    journal.add("external -> " + target.name);
                    produceExternal(world, target, rng, "x" + seedMessages + "e" + e);
                }
            } else if (roll < 67) {
                SimChannel channel = channels.get(rng.nextInt(channelCount));
                journal.add("dead slot on " + channel.name);
                world.appendDead(channel);
            } else if (roll < 77) {
                journal.add(p.name + " commit");
                p.commitStep();
            } else if (roll < 80) {
                journal.add(p.name + " abort (crash+restart: an aborted step never resumes in place)");
                p.crash();
                guard(p, p::start, journal, ledger);
            } else if (roll < 83) {
                journal.add(p.name + " crash+start");
                p.crash();
                guard(p, p::start, journal, ledger);
            } else if (roll < 86) {
                journal.add(p.name + " stop+start");
                p.stopCleanly();
                guard(p, p::start, journal, ledger);
            } else if (roll < 88) {
                journal.add(p.name + " rewind");
                rewind(p, world, rng, journal, ledger);
            } else if (roll < 90) {
                truncateEvent(world, processes, channels, rng, journal, ledger);
            } else if (roll < 92) {
                killEvent(world, processes, channels, rng, journal, ledger);
            } else if (roll < 96) {
                recreateEvent(world, processes, channels, rng, journal, ledger);
            } else {
                redeclareEvent(p, channels, rng, journal, ledger);
            }
        }

        quiesce(processes, ledger);

        oracle.finalChecks();

        Set<String> waived = new HashSet<>();
        for (SimProcess p : processes) {
            if (p.failedClosed() || wedged(p)) {
                waived.add(p.name);
            }
        }
        oracle.checkAllReceivedDelivered(waived);
        List<String> violations = new ArrayList<>(oracle.violations());
        violations.addAll(ledger.violations);
        for (SimProcess p : processes) {
            if (!waived.contains(p.name) && p.isRunning() && p.engine().heldCountTotal() != 0) {
                violations.add("Liveness: " + p.name + " still holds " + p.engine().heldCountTotal()
                        + " messages at quiescence");
            }
        }

        for (SimProcess p : processes) {
            if (p.failedClosed() || !p.isRunning() || !wedged(p)) {
                continue;
            }
            oracle.undeliveredOwedByChannel(p.name).forEach((channelId, owed) -> {
                int heldCount = p.engine().heldCount(channelId);
                if (owed.size() > heldCount) {
                    violations.add("Liveness: " + p.name + " (wedged) owes " + owed.size()
                            + " undelivered message(s) on " + channelId + " but holds only " + heldCount);
                }
            });
        }

        for (SimProcess p : processes) {
            if (p.failedClosed()) {
                continue;
            }
            Map<ChannelId, Long> deliveredPast = oracle.deliveredPastMax(p.name);
            for (SimChannel channel : iterate(p.receivedChannels())) {
                long covered = Math.max(p.highWaterNextRead(channel),
                        deliveredPast.getOrDefault(channel.id(), Long.MIN_VALUE) + 1);
                if (!channel.dead && channel.logStart > covered) {
                    violations.add("Safety 8: " + p.name + " sailed past truncation on " + channel.name
                            + " (earliest retained " + channel.logStart + " > covered position "
                            + covered + ") without failing closed");
                }
            }
        }
        for (CorruptSpot spot : corrupted) {
            for (SimProcess p : processes) {
                if (oracle.committedFeedOf(p.name, spot.instance())) {
                    violations.add("Safety 7: " + p.name + " committed a step that read undecodable metadata"
                            + " at " + spot.channel().name + "@" + spot.position() + " without failing closed");
                }
            }
        }

        for (SimProcess p : processes) {
            if (p.failedClosed()) {
                continue;
            }
            for (SimChannel channel : iterate(p.receivedChannels())) {
                SimChannel current = world.currentByName(channel.name);
                if (channel.dead && current != null && !current.id().equals(channel.id())) {
                    violations.add("Assumption 2: " + p.name + " still runs while its received topic ("
                            + channel.name + ") was deleted and recreated under the same name, without failing closed");
                }
            }
        }
        return new Result(List.copyOf(violations), oracle, null, List.copyOf(journal));
    }

    private static boolean wedged(SimProcess p) {
        for (SimChannel channel : iterate(p.receivedChannels())) {
            if (!channel.dead && p.workingNextRead(channel) < channel.logStart) {
                return true;
            }
        }
        return false;
    }

    private static boolean guard(SimProcess p, Runnable op, List<String> journal, RefusalLedger ledger) {
        return guarded(p, () -> {
            op.run();
            return Boolean.TRUE;
        }, journal, ledger) != null;
    }

    private static <T> T guarded(SimProcess p, java.util.function.Supplier<T> op, List<String> journal,
                                 RefusalLedger ledger) {
        try {
            return op.get();
        } catch (ParsleyFailClosedException e) {
            p.failClosed(e);
            if (journal != null) {
                journal.add(p.name + " FAILED CLOSED: " + e.reason());
            }
            if (ledger != null && !ledger.justifiable.contains(e.reason())) {
                ledger.violations.add("Over-refusal: " + p.name + " failed closed with " + e.reason()
                        + " though no event of this run created that condition (" + e.getMessage() + ")");
            }
            return null;
        }
    }

    private static void rewind(SimProcess p, SimWorld world, Random rng, List<String> journal, RefusalLedger ledger) {
        p.stopCleanly();
        List<SimChannel> received = iterate(p.receivedChannels());
        SimChannel channel = received.get(rng.nextInt(received.size()));
        p.rewindCommitted(channel, rng.nextInt(4) + 1);
        guard(p, p::start, journal, ledger);
    }

    private static void truncateEvent(SimWorld world, List<SimProcess> processes, List<SimChannel> channels,
                                      Random rng, List<String> journal, RefusalLedger ledger) {
        SimChannel channel = channels.get(rng.nextInt(channels.size()));
        long lso = world.lso(channel);
        List<SimProcess> readers = processes.stream()
                .filter(p -> iterate(p.receivedChannels()).contains(channel))
                .toList();
        long target;
        int shape = rng.nextInt(4);
        if (shape <= 1 && !readers.isEmpty()) {
            SimProcess reader = readers.get(rng.nextInt(readers.size()));

            target = reader.committedNextRead(channel) + (shape == 0 ? 0 : 1);
        } else if (shape == 2) {
            target = lso;
        } else {
            target = lso == 0 ? 0 : rng.nextLong(lso + 1);
        }
        target = Math.max(0, Math.min(target, lso));
        journal.add("truncate " + channel.name + " to " + target);
        ledger.justifiable.add(ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD);
        world.truncate(channel, target);
    }

    private static List<SimChannel> destructibleChannels(List<SimProcess> processes, List<SimChannel> channels) {
        Set<UUID> sendTopics = new HashSet<>();
        for (SimProcess p : processes) {
            for (SimChannel send : p.sendChannels()) {
                sendTopics.add(send.id().topicId());
            }
        }
        return channels.stream()
                .filter(c -> !c.dead && !sendTopics.contains(c.id().topicId()))
                .toList();
    }

    private static void killEvent(SimWorld world, List<SimProcess> processes, List<SimChannel> channels,
                                  Random rng, List<String> journal, RefusalLedger ledger) {
        List<SimChannel> eligible = destructibleChannels(processes, channels);
        if (eligible.isEmpty()) {
            return;
        }

        List<SimChannel> heldFrom = eligible.stream()
                .filter(c -> processes.stream().anyMatch(p ->
                        p.isRunning() && p.engine().receivedChannelSet().contains(c.id())
                                && p.engine().heldCount(c.id()) > 0))
                .toList();
        List<SimChannel> pool = heldFrom.isEmpty() || rng.nextInt(4) == 0 ? eligible : heldFrom;
        SimChannel victim = pool.get(rng.nextInt(pool.size()));
        journal.add("kill topic of " + victim.name);
        ledger.justifiable.add(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES);
        world.killChannel(victim);
    }

    private static void recreateEvent(SimWorld world, List<SimProcess> processes, List<SimChannel> channels,
                                      Random rng, List<String> journal, RefusalLedger ledger) {
        List<SimChannel> eligible = destructibleChannels(processes, channels);
        if (eligible.isEmpty()) {
            return;
        }

        Set<ChannelId> receivedNow = new HashSet<>();
        for (SimProcess p : processes) {
            for (SimChannel channel : p.receivedChannels()) {
                receivedNow.add(channel.id());
            }
        }
        List<SimChannel> watched = eligible.stream().filter(c -> receivedNow.contains(c.id())).toList();
        List<SimChannel> pool = watched.isEmpty() ? eligible : watched;
        SimChannel victim = pool.get(rng.nextInt(pool.size()));
        journal.add("recreate topic of " + victim.name);
        ledger.justifiable.add(ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED);

        ledger.justifiable.add(ParsleyFailClosedException.Reason.CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES);
        List<SimChannel> fresh = world.recreateTopic(victim);
        channels.removeIf(c -> c.id().topicId().equals(victim.id().topicId()));
        channels.addAll(fresh);
    }

    private static void redeclareEvent(SimProcess p, List<SimChannel> channels, Random rng, List<String> journal,
                                       RefusalLedger ledger) {
        if (p.isRunning()) {
            p.stopCleanly();
        }
        List<SimChannel> declaration = iterate(p.receivedChannels());
        Set<SimChannel> current = new LinkedHashSet<>(declaration);
        List<SimChannel> absent = channels.stream()
                .filter(c -> !current.contains(c) && !c.dead)
                .toList();
        if (declaration.size() > 1 && (absent.isEmpty() || rng.nextBoolean())) {
            SimChannel removed = declaration.remove(rng.nextInt(declaration.size()));
            journal.add(p.name + " redeclare without " + removed.name);
            ledger.justifiable.add(ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES);
        } else if (!absent.isEmpty()) {
            SimChannel added = absent.get(rng.nextInt(absent.size()));
            declaration.add(added);
            journal.add(p.name + " redeclare adding " + added.name);
        } else {
            journal.add(p.name + " redeclare unchanged");
        }
        p.redeclare(declaration);
        guard(p, p::start, journal, ledger);
    }

    private static void produceExternal(SimWorld world, SimChannel target, Random rng, String uid) {
        if (rng.nextBoolean()) {
            world.appendExternal(target, (channelId, pos) -> new Instance(
                    channelId, pos, uid, rng.nextBoolean() ? uid.getBytes() : null, uid.getBytes(),
                    List.of(new HeaderKV("app.header", new byte[] {1})), Causes.none(), java.util.Set.of()));
        } else {
            Instance observed = randomCommitted(world, rng);
            if (observed == null) {
                produceExternalPlain(world, target, uid);
                return;
            }
            Map<ChannelId, Long> meta = new TreeMap<>(observed.meta.byChannel());
            meta.merge(observed.channel, observed.position, Math::max);
            java.util.Set<Instance> causes = new java.util.HashSet<>(observed.trueCauses);
            causes.add(observed);
            byte[] header = CausesCodec.encode(Causes.of(meta));
            world.appendExternal(target, (channelId, pos) -> new Instance(
                    channelId, pos, uid, uid.getBytes(), uid.getBytes(),
                    List.of(new HeaderKV(CausesCodec.HEADER_KEY, header)), Causes.of(meta), causes));
        }
    }

    private static CorruptSpot produceCorrupt(SimWorld world, SimChannel target, String uid) {
        Instance appended = world.appendExternal(target, (channelId, pos) -> new Instance(
                channelId, pos, uid, uid.getBytes(), uid.getBytes(),
                List.of(new HeaderKV(CausesCodec.HEADER_KEY, new byte[] {99, 1, 2, 3})),
                Causes.none(), java.util.Set.of()));
        return new CorruptSpot(target, appended.position, appended);
    }

    private static void produceExternalPlain(SimWorld world, SimChannel target, String uid) {
        world.appendExternal(target, (channelId, pos) -> new Instance(
                channelId, pos, uid, uid.getBytes(), uid.getBytes(), List.of(), Causes.none(), java.util.Set.of()));
    }

    private static Instance randomCommitted(SimWorld world, Random rng) {
        List<Instance> committed = new ArrayList<>();
        for (SimChannel channel : world.allChannels()) {
            for (long pos = channel.logStart; pos <= world.lastAssigned(channel); pos++) {
                if (world.slot(channel, pos) instanceof SimWorld.MessageSlot m) {
                    committed.add(m.instance());
                }
            }
        }
        return committed.isEmpty() ? null : committed.get(rng.nextInt(committed.size()));
    }

    private static List<SimChannel> emitTargets(Instance delivered, List<SimChannel> sends) {
        long depth = delivered.uid.chars().filter(c -> c == '>').count() / 2;
        if (depth >= 3 || sends.isEmpty()) {
            return List.of();
        }
        List<SimChannel> targets = new ArrayList<>();
        for (SimChannel send : sends) {
            if (((delivered.uid + ">" + send.name).hashCode() & 3) != 0) {
                targets.add(send);
            }
        }
        return targets;
    }

    static void quiesce(List<SimProcess> processes) {
        quiesce(processes, null);
    }

    private static void quiesce(List<SimProcess> processes, RefusalLedger ledger) {
        boolean progress = true;
        while (progress) {
            progress = false;
            for (SimProcess p : processes) {
                if (p.failedClosed()) {
                    continue;
                }
                if (!p.isRunning() && !guard(p, p::start, null, ledger)) {
                    continue;
                }
                for (SimChannel channel : iterate(p.receivedChannels())) {
                    boolean channelDone = false;
                    while (!channelDone && !p.failedClosed()) {
                        long before = p.workingNextRead(channel);
                        SimProcess.FeedResult fed = guarded(p, () -> p.feedOne(channel), null, ledger);
                        if (fed == null) {
                            break;
                        }
                        if (fed == SimProcess.FeedResult.FED) {
                            progress = true;
                            continue;
                        }
                        if (p.workingNextRead(channel) != before) {
                            progress = true;
                        }
                        channelDone = true;
                    }
                    if (p.failedClosed()) {
                        break;
                    }
                }
                if (p.failedClosed()) {
                    continue;
                }
                Integer drained = guarded(p, p::drain, null, ledger);
                if (drained == null) {
                    continue;
                }
                if (drained > 0) {
                    progress = true;
                }
                p.commitStep();
                if (!guard(p, p::ingestFacts, null, ledger)) {
                    continue;
                }
                drained = guarded(p, p::drain, null, ledger);
                if (drained == null) {
                    continue;
                }
                if (drained > 0) {
                    progress = true;
                }
                p.commitStep();
            }
        }
    }

    private static List<SimChannel> iterate(Iterable<SimChannel> channels) {
        List<SimChannel> list = new ArrayList<>();
        channels.forEach(list::add);
        return list;
    }

    private static List<SimChannel> randomSubset(Random rng, List<SimChannel> channels, int size) {
        List<SimChannel> shuffled = new ArrayList<>(channels);
        java.util.Collections.shuffle(shuffled, rng);
        return new ArrayList<>(shuffled.subList(0, Math.min(size, shuffled.size())));
    }
}
