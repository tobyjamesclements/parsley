package io.github.tobyjamesclements.parsley.sim;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.NodeConfig;
import io.github.tobyjamesclements.parsley.core.DeliveryProtocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * One simulated world: a broker, a set of causal nodes, scripted edge producers, an oracle, and
 * a seeded scheduler that interleaves fetches, acknowledgement deliveries, producer ops,
 * crashes, and restarts until the world drains or the step budget runs out.
 *
 * <p>Draining means no action is enabled anywhere: every fetchable record fetched, every
 * acknowledgement delivered, every scripted op done, every node up. A world that cannot drain
 * inside the budget fails (this is what catches null-message storms and wedged frontiers), and
 * a drained world must pass the oracle's completeness check (every business record above a
 * node's baseline on a consumed channel was delivered there).
 */
final class SimWorld {

    final SimBroker broker = new SimBroker();
    final Oracle oracle = new Oracle();
    private final Random rng;

    private final List<SimNode> nodes = new ArrayList<>();
    private final List<EdgeProducer> producers = new ArrayList<>();
    private final Map<UUID, String> topicNames = new HashMap<>();

    private int crashBudget;
    private boolean allowTaskFailures;
    private final List<String> taskFailures = new ArrayList<>();
    private long stepsTaken;

    SimWorld(long seed) {
        this.rng = new Random(seed);
    }

    SimWorld topic(String name, int partitions) {
        broker.createTopic(name, partitions);
        topicNames.put(broker.topicId(name), name);
        return this;
    }

    SimWorld crashBudget(int crashes) {
        this.crashBudget = crashes;
        return this;
    }

    SimWorld allowTaskFailures() {
        this.allowTaskFailures = true;
        return this;
    }

    /** Adds a causal node consuming the given (topic, partition) channels. */
    SimNode node(String name, int taskPartition, List<String> inputTopicPartitions, List<String> sinkTopics,
                 SimBehavior behavior, BiFunction<NodeConfig, SimNode, DeliveryProtocol> factory) {
        Set<Channel> consumed = new HashSet<>();
        for (String tp : inputTopicPartitions) {
            int i = tp.lastIndexOf(':');
            consumed.add(broker.channel(tp.substring(0, i), Integer.parseInt(tp.substring(i + 1))));
        }
        Set<UUID> sinks = new HashSet<>();
        for (String t : sinkTopics) sinks.add(broker.topicId(t));
        NodeConfig config = new NodeConfig(name, consumed, sinks, taskPartition, 64);
        SimNode n = new SimNode(name, this, config, behavior, factory);
        nodes.add(n);
        n.start();
        return n;
    }

    EdgeProducer producer(String name) {
        EdgeProducer p = new EdgeProducer(name, this);
        producers.add(p);
        return p;
    }

    Channel routeByKey(String topic, byte[] key) {
        int parts = broker.partitions(topic);
        int p = key == null ? 0 : Math.floorMod(Arrays.hashCode(key), parts);
        return broker.channel(topic, p);
    }

    Channel ownPartition(UUID sinkTopic, int taskPartition) {
        String name = topicNames.get(sinkTopic);
        return broker.channel(name, taskPartition % broker.partitions(name));
    }

    void taskFailure(String node, RuntimeException ex) {
        if (!allowTaskFailures) {
            throw new AssertionError("task failure at " + node + ": " + ex, ex);
        }
        taskFailures.add(node + ": " + ex);
    }

    List<String> taskFailures() {
        return taskFailures;
    }

    long stepsTaken() {
        return stepsTaken;
    }

    private sealed interface Action {
        record Process(SimNode node, Channel channel, boolean crash) implements Action {}
        record DeliverAck(SimNode node) implements Action {}
        record Restart(SimNode node) implements Action {}
        record CrashIdle(SimNode node) implements Action {}
        record ProducerStep(EdgeProducer producer) implements Action {}
    }

    /** Runs to drain; throws when the budget is exhausted first. */
    void run(long maxSteps) {
        for (stepsTaken = 0; stepsTaken < maxSteps; stepsTaken++) {
            List<Action> enabled = new ArrayList<>();
            for (SimNode n : nodes) {
                if (n.up) {
                    for (Channel c : n.config.consumed()) {
                        if (n.hasWork(c)) {
                            enabled.add(new Action.Process(n, c, false));
                            if (crashBudget > 0) enabled.add(new Action.Process(n, c, true));
                        }
                    }
                    if (n.sends.hasInFlight()) enabled.add(new Action.DeliverAck(n));
                    if (crashBudget > 0) enabled.add(new Action.CrashIdle(n));
                } else {
                    enabled.add(new Action.Restart(n));
                }
            }
            for (EdgeProducer p : producers) {
                if (p.hasWork()) enabled.add(new Action.ProducerStep(p));
            }
            if (enabled.isEmpty()) {
                verifyDrained();
                return;
            }
            switch (enabled.get(rng.nextInt(enabled.size()))) {
                case Action.Process(SimNode n, Channel c, boolean crash) -> {
                    if (crash) crashBudget--;
                    n.step(c, crash);
                }
                case Action.DeliverAck(SimNode n) -> n.sends.deliverOneAck();
                case Action.Restart(SimNode n) -> n.start();
                case Action.CrashIdle(SimNode n) -> {
                    crashBudget--;
                    n.crashIdle();
                }
                case Action.ProducerStep(EdgeProducer p) -> p.step();
            }
        }
        throw new AssertionError("world did not drain within " + maxSteps
                + " steps (wedged frontier, unresolvable hold, or null-message storm)");
    }

    private void verifyDrained() {
        for (SimNode n : nodes) {
            oracle.checkCompleteness(n.name, n.config.consumed(), broker);
        }
    }
}
