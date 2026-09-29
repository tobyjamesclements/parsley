package io.github.tobyjamesclements.parsley;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One process: the topics it receives, the topics it sends on, and the stores it owns.
 *
 * <p>A process is the unit {@link Parsley#start} runs. Each one becomes its own Kafka
 * Streams application. Declaring a topic as sent is what permits a {@link Handler}
 * to send on it.
 *
 * @see Builder
 * @see Parsley#start(ParsleyConfig, Process...)
 */
public final class Process {

    /**
     * A received topic and the logic that handles it.
     *
     * @param topic the topic received
     * @param handler the logic invoked for each delivery
     * @param <K>     key type
     * @param <V>     value type
     */
    public record Input<K, V>(Topic<K, V> topic, Handler<K, V> handler) {
        /**
         * @throws IllegalArgumentException if {@code topic} or {@code handler} is null;
         *         a null handler would otherwise surface as an NPE on the stream thread at
         *         first delivery
         */
        public Input {
            if (topic == null) {
                throw new IllegalArgumentException("received topic must be non-null");
            }
            if (handler == null) {
                throw new IllegalArgumentException(topic.name()
                        + ": handler must be non-null; it is invoked at first delivery on the stream thread");
            }
        }
    }

    private final String name;
    private final Map<String, Input<?, ?>> inputsByTopic;
    private final Map<String, Topic<?, ?>> outputsByTopic;
    private final Map<String, Store<?, ?>> storesByName;

    // Declaration order is part of the contract: the topology's sources, state stores and
    // composed changelog names are derived by iterating these, and Map.copyOf randomises
    // iteration order per JVM, which would make the generated topology nondeterministic
    // across restarts.
    private Process(String name, Map<String, Input<?, ?>> inputsByTopic,
                    Map<String, Topic<?, ?>> outputsByTopic, Map<String, Store<?, ?>> storesByName) {
        this.name = name;
        this.inputsByTopic = Collections.unmodifiableMap(new LinkedHashMap<>(inputsByTopic));
        this.outputsByTopic = Collections.unmodifiableMap(new LinkedHashMap<>(outputsByTopic));
        this.storesByName = Collections.unmodifiableMap(new LinkedHashMap<>(storesByName));
    }

    /**
     * Begins a process.
     *
     * <p>The name identifies the process across restarts and appears in its Kafka
     * application id, so changing it starts a process with no committed state.
     *
     * @param name the process name, which becomes part of every changelog topic name, so
     *             Kafka's topic-name rules apply to it
     * @return a builder
     * @throws IllegalArgumentException if {@code name} is null or blank, or contains the
     *         reserved {@link Parsley#RESERVED_PREFIX} namespace
     */
    public static Builder named(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("process name must be non-blank");
        }
        if (name.contains(Parsley.RESERVED_PREFIX)) {
            throw new IllegalArgumentException("process name may not contain the reserved namespace "
                    + Parsley.RESERVED_PREFIX + ": it becomes part of application ids, consumer groups"
                    + " and changelog topic names, which would then sit inside parsley's own"
                    + " namespace: " + name);
        }
        return new Builder(name);
    }

    /**
     * Returns the process name.
     *
     * @return the process name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the topics this process receives, each with its handler, in declaration
     * order.
     *
     * @return the received topics and their handlers, in declaration order
     */
    public List<Input<?, ?>> inputs() {
        return List.copyOf(inputsByTopic.values());
    }

    /**
     * Looks up a received topic.
     *
     * @param topic a topic name
     * @return the topic and handler for {@code topic}, or {@code null} if not received
     */
    public Input<?, ?> input(String topic) {
        return inputsByTopic.get(topic);
    }

    /**
     * Returns the topics this process may send on, in declaration order.
     *
     * @return the topics this process may send on, in declaration order
     */
    public List<Topic<?, ?>> outputs() {
        return List.copyOf(outputsByTopic.values());
    }

    /**
     * Looks up a topic this process may send on.
     *
     * @param topic a topic name
     * @return the topic declared for sending on {@code topic}, or {@code null}
     */
    public Topic<?, ?> output(String topic) {
        return outputsByTopic.get(topic);
    }

    /**
     * Returns the stores this process owns, in declaration order.
     *
     * @return the stores this process owns, in declaration order
     */
    public List<Store<?, ?>> stores() {
        return List.copyOf(storesByName.values());
    }

    /**
     * Looks up a declared store.
     *
     * @param name a store name
     * @return the store declared under {@code name}, or {@code null}
     */
    public Store<?, ?> store(String name) {
        return storesByName.get(name);
    }

    /** Accumulates the topics and stores of one process. */
    public static final class Builder {
        private final String name;
        private final Map<String, Input<?, ?>> inputs = new LinkedHashMap<>();
        private final Map<String, Topic<?, ?>> outputs = new LinkedHashMap<>();
        private final Map<String, Store<?, ?>> stores = new LinkedHashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        /**
         * Receives a topic, handling each delivery with {@code handler}.
         *
         * @param topic the topic to receive
         * @param handler the logic for each delivery
         * @param <K>     key type
         * @param <V>     value type
         * @return this builder
         * @throws IllegalArgumentException if {@code topic} or {@code handler} is null,
         *                                  or a topic of that name is already received
         */
        public <K, V> Builder receives(Topic<K, V> topic, Handler<K, V> handler) {
            Input<K, V> input = new Input<>(topic, handler);
            if (inputs.putIfAbsent(input.topic().name(), input) != null) {
                throw new IllegalArgumentException(name + " already receives " + topic.name());
            }
            return this;
        }

        /**
         * Declares the topics this process may send on. Repeats of the same topic are
         * ignored; the same topic through a different {@code Topic} instance is refused,
         * because two instances for one topic leave it ambiguous which declared serdes the
         * sends on that topic carry.
         *
         * <p>The whole argument list is validated before any of it is committed, so a
         * refused call leaves the builder exactly as it was.
         *
         * @param topics the topics to declare
         * @return this builder
         * @throws IllegalArgumentException if {@code topics} or an element is null, or a
         *                                  topic is declared through two different
         *                                  {@code Topic} instances
         */
        public Builder sends(Topic<?, ?>... topics) {
            if (topics == null) {
                throw new IllegalArgumentException(name + ": sends requires a non-null topic array");
            }
            Map<String, Topic<?, ?>> accepted = new LinkedHashMap<>(outputs);
            for (Topic<?, ?> topic : topics) {
                if (topic == null) {
                    throw new IllegalArgumentException(name + ": sent topics must be non-null");
                }
                Topic<?, ?> existing = accepted.putIfAbsent(topic.name(), topic);
                if (existing != null && existing != topic) {
                    throw new IllegalArgumentException(name + " already declares sending on "
                            + topic.name() + " through a different Topic instance; sends"
                            + " on a topic serialize with its declared serdes, so declare each"
                            + " send topic once");
                }
            }
            // Append-only commit: accepted was seeded from the field and putIfAbsent never
            // replaced an entry, so earlier declarations keep their order and identity.
            outputs.putAll(accepted);
            return this;
        }

        /**
         * Declares the stores this process owns.
         *
         * <p>The whole argument list is validated before any of it is committed, so a
         * refused call leaves the builder exactly as it was.
         *
         * @param stores the stores to declare
         * @return this builder
         * @throws IllegalArgumentException if {@code stores} or an element is null, or a
         *                                  store name is declared twice
         */
        public Builder stores(Store<?, ?>... stores) {
            if (stores == null) {
                throw new IllegalArgumentException(name + ": stores requires a non-null store array");
            }
            Map<String, Store<?, ?>> accepted = new LinkedHashMap<>(this.stores);
            for (Store<?, ?> store : stores) {
                if (store == null) {
                    throw new IllegalArgumentException(name + ": declared stores must be non-null");
                }
                if (accepted.putIfAbsent(store.name(), store) != null) {
                    throw new IllegalArgumentException(name + " already declares store " + store.name());
                }
            }
            // Append-only commit, as in sends().
            this.stores.putAll(accepted);
            return this;
        }

        /**
         * Builds the process.
         *
         * @return the process
         * @throws IllegalArgumentException if no topic is received
         */
        public Process build() {
            if (inputs.isEmpty()) {
                throw new IllegalArgumentException("process " + name + " must receive from at least one topic");
            }
            return new Process(name, inputs, outputs, stores);
        }
    }
}
