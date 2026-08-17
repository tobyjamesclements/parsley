package io.github.tobyjamesclements.parsley.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One process: the channels it receives, the channels it sends on, and the stores it owns.
 *
 * <p>A definition is the unit {@link Parsley#start} runs. Each definition becomes its own
 * Kafka Streams application. Declaring a channel as sent is what permits a {@link Handler}
 * to emit on it.
 *
 * @see Builder
 * @see Parsley#start(ParsleyConfig, ProcessDefinition...)
 */
public final class ProcessDefinition {

    /**
     * A received channel and the logic that handles it.
     *
     * @param channel the channel received
     * @param handler the logic invoked for each delivery
     * @param <K>     key type
     * @param <V>     value type
     */
    public record Input<K, V>(Channel<K, V> channel, Handler<K, V> handler) {
        /**
         * @throws IllegalArgumentException if {@code channel} or {@code handler} is null;
         *         a null handler would otherwise surface as an NPE on the stream thread at
         *         first delivery
         */
        public Input {
            if (channel == null) {
                throw new IllegalArgumentException("received channel must be non-null");
            }
            if (handler == null) {
                throw new IllegalArgumentException(channel.topic()
                        + ": handler must be non-null; it is invoked at first delivery on the stream thread");
            }
        }
    }

    private final String name;
    private final Map<String, Input<?, ?>> inputsByTopic;
    private final Map<String, Channel<?, ?>> sendsByTopic;
    private final Map<String, Store<?, ?>> storesByName;

    // Declaration order is part of the contract: the topology's sources, state stores and
    // composed changelog names are derived by iterating these, and Map.copyOf randomises
    // iteration order per JVM, which would make the generated topology nondeterministic
    // across restarts.
    private ProcessDefinition(String name, Map<String, Input<?, ?>> inputsByTopic,
                              Map<String, Channel<?, ?>> sendsByTopic, Map<String, Store<?, ?>> storesByName) {
        this.name = name;
        this.inputsByTopic = Collections.unmodifiableMap(new LinkedHashMap<>(inputsByTopic));
        this.sendsByTopic = Collections.unmodifiableMap(new LinkedHashMap<>(sendsByTopic));
        this.storesByName = Collections.unmodifiableMap(new LinkedHashMap<>(storesByName));
    }

    /**
     * Begins a definition.
     *
     * <p>The name identifies the process across restarts and appears in its Kafka
     * application id, so changing it starts a process with no committed state.
     *
     * @param name the process name, a valid Kafka topic-name component
     * @return a builder
     * @throws IllegalArgumentException if {@code name} is null or malformed; it becomes
     *         part of every changelog topic name, so Kafka's topic-name rules apply
     */
    public static Builder named(String name) {
        if (!KafkaNames.isValidTopicName(name)) {
            throw new IllegalArgumentException("process name must be a valid Kafka topic-name"
                    + " component (" + KafkaNames.RULE + "), since it names changelog topics: " + name);
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
     * Returns the topics this process receives, in declaration order.
     *
     * @return the topics this process receives, in declaration order
     */
    public Set<String> receivedTopics() {
        return inputsByTopic.keySet();
    }

    /**
     * Looks up a received channel.
     *
     * @param topic a topic name
     * @return the channel and handler for {@code topic}, or {@code null} if not received
     */
    public Input<?, ?> input(String topic) {
        return inputsByTopic.get(topic);
    }

    /**
     * Returns the topics this process may send on, in declaration order.
     *
     * @return the topics this process may send on, in declaration order
     */
    public Set<String> sendTopics() {
        return sendsByTopic.keySet();
    }

    /**
     * Looks up a sent channel.
     *
     * @param topic a topic name
     * @return the channel declared for sending on {@code topic}, or {@code null}
     */
    public Channel<?, ?> sendChannel(String topic) {
        return sendsByTopic.get(topic);
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

    /** Accumulates the channels and stores of one process. */
    public static final class Builder {
        private final String name;
        private final Map<String, Input<?, ?>> inputs = new LinkedHashMap<>();
        private final Map<String, Channel<?, ?>> sends = new LinkedHashMap<>();
        private final Map<String, Store<?, ?>> stores = new LinkedHashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        /**
         * Receives a channel, handling each delivery with {@code handler}.
         *
         * @param channel the channel to receive
         * @param handler the logic for each delivery
         * @param <K>     key type
         * @param <V>     value type
         * @return this builder
         * @throws IllegalArgumentException if {@code channel} or {@code handler} is null,
         *                                  or this channel's topic is already received
         */
        public <K, V> Builder receives(Channel<K, V> channel, Handler<K, V> handler) {
            if (channel == null) {
                throw new IllegalArgumentException(name + ": received channel must be non-null");
            }
            if (inputs.putIfAbsent(channel.topic(), new Input<>(channel, handler)) != null) {
                throw new IllegalArgumentException(name + " already receives " + channel.topic());
            }
            return this;
        }

        /**
         * Declares the channels this process may send on. Repeats of the same channel are
         * ignored; the same topic through a different {@code Channel} instance is refused,
         * because two instances for one topic leave it ambiguous which declared serdes the
         * emissions on that topic carry.
         *
         * <p>The whole argument list is validated before any of it is committed, so a
         * refused call leaves the builder exactly as it was.
         *
         * @param channels the channels to declare
         * @return this builder
         * @throws IllegalArgumentException if {@code channels} or an element is null, or a
         *                                  topic is declared through two different
         *                                  {@code Channel} instances
         */
        public Builder sends(Channel<?, ?>... channels) {
            if (channels == null) {
                throw new IllegalArgumentException(name + ": sends requires a non-null channel array");
            }
            Map<String, Channel<?, ?>> accepted = new LinkedHashMap<>(sends);
            for (Channel<?, ?> channel : channels) {
                if (channel == null) {
                    throw new IllegalArgumentException(name + ": sent channels must be non-null");
                }
                Channel<?, ?> existing = accepted.putIfAbsent(channel.topic(), channel);
                if (existing != null && existing != channel) {
                    throw new IllegalArgumentException(name + " already declares sending on "
                            + channel.topic() + " through a different Channel instance; emissions"
                            + " on a topic serialize with its declared serdes, so declare each"
                            + " send topic once");
                }
            }
            sends.clear();
            sends.putAll(accepted);
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
            this.stores.clear();
            this.stores.putAll(accepted);
            return this;
        }

        /**
         * Builds the definition.
         *
         * @return the definition
         * @throws IllegalArgumentException if no channel is received
         */
        public ProcessDefinition build() {
            if (inputs.isEmpty()) {
                throw new IllegalArgumentException("process " + name + " must receive from at least one channel");
            }
            return new ProcessDefinition(name, inputs, sends, stores);
        }
    }
}
