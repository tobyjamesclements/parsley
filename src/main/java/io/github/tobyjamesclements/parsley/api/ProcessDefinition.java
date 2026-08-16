package io.github.tobyjamesclements.parsley.api;

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
    }

    private final String name;
    private final Map<String, Input<?, ?>> inputsByTopic;
    private final Map<String, Channel<?, ?>> sendsByTopic;
    private final Map<String, Store<?, ?>> storesByName;

    private ProcessDefinition(String name, Map<String, Input<?, ?>> inputsByTopic,
                              Map<String, Channel<?, ?>> sendsByTopic, Map<String, Store<?, ?>> storesByName) {
        this.name = name;
        this.inputsByTopic = Map.copyOf(inputsByTopic);
        this.sendsByTopic = Map.copyOf(sendsByTopic);
        this.storesByName = Map.copyOf(storesByName);
    }

    /**
     * Begins a definition.
     *
     * <p>The name identifies the process across restarts and appears in its Kafka
     * application id, so changing it starts a process with no committed state.
     *
     * @param name the process name, matching {@code [a-zA-Z0-9._-]+}
     * @return a builder
     * @throws IllegalArgumentException if {@code name} is null, blank or malformed
     */
    public static Builder named(String name) {
        if (name == null || name.isBlank() || !name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("process name must be non-blank and [a-zA-Z0-9._-]+: " + name);
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
     * Returns the topics this process receives.
     *
     * @return the topics this process receives
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
     * Returns the topics this process may send on.
     *
     * @return the topics this process may send on
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
         * @throws IllegalArgumentException if this channel's topic is already received
         */
        public <K, V> Builder receives(Channel<K, V> channel, Handler<K, V> handler) {
            if (inputs.putIfAbsent(channel.topic(), new Input<>(channel, handler)) != null) {
                throw new IllegalArgumentException(name + " already receives " + channel.topic());
            }
            return this;
        }

        /**
         * Declares the channels this process may send on. Repeats of the same channel are
         * ignored; the same topic through a different {@code Channel} instance is refused,
         * because emissions must use the declared instance and a silently dropped duplicate
         * would surface as a fail-closed refusal at first emission.
         *
         * @param channels the channels to declare
         * @return this builder
         * @throws IllegalArgumentException if a topic is declared through two different
         *                                  {@code Channel} instances
         */
        public Builder sends(Channel<?, ?>... channels) {
            for (Channel<?, ?> channel : channels) {
                Channel<?, ?> existing = sends.putIfAbsent(channel.topic(), channel);
                if (existing != null && existing != channel) {
                    throw new IllegalArgumentException(name + " already declares sending on "
                            + channel.topic() + " through a different Channel instance; emissions"
                            + " must use the declared instance, so declare each send topic once");
                }
            }
            return this;
        }

        /**
         * Declares the stores this process owns.
         *
         * @param stores the stores to declare
         * @return this builder
         * @throws IllegalArgumentException if a store name is declared twice
         */
        public Builder stores(Store<?, ?>... stores) {
            for (Store<?, ?> store : stores) {
                if (this.stores.putIfAbsent(store.name(), store) != null) {
                    throw new IllegalArgumentException(name + " already declares store " + store.name());
                }
            }
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
