package io.github.tobyjamesclements.parsley.api;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One declared process: the channels it receives from (each with its typed handler), the channels it sends to, and
 * its application state stores (SPEC Structural 17, 18). The host induces one process per partition group from this
 * declaration (SPEC Assumption 14). Any arrangement is expressible — several processes, shared channels, cycles,
 * channels from a process to itself (SPEC Structural 2).
 */
public final class ProcessDefinition {

    /** One received channel with its handler; the two type parameters travel together. */
    public record Input<K, V>(Channel<K, V> channel, Handler<K, V> handler) {
    }

    private final String name;
    private final Map<String, Input<?, ?>> inputsByTopic;
    private final Map<String, Channel<?, ?>> sendsByTopic;
    private final Map<String, StoreDef<?, ?>> storesByName;

    private ProcessDefinition(String name, Map<String, Input<?, ?>> inputsByTopic,
                              Map<String, Channel<?, ?>> sendsByTopic, Map<String, StoreDef<?, ?>> storesByName) {
        this.name = name;
        this.inputsByTopic = Map.copyOf(inputsByTopic);
        this.sendsByTopic = Map.copyOf(sendsByTopic);
        this.storesByName = Map.copyOf(storesByName);
    }

    public static Builder named(String name) {
        if (name == null || name.isBlank() || !name.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("process name must be non-blank and [a-zA-Z0-9._-]+: " + name);
        }
        return new Builder(name);
    }

    public String name() {
        return name;
    }

    public Set<String> receivedTopics() {
        return inputsByTopic.keySet();
    }

    public Input<?, ?> input(String topic) {
        return inputsByTopic.get(topic);
    }

    public Set<String> sendTopics() {
        return sendsByTopic.keySet();
    }

    public Channel<?, ?> sendChannel(String topic) {
        return sendsByTopic.get(topic);
    }

    public List<StoreDef<?, ?>> stores() {
        return List.copyOf(storesByName.values());
    }

    public StoreDef<?, ?> store(String name) {
        return storesByName.get(name);
    }

    public static final class Builder {
        private final String name;
        private final Map<String, Input<?, ?>> inputs = new LinkedHashMap<>();
        private final Map<String, Channel<?, ?>> sends = new LinkedHashMap<>();
        private final Map<String, StoreDef<?, ?>> stores = new LinkedHashMap<>();
        private final Set<String> storeNames = new LinkedHashSet<>();

        private Builder(String name) {
            this.name = name;
        }

        public <K, V> Builder receives(Channel<K, V> channel, Handler<K, V> handler) {
            if (inputs.putIfAbsent(channel.topic(), new Input<>(channel, handler)) != null) {
                throw new IllegalArgumentException(name + " already receives " + channel.topic());
            }
            return this;
        }

        public Builder sends(Channel<?, ?>... channels) {
            for (Channel<?, ?> channel : channels) {
                sends.putIfAbsent(channel.topic(), channel);
            }
            return this;
        }

        public Builder stores(StoreDef<?, ?>... defs) {
            for (StoreDef<?, ?> def : defs) {
                if (!storeNames.add(def.name())) {
                    throw new IllegalArgumentException(name + " already declares store " + def.name());
                }
                stores.put(def.name(), def);
            }
            return this;
        }

        public ProcessDefinition build() {
            if (inputs.isEmpty()) {
                throw new IllegalArgumentException("process " + name + " must receive from at least one channel");
            }
            return new ProcessDefinition(name, inputs, sends, stores);
        }
    }
}
