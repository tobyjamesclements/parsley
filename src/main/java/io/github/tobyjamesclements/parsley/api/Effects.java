package io.github.tobyjamesclements.parsley.api;

import java.util.ArrayList;
import java.util.List;

import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.HeaderKV;

/**
 * Everything one step changes: the messages it sends and the state it writes.
 *
 * <p>A {@link Handler} returns effects rather than performing them. The runtime commits the
 * state writes, the sends and the consumed read positions in a single transaction, so a step
 * takes hold in full or not at all.
 *
 * <p>Causal metadata is attached to each emission by the runtime. Application logic has no
 * way to write it and no way to observe it.
 *
 * @see Handler
 * @see #builder()
 */
public final class Effects {

    /**
     * One message to send.
     *
     * @param channel the channel to send on, which the process must have declared
     * @param key     the message key
     * @param value   the message value
     * @param headers application headers to attach
     * @param <K>     key type
     * @param <V>     value type
     */
    public record Emission<K, V>(Channel<K, V> channel, K key, V value, List<HeaderKV> headers) {
        /**
         * Copies the headers and rejects any using the reserved prefix.
         *
         * @throws IllegalArgumentException if {@code channel} or {@code headers} is null,
         *         or {@code headers} contains a null element
         * @throws io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException
         *         if any header uses {@link CausesCodec#RESERVED_HEADER_PREFIX}
         */
        public Emission {
            if (channel == null) {
                throw new IllegalArgumentException("channel must be non-null");
            }
            if (headers == null) {
                throw new IllegalArgumentException("headers must be non-null; pass List.of() for none");
            }
            for (HeaderKV header : headers) {
                if (header == null) {
                    throw new IllegalArgumentException(channel.topic()
                            + ": headers may not contain a null element");
                }
            }
            headers = List.copyOf(headers);
            for (HeaderKV header : headers) {
                if (header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                    throw new io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException(
                            io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason.RESERVED_HEADER_USED,
                            "application headers may not use the reserved prefix "
                                    + CausesCodec.RESERVED_HEADER_PREFIX);
                }
            }
        }
    }

    /**
     * One change to application state.
     *
     * @param store the store to write, which the process must have declared
     * @param key   the key to write
     * @param value the value to write, or {@code null} to remove the key
     * @param <K>   key type
     * @param <V>   value type
     */
    public record StateWrite<K, V>(Store<K, V> store, K key, V value) {
        /**
         * @throws IllegalArgumentException if {@code store} or {@code key} is null; a null
         *         key cannot address a store entry and would only surface as the state
         *         backend's own NPE on the stream thread
         */
        public StateWrite {
            if (store == null) {
                throw new IllegalArgumentException("store must be non-null");
            }
            if (key == null) {
                throw new IllegalArgumentException(store.name() + ": state write key must be non-null");
            }
        }
    }

    private static final Effects NONE = new Effects(List.of(), List.of());

    private final List<Emission<?, ?>> emissions;
    private final List<StateWrite<?, ?>> writes;

    private Effects(List<Emission<?, ?>> emissions, List<StateWrite<?, ?>> writes) {
        this.emissions = List.copyOf(emissions);
        this.writes = List.copyOf(writes);
    }

    /**
     * The empty result, for a step that observes without changing anything.
     *
     * @return effects with no sends and no writes
     */
    public static Effects none() {
        return NONE;
    }

    /**
     * Returns a new builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the messages to send, in declaration order.
     *
     * @return the messages to send, in declaration order
     */
    public List<Emission<?, ?>> emissions() {
        return emissions;
    }

    /**
     * Returns the state changes to apply, in declaration order.
     *
     * @return the state changes to apply, in declaration order
     */
    public List<StateWrite<?, ?>> writes() {
        return writes;
    }

    /** Accumulates sends and writes. Not thread-safe, and intended for use within one step. */
    public static final class Builder {
        private final List<Emission<?, ?>> emissions = new ArrayList<>();
        private final List<StateWrite<?, ?>> writes = new ArrayList<>();

        /** Builds an empty accumulator. */
        Builder() {
        }

        /**
         * Sends a message with no application headers.
         *
         * @param channel the channel to send on
         * @param key     the message key
         * @param value   the message value
         * @param <K>     key type
         * @param <V>     value type
         * @return this builder
         * @throws IllegalArgumentException if {@code channel} is null
         */
        public <K, V> Builder send(Channel<K, V> channel, K key, V value) {
            emissions.add(new Emission<>(channel, key, value, List.of()));
            return this;
        }

        /**
         * Sends a message carrying application headers.
         *
         * @param channel the channel to send on
         * @param key     the message key
         * @param value   the message value
         * @param headers headers to attach
         * @param <K>     key type
         * @param <V>     value type
         * @return this builder
         * @throws IllegalArgumentException if {@code channel} or {@code headers} is null,
         *         or {@code headers} contains a null element
         * @throws io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException
         *         if a header uses the reserved prefix
         */
        public <K, V> Builder send(Channel<K, V> channel, K key, V value, List<HeaderKV> headers) {
            emissions.add(new Emission<>(channel, key, value, headers));
            return this;
        }

        /**
         * Writes a value.
         *
         * @param store the store to write
         * @param key   the key to write
         * @param value the value, which must be non-null
         * @param <K>   key type
         * @param <V>   value type
         * @return this builder
         * @throws IllegalArgumentException if {@code store}, {@code key} or {@code value}
         *                                  is null
         * @see #delete(Store, Object)
         */
        public <K, V> Builder put(Store<K, V> store, K key, V value) {
            if (value == null) {
                throw new IllegalArgumentException("use delete() to remove a key");
            }
            writes.add(new StateWrite<>(store, key, value));
            return this;
        }

        /**
         * Removes a key.
         *
         * @param store the store to write
         * @param key   the key to remove
         * @param <K>   key type
         * @param <V>   value type
         * @return this builder
         * @throws IllegalArgumentException if {@code store} or {@code key} is null
         */
        public <K, V> Builder delete(Store<K, V> store, K key) {
            writes.add(new StateWrite<>(store, key, null));
            return this;
        }

        /**
         * Returns the accumulated effects.
         *
         * @return the accumulated effects
         */
        public Effects build() {
            return new Effects(emissions, writes);
        }
    }
}
