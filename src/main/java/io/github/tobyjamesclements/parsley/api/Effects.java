package io.github.tobyjamesclements.parsley.api;

import java.util.ArrayList;
import java.util.List;

import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.Header;

/**
 * Everything one step changes: the messages it sends and the state it writes.
 *
 * <p>A {@link Handler} returns effects rather than performing them. The runtime commits the
 * state writes, the sends and the consumed read positions in a single transaction, so a step
 * takes hold in full or not at all.
 *
 * <p>Causal metadata is attached to each send by the runtime. Application logic has no
 * way to write it and no way to observe it. A send carries the delivered message's
 * timestamp unless it is given one of its own.
 *
 * @see Handler
 * @see #builder()
 */
public final class Effects {

    /**
     * One message to send.
     *
     * @param channel   the channel to send on, which the process must have declared
     * @param key       the message key
     * @param value     the message value
     * @param headers   application headers to attach
     * @param timestamp the message timestamp, or empty to inherit the delivered message's
     *                  (D15); time-based retention and downstream event-time windows read
     *                  it, so a message sent long after the one it answers may want its
     *                  own, derived deterministically from delivered data
     * @param <K>       key type
     * @param <V>       value type
     */
    public record Send<K, V>(Channel<K, V> channel, K key, V value, List<Header> headers,
                                 java.util.OptionalLong timestamp) {
        /**
         * A send inheriting the delivered message's timestamp.
         *
         * @param channel the channel to send on, which the process must have declared
         * @param key     the message key
         * @param value   the message value
         * @param headers application headers to attach
         */
        public Send(Channel<K, V> channel, K key, V value, List<Header> headers) {
            this(channel, key, value, headers, java.util.OptionalLong.empty());
        }

        /**
         * Copies the headers and rejects any using the reserved prefix.
         *
         * @throws IllegalArgumentException if {@code channel}, {@code headers} or
         *         {@code timestamp} is null, {@code headers} contains a null element, or
         *         the timestamp is negative
         * @throws io.github.tobyjamesclements.parsley.core.FailClosedException
         *         if any header uses {@link CausesCodec#RESERVED_HEADER_PREFIX}
         */
        public Send {
            if (channel == null) {
                throw new IllegalArgumentException("channel must be non-null");
            }
            if (headers == null) {
                throw new IllegalArgumentException("headers must be non-null; pass List.of() for none");
            }
            if (timestamp == null) {
                throw new IllegalArgumentException(channel.topic()
                        + ": timestamp must be non-null; pass OptionalLong.empty() to inherit");
            }
            if (timestamp.isPresent() && timestamp.getAsLong() < 0) {
                throw new IllegalArgumentException(channel.topic() + ": timestamp must be non-negative: "
                        + timestamp.getAsLong());
            }
            // One snapshot, one pass: checking the caller's mutable list and then copying
            // it separately would let a mutation between the passes surface as List.copyOf's
            // bare NPE instead of the refusals documented here.
            Header[] snapshot = headers.toArray(new Header[0]);
            for (Header header : snapshot) {
                if (header == null) {
                    throw new IllegalArgumentException(channel.topic()
                            + ": headers may not contain a null element");
                }
                if (header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                    throw new io.github.tobyjamesclements.parsley.core.FailClosedException(
                            io.github.tobyjamesclements.parsley.core.FailClosedException.Reason.RESERVED_HEADER_USED,
                            "application headers may not use the reserved prefix "
                                    + CausesCodec.RESERVED_HEADER_PREFIX);
                }
            }
            headers = List.of(snapshot);
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
    public record Write<K, V>(Store<K, V> store, K key, V value) {
        /**
         * @throws IllegalArgumentException if {@code store} or {@code key} is null; a null
         *         key cannot address a store entry and would only surface as the state
         *         backend's own NPE on the stream thread
         */
        public Write {
            if (store == null) {
                throw new IllegalArgumentException("store must be non-null");
            }
            if (key == null) {
                throw new IllegalArgumentException(store.name() + ": state write key must be non-null");
            }
        }
    }

    private static final Effects NONE = new Effects(List.of(), List.of());

    private final List<Send<?, ?>> sends;
    private final List<Write<?, ?>> writes;

    private Effects(List<Send<?, ?>> sends, List<Write<?, ?>> writes) {
        this.sends = List.copyOf(sends);
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
    public List<Send<?, ?>> sends() {
        return sends;
    }

    /**
     * Returns the state changes to apply, in declaration order.
     *
     * @return the state changes to apply, in declaration order
     */
    public List<Write<?, ?>> writes() {
        return writes;
    }

    /** Accumulates sends and writes. Not thread-safe, and intended for use within one step. */
    public static final class Builder {
        private final List<Send<?, ?>> sends = new ArrayList<>();
        private final List<Write<?, ?>> writes = new ArrayList<>();

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
            sends.add(new Send<>(channel, key, value, List.of()));
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
         * @throws io.github.tobyjamesclements.parsley.core.FailClosedException
         *         if a header uses the reserved prefix
         */
        public <K, V> Builder send(Channel<K, V> channel, K key, V value, List<Header> headers) {
            sends.add(new Send<>(channel, key, value, headers));
            return this;
        }

        /**
         * Sends a message with its own timestamp, in place of the delivered message's.
         *
         * <p>Derive it from delivered data, never from a clock: the runtime may invoke a
         * handler again for the same message, and the effects must be identical.
         *
         * @param channel   the channel to send on
         * @param key       the message key
         * @param value     the message value
         * @param timestamp the message timestamp, non-negative
         * @param <K>       key type
         * @param <V>       value type
         * @return this builder
         * @throws IllegalArgumentException if {@code channel} is null or {@code timestamp}
         *         is negative
         */
        public <K, V> Builder send(Channel<K, V> channel, K key, V value, long timestamp) {
            sends.add(new Send<>(channel, key, value, List.of(), java.util.OptionalLong.of(timestamp)));
            return this;
        }

        /**
         * Sends a message with application headers and its own timestamp.
         *
         * @param channel   the channel to send on
         * @param key       the message key
         * @param value     the message value
         * @param headers   headers to attach
         * @param timestamp the message timestamp, non-negative
         * @param <K>       key type
         * @param <V>       value type
         * @return this builder
         * @throws IllegalArgumentException if {@code channel} or {@code headers} is null,
         *         {@code headers} contains a null element, or {@code timestamp} is negative
         * @throws io.github.tobyjamesclements.parsley.core.FailClosedException
         *         if a header uses the reserved prefix
         */
        public <K, V> Builder send(Channel<K, V> channel, K key, V value, List<Header> headers, long timestamp) {
            sends.add(new Send<>(channel, key, value, headers, java.util.OptionalLong.of(timestamp)));
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
            writes.add(new Write<>(store, key, value));
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
            writes.add(new Write<>(store, key, null));
            return this;
        }

        /**
         * Returns the accumulated effects.
         *
         * @return the accumulated effects
         */
        public Effects build() {
            return new Effects(sends, writes);
        }
    }
}
