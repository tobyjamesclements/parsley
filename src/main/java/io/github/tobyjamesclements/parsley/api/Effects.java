package io.github.tobyjamesclements.parsley.api;

import java.util.ArrayList;
import java.util.List;

import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.HeaderKV;

/**
 * Everything application logic did: the messages to send and the application state to persist. This returned value is
 * the only way effects leave the seam; the implementation applies them within the step, so a step that cannot send or
 * persist does not commit (SPEC Structural 3, 19). Emissions are statically typed per channel (SPEC Structural 4).
 */
public final class Effects {

    /** One message to send: the channel the application named, and the key and value exactly as the application made them. */
    public record Emission<K, V>(Channel<K, V> channel, K key, V value, List<HeaderKV> headers) {
        public Emission {
            headers = List.copyOf(headers);
            for (HeaderKV header : headers) {
                if (header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                    // Thrown in the handler's own frame; it escapes through the seam, fails the step and stops
                    // the process — the reserved namespace is unforgeable by construction (SPEC Structural 5).
                    throw new io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException(
                            io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException.Reason.RESERVED_HEADER_USED,
                            "application headers may not use the reserved prefix "
                                    + CausesCodec.RESERVED_HEADER_PREFIX);
                }
            }
        }
    }

    /** One application state write: a put (value non-null) or a delete (value null). */
    public record StateWrite<K, V>(StoreDef<K, V> store, K key, V value) {
    }

    private static final Effects NONE = new Effects(List.of(), List.of());

    private final List<Emission<?, ?>> emissions;
    private final List<StateWrite<?, ?>> writes;

    private Effects(List<Emission<?, ?>> emissions, List<StateWrite<?, ?>> writes) {
        this.emissions = List.copyOf(emissions);
        this.writes = List.copyOf(writes);
    }

    public static Effects none() {
        return NONE;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Emission<?, ?>> emissions() {
        return emissions;
    }

    public List<StateWrite<?, ?>> writes() {
        return writes;
    }

    public static final class Builder {
        private final List<Emission<?, ?>> emissions = new ArrayList<>();
        private final List<StateWrite<?, ?>> writes = new ArrayList<>();

        public <K, V> Builder send(Channel<K, V> channel, K key, V value) {
            emissions.add(new Emission<>(channel, key, value, List.of()));
            return this;
        }

        public <K, V> Builder send(Channel<K, V> channel, K key, V value, List<HeaderKV> headers) {
            emissions.add(new Emission<>(channel, key, value, headers));
            return this;
        }

        public <K, V> Builder put(StoreDef<K, V> store, K key, V value) {
            if (value == null) {
                throw new IllegalArgumentException("use delete() to remove a key");
            }
            writes.add(new StateWrite<>(store, key, value));
            return this;
        }

        public <K, V> Builder delete(StoreDef<K, V> store, K key) {
            writes.add(new StateWrite<>(store, key, null));
            return this;
        }

        public Effects build() {
            return new Effects(emissions, writes);
        }
    }
}
