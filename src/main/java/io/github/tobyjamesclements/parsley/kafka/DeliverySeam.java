package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Delivery;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.StateReader;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.CausesCodec;
import io.github.tobyjamesclements.parsley.core.DeliverableMessage;
import io.github.tobyjamesclements.parsley.core.HeaderKV;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * The handler seam, independent of the host that drives it.
 *
 * <p>One delivery is decoded through the channel's declared serdes, handed to its handler
 * with a read view of the declared stores, and the returned effects are planned in full —
 * every target resolved, every payload serialized — before the first write reaches a store
 * or the first record reaches a sink. A fail-closed refusal raised inside application code
 * is latched and rethrown at every seam boundary, so an application catch cannot commit a
 * step whose reads were refused.
 *
 * <p>The host supplies byte-level stores, the sink that carries emissions, and the causal
 * metadata each emission is stamped with. Nothing here names a host type, so the Kafka
 * Streams processor and the kafka-clients runtime run one copy of the seam.
 */
final class DeliverySeam {

    /** A byte-level application store, as the host keeps it. */
    interface ByteStore {
        byte[] get(byte[] key);

        void put(byte[] key, byte[] value);

        void delete(byte[] key);
    }

    /** Where a planned emission goes. */
    @FunctionalInterface
    interface Sink {
        void send(String topic, byte[] key, byte[] value, RecordHeaders headers, long timestamp);
    }

    private final ProcessDefinition definition;
    private final Map<String, ByteStore> appStores;
    private final Map<String, String> serdeTopicByStore;
    private final Supplier<byte[]> causesHeader;
    private final Sink sink;
    private final StateReader stateReader = new StoreStateReader();

    /**
     * A fail-closed refusal raised by the state reader inside application code. The reader
     * latches it here before throwing, and {@code deliver} rethrows at every seam boundary
     * — frame entry, after the delivered payload's deserializers, after the handler, and
     * after the planned effects apply — so an application catch cannot commit a step whose
     * reads were refused, wherever in the frame the read ran. Volatile because the reader
     * is an object application code can hold: a latch written from an application thread
     * must be visible to the host thread's next check.
     */
    private volatile ParsleyFailClosedException swallowedSeamViolation;

    /**
     * @param definition        the process whose handlers run here
     * @param appStores         the declared stores, by name, as the host keeps them
     * @param serdeTopicByStore the serde topic each store's codecs are handed, by store name
     * @param causesHeader      the causal metadata every emission of the current step carries
     * @param sink              where emissions go
     */
    DeliverySeam(ProcessDefinition definition, Map<String, ByteStore> appStores,
                 Map<String, String> serdeTopicByStore, Supplier<byte[]> causesHeader, Sink sink) {
        this.definition = definition;
        this.appStores = Map.copyOf(appStores);
        this.serdeTopicByStore = Map.copyOf(serdeTopicByStore);
        this.causesHeader = causesHeader;
        this.sink = sink;
    }

    /** The read view handed to every handler of this seam. */
    StateReader stateReader() {
        return stateReader;
    }

    /**
     * Delivers one message: decodes it, runs its handler, and applies the returned effects.
     *
     * @throws ParsleyFailClosedException if the payload cannot be decoded, the handler
     *         refuses, an effect names an undeclared target, a payload cannot be serialized,
     *         a reserved header is used, or a read was refused inside application code
     */
    <K, V> void deliver(ProcessDefinition.Input<K, V> input, DeliverableMessage message) {
        // Checked at entry, never blanket-reset mid-frame: a reset placed after any
        // application code would erase what that code latched. The delivered payload's
        // own deserializers are application code and run before the handler, so a refusal
        // they latch through a captured reader must fail this step, not vanish.
        rethrowSeamViolation();
        Channel<K, V> channel = input.channel();
        String topic = channel.topic();
        // Reserved transport headers are parsley's own carriage, invisible to application
        // logic in both directions (D56): deserializers see exactly the headers the
        // application sent, the same view Delivery presents one frame later.
        List<HeaderKV> applicationHeaders = withoutReservedHeaders(message.headers());
        RecordHeaders receivedHeaders = toKafkaHeaders(applicationHeaders);
        K key;
        V value;
        try {
            key = message.key() == null
                    ? null : channel.keySerde().deserializer().deserialize(topic, receivedHeaders, message.key());
            value = message.value() == null
                    ? null : channel.valueSerde().deserializer().deserialize(topic, receivedHeaders, message.value());
        } catch (RuntimeException e) {
            // A reader refusal thrown through the deserializer keeps its own reason: the
            // latch identifies it, and wrapping it as a payload failure would mislabel
            // the stop for status().
            rethrowSeamViolation();
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                    definition.name() + ": " + topic + "@" + message.position(), e);
        }
        rethrowSeamViolation();

        Delivery<K, V> delivery = Delivery.of(channel, message.channel().partition(), message.position(),
                message.timestamp(), key, value, applicationHeaders);
        Effects effects = input.handler().handle(delivery, stateReader);
        // The reader's refusal was thrown inside the handler's own frame, where an
        // application catch can swallow it; the latch makes the step fail regardless,
        // as docs/failing-closed.md promises for every fail-closed event.
        rethrowSeamViolation();
        if (effects == null) {
            // A deliberate refusal that recurs identically on restart, so it carries its
            // own reason and reaches status() rather than an empty refusalReason.
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.HANDLER_RETURNED_NULL_EFFECTS,
                    definition.name() + ": handler for " + topic + " returned null effects; return"
                            + " Effects.none() for a step that changes nothing");
        }
        // Plan, then apply: resolving every effect's declared target and serializing every
        // payload is a pure function of the definition and the returned Effects, so every
        // refusal that can be raised here — undeclared target, unserializable payload,
        // reserved header, exceeded metadata budget — fires before the first write reaches
        // a store or the first record is forwarded, never relying on the transaction abort
        // alone to unwind a half-applied step. Apply then consumes the plan, so an effect
        // cannot reach a store or a sink without having been planned.
        List<PlannedWrite> writes = new ArrayList<>(effects.writes().size());
        for (Effects.StateWrite<?, ?> write : effects.writes()) {
            writes.add(planWrite(write));
        }
        List<PlannedSend> sends = new ArrayList<>(effects.emissions().size());
        for (Effects.Emission<?, ?> emission : effects.emissions()) {
            sends.add(planEmission(emission, emission.timestamp().orElse(message.timestamp())));
        }
        for (PlannedWrite write : writes) {
            if (write.value() == null) {
                write.store().delete(write.key());
            } else {
                write.store().put(write.key(), write.value());
            }
        }
        for (PlannedSend send : sends) {
            sink.send(send.topic(), send.key(), send.value(), send.headers(), send.timestamp());
        }
        // Application code can still run after the post-handler check — a serializer
        // invoked during planning may hold the reader and latch a refusal there. The
        // step's effects have applied, but the transaction abort unwinds them.
        rethrowSeamViolation();
    }

    /** Rethrows a latched seam refusal, clearing the latch so it is raised exactly once. */
    private void rethrowSeamViolation() {
        ParsleyFailClosedException violation = swallowedSeamViolation;
        if (violation != null) {
            swallowedSeamViolation = null;
            throw violation;
        }
    }

    private static List<HeaderKV> withoutReservedHeaders(List<HeaderKV> headers) {
        List<HeaderKV> application = new ArrayList<>(headers.size());
        for (HeaderKV header : headers) {
            if (!header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                application.add(header);
            }
        }
        return application;
    }

    /** One resolved, serialized state write, ready to apply. */
    private record PlannedWrite(ByteStore store, byte[] key, byte[] value) {}

    /** One resolved, serialized, stamped emission, ready to send. */
    private record PlannedSend(String topic, byte[] key, byte[] value, RecordHeaders headers, long timestamp) {}

    /**
     * The store seam matches by identity where the send seam matches by name: a store read
     * returns a value the caller casts to the passed instance's types, so resolving a
     * look-alike store by name would smuggle a differently-typed codec into the
     * application's own frame. An emission is write-only and has no such path back.
     */
    private void requireDeclaredStore(Store<?, ?> store, String access) {
        if (store == null) {
            throw new IllegalArgumentException(definition.name() + ": " + access + " store must be non-null");
        }
        Store<?, ?> declared = definition.store(store.name());
        if (declared != store) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE,
                    definition.name() + ": " + access + " targets a store not declared by stores(...): "
                            + store.name()
                            + (declared == null ? "" : " (a Store instance other than the declared one)"));
        }
    }

    private PlannedWrite planWrite(Effects.StateWrite<?, ?> write) {
        Store<?, ?> declared = write.store();
        requireDeclaredStore(declared, "state write");
        String serdeTopic = serdeTopicByStore.get(declared.name());
        byte[] keyBytes = serialize(declared.keySerde(), serdeTopic, null, write.key());
        if (keyBytes == null) {
            // The Serializer contract permits signalling failure by returning null; a
            // null store key cannot address an entry, so it must fail the plan with its
            // reason rather than surface as the store's bare NPE during apply.
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                    definition.name() + ": " + declared.name() + " state write key serialized to null;"
                            + " the declared key serde could not encode it");
        }
        byte[] valueBytes = write.value() == null
                ? null : serialize(declared.valueSerde(), serdeTopic, null, write.value());
        return new PlannedWrite(appStores.get(declared.name()), keyBytes, valueBytes);
    }

    private PlannedSend planEmission(Effects.Emission<?, ?> emission, long timestamp) {
        String topic = emission.channel().topic();
        Channel<?, ?> declared = definition.sendChannel(topic);
        if (declared == null) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.EMISSION_TO_UNDECLARED_CHANNEL,
                    definition.name() + " emitted to undeclared channel " + topic);
        }
        RecordHeaders headers = toKafkaHeaders(emission.headers());
        // The declared channel's serdes produce the bytes, the way the store seam writes
        // with its declared store: name resolution decides the codec, so a second Channel
        // instance for a declared topic has no serdes to smuggle past sends(...).
        byte[] keyBytes = emission.key() == null
                ? null : serialize(declared.keySerde(), topic, headers, emission.key());
        byte[] valueBytes = emission.value() == null
                ? null : serialize(declared.valueSerde(), topic, headers, emission.value());
        // The emission's own headers were checked at construction, but the serializers were
        // just handed the mutable collection; re-check before the genuine stamp goes on, so
        // a header-writing serializer fails here instead of poisoning every receiver.
        for (Header header : headers) {
            if (header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                throw new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.RESERVED_HEADER_USED,
                        definition.name() + ": serializer for " + topic + " wrote reserved header '"
                                + header.key() + "'");
            }
        }
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, causesHeader.get()));
        return new PlannedSend(topic, keyBytes, valueBytes, headers, timestamp);
    }

    /**
     * Serializes through the declared serde. A type-level mismatch between a look-alike
     * effect instance and the declared one lands in the unchecked cast's
     * {@code ClassCastException}, wrapped with a reason here so the stop is diagnosable —
     * though a declared serde typed loosely enough to accept any object serializes a
     * mismatched payload as-is (D73's Cost records this).
     */
    @SuppressWarnings("unchecked")
    private byte[] serialize(Serde<?> serde, String topic, RecordHeaders headers, Object data) {
        try {
            Serializer<Object> serializer = (Serializer<Object>) serde.serializer();
            return headers == null
                    ? serializer.serialize(topic, data)
                    : serializer.serialize(topic, headers, data);
        } catch (RuntimeException e) {
            // A reader refusal thrown through the serializer keeps its own reason rather
            // than being relabeled as a payload failure.
            rethrowSeamViolation();
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                    definition.name() + ": " + topic + " payload could not be serialized by the"
                            + " declared serde", e);
        }
    }

    static RecordHeaders toKafkaHeaders(List<HeaderKV> headers) {
        RecordHeaders kafkaHeaders = new RecordHeaders();
        for (HeaderKV header : headers) {
            kafkaHeaders.add(new RecordHeader(header.key(), header.value()));
        }
        return kafkaHeaders;
    }

    private final class StoreStateReader implements StateReader {
        @Override
        public <K, V> V get(Store<K, V> store, K key) {
            try {
                requireDeclaredStore(store, "state read");
            } catch (ParsleyFailClosedException e) {
                throw latched(e);
            }
            if (key == null) {
                throw new IllegalArgumentException(store.name() + ": state read key must be non-null");
            }
            String serdeTopic = serdeTopicByStore.get(store.name());
            byte[] keyBytes;
            try {
                keyBytes = store.keySerde().serializer().serialize(serdeTopic, key);
            } catch (RuntimeException e) {
                throw latched(new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                        definition.name() + ": " + store.name() + " state read key could not be serialized"
                                + " by the declared serde", e));
            }
            if (keyBytes == null) {
                // The Serializer contract permits signalling failure by returning null;
                // without this guard that shape surfaces as the store's bare NPE inside
                // the handler's frame, unlatched and swallowable.
                throw latched(new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                        definition.name() + ": " + store.name() + " state read key serialized to null;"
                                + " the declared key serde could not encode it"));
            }
            byte[] valueBytes = appStores.get(store.name()).get(keyBytes);
            if (valueBytes == null) {
                return null;
            }
            try {
                return store.valueSerde().deserializer().deserialize(serdeTopic, valueBytes);
            } catch (RuntimeException e) {
                throw latched(new ParsleyFailClosedException(
                        ParsleyFailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                        definition.name() + ": " + store.name() + " stored value could not be decoded"
                                + " by the declared serde", e));
            }
        }

        /** Latches a refusal raised inside the handler's frame, so a catch cannot swallow it. */
        private ParsleyFailClosedException latched(ParsleyFailClosedException e) {
            swallowedSeamViolation = e;
            return e;
        }
    }
}
