package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;


/**
 * The Kafka Streams processor driving one {@link ProcessEngine}.
 *
 * <p>Each record is fed to the engine, then every message the engine declares deliverable is
 * decoded, handed to its handler, and the handler's effects are written and forwarded. State
 * writes, sends and consumed positions commit together under {@code exactly_once_v2}.
 *
 * <p>Nothing is asked of the broker between deliveries. A cause names the position of a
 * message that was sent, so receiving that message is what satisfies it (wire-format
 * constraint 8, D115). Task initialisation asks the substrate one question — which of the
 * topics its state names still exist — and settles or refuses on the answer; a wall-clock
 * punctuation then only drains what receipt already released, flushes holds, and asks the
 * identity question again where it went unanswered.
 *
 * @see ProcessTopology
 */
final class ProcessNode implements Processor<byte[], byte[], byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessNode.class);

    private final Process definition;
    private final Map<String, ResolvedTopic> topics;
    private final TopicIdentitySource identitySource;
    private final Map<TopicPartition, Long> startPositions;
    private final Duration punctuationInterval;
    private final int metadataBudgetBytes;

    /** How often each task runs its punctuation: the drain, the flush and the identity retry. */
    static final Duration PUNCTUATION_INTERVAL = Duration.ofSeconds(1);

    private Cancellable punctuator;
    /**
     * True from an initialisation until its identity question has been answered. A source
     * that could not answer at initialisation is asked again from the punctuation, so the
     * check is event-driven and eventual, never periodic (D115) — and, since each attempt
     * can block the stream thread for the describe's timeout, not before
     * {@link #identityRetryNotBefore}, which backs off exponentially from one punctuation
     * interval to {@link #IDENTITY_RETRY_CAP} while the substrate keeps not answering.
     */
    private boolean identityCheckPending;
    private long identityRetryNotBefore;
    private Duration identityRetryDelay;
    static final Duration IDENTITY_RETRY_CAP = Duration.ofMinutes(1);

    private ProcessorContext<byte[], byte[]> context;
    private ProcessEngine engine;
    private int partition;
    private final Map<String, Channel> channelByTopic = new HashMap<>();
    private final Map<Channel, String> topicByChannel = new HashMap<>();
    private final Map<String, KeyValueStore<Bytes, byte[]>> appStores = new HashMap<>();
    private final Map<String, String> serdeTopicByStore = new HashMap<>();
    private State state;
    /**
     * A fail-closed refusal raised by the {@link State} inside application code. The state
     * latches it here before throwing, and {@code deliver} rethrows at every seam boundary
     * — frame entry, after the delivered payload's deserializers, after the handler, and
     * after the planned effects apply — so an application catch cannot commit a step whose
     * reads were refused, wherever in the frame the read ran. Volatile because the reader
     * is an object application code can hold: a latch written from an application thread
     * must be visible to the stream thread's next check.
     */
    private volatile FailClosedException swallowedSeamViolation;

    /**
     * @param definition          the process this instance runs
     * @param topics              resolved identity and width for every topic it uses
     * @param identitySource      where topic identity is checked at task initialisation
     * @param startPositions      per received partition, the position the host feeds first,
     *                            as the bootstrap established it (SPEC Host obligation 2). A
     *                            task re-created mid-run is handed the same map; its restored
     *                            coverage is already at or past it, and coverage is never
     *                            lowered, so the position matters only to a task with no
     *                            state behind it
     * @param punctuationInterval how often each task runs its punctuation
     * @param metadataBudgetBytes the largest causal metadata a message may carry
     */
    ProcessNode(Process definition, Map<String, ResolvedTopic> topics,
                     TopicIdentitySource identitySource, Map<TopicPartition, Long> startPositions,
                     Duration punctuationInterval, int metadataBudgetBytes) {
        this.definition = definition;
        this.topics = topics;
        this.identitySource = identitySource;
        this.startPositions = Map.copyOf(startPositions);
        this.punctuationInterval = punctuationInterval;
        this.metadataBudgetBytes = metadataBudgetBytes;
    }

    /**
     * Builds the engine for this task, restores its ordering state, and checks the identity
     * of every topic that state names.
     *
     * <p>Runs on every initialisation of the task on this thread: a first assignment, a
     * migration, and the host's own re-creation of a task whose source topic went missing.
     * That is what makes the identity check event-driven rather than periodic (D115).
     * Nothing is delivered from here (D34): a hold the identity report releases goes on the
     * next punctuation or record.
     *
     * @param context the task context
     * @throws FailClosedException if restored state cannot be read, if the task
     *         width changed so that state no longer matches its partitioning, if a received
     *         topic was recreated under its name, or if one was deleted while messages from
     *         it remain held
     */
    @Override
    public void init(ProcessorContext<byte[], byte[]> context) {
        // A revived task runs close() and then init() on this same instance against restored
        // state; both cancel the previous incarnation's punctuator, so a lifecycle that
        // re-initialises without closing is covered too.
        if (punctuator != null) {
            punctuator.cancel();
            punctuator = null;
        }

        this.context = context;
        partition = context.taskId().partition();

        channelByTopic.clear();
        topicByChannel.clear();
        Map<Channel, Long> taskStartPositions = new HashMap<>();
        for (String topic : ProcessTopology.inputTopics(definition)) {
            ResolvedTopic info = topics.get(topic);
            if (partition < info.partitions()) {
                Channel channel = new Channel(info.topicId(), partition);
                channelByTopic.put(topic, channel);
                topicByChannel.put(channel, topic);
                Long start = startPositions.get(new TopicPartition(topic, partition));
                if (start != null) {
                    taskStartPositions.put(channel, start);
                }
            }
        }

        KeyValueStore<Bytes, byte[]> orderingStore = context.getStateStore(ProcessTopology.ORDERING_STORE);
        engine = new ProcessEngine(definition.name() + "-" + context.taskId(),
                topicByChannel, new StreamsOrderingStore(orderingStore), metadataBudgetBytes, taskStartPositions);

        appStores.clear();
        serdeTopicByStore.clear();
        for (Store<?, ?> store : definition.stores()) {
            appStores.put(store.name(), context.getStateStore(store.name()));
            // Composed once per store: the same name start() validated, recomposing it per
            // state access would be a dead length check on the hot path.
            serdeTopicByStore.put(store.name(),
                    ProcessTopology.changelogName(context.applicationId(), store.name()));
        }
        state = new StreamsState();
        swallowedSeamViolation = null;

        identityRetryDelay = punctuationInterval;
        identityRetryNotBefore = Long.MIN_VALUE;
        checkIdentity();

        punctuator = context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if (identityCheckPending && timestamp >= identityRetryNotBefore) {
                identityRetryNotBefore = timestamp + identityRetryDelay.toMillis();
                identityRetryDelay = min(identityRetryDelay.multipliedBy(2), IDENTITY_RETRY_CAP);
                checkIdentity();
            }
            drain();
            engine.flushHolds();
        });
    }

    /**
     * Asks the identity source about every topic this task's state names — the received
     * topics at the identity resolved at start, and every topic in the restored frontier —
     * and hands the engine what was confirmed gone. A source that cannot answer is not
     * evidence: the causes stay expressed, nothing settles, and the question stays pending,
     * to be asked again at the next punctuation until it is answered (D44's rule, kept:
     * absence of an answer is never a verdict).
     */
    private void checkIdentity() {
        // Every channel this task's state names, defined once: the received channels and
        // the restored frontier.
        Set<Channel> known = new HashSet<>(engine.frontierSnapshot().byChannel().keySet());
        known.addAll(engine.receivedChannelSet());
        Set<UUID> topicIds = new HashSet<>();
        for (Channel channel : known) {
            topicIds.add(channel.topicId());
        }
        TopicIdentityVerdicts verdicts;
        try {
            verdicts = identitySource.resolve(topicIds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            identityCheckPending = true;
            return;
        } catch (Exception e) {
            identityCheckPending = true;
            LOG.warn("{}: topic identity could not be checked ({}); continuing on the identities resolved"
                    + " at start, and asking again from the punctuation", definition.name(), e.toString());
            LOG.debug("{}: identity check failure", definition.name(), e);
            return;
        }
        identityCheckPending = !verdicts.answered();
        if (!identityCheckPending) {
            identityRetryDelay = punctuationInterval;
        }
        if (verdicts.deleted().isEmpty() && verdicts.recreated().isEmpty()) {
            return;
        }
        Set<Channel> dead = new HashSet<>();
        Set<Channel> recreated = new HashSet<>();
        for (Channel channel : known) {
            if (verdicts.recreated().contains(channel.topicId())) {
                recreated.add(channel);
            } else if (verdicts.deleted().contains(channel.topicId())) {
                dead.add(channel);
            }
        }
        engine.onIdentityReport(new IdentityReport(dead, recreated));
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Cancels the punctuator. On the revival path this runs before the successor's
     * {@code init}, which repeats the cancellation.
     */
    @Override
    public void close() {
        if (punctuator != null) {
            punctuator.cancel();
            punctuator = null;
        }
    }

    /**
     * Feeds one record to the engine and delivers whatever that makes deliverable.
     *
     * @param record the record, as raw bytes
     * @throws FailClosedException if the guarantee cannot be upheld, which stops this
     *         process
     */
    @Override
    public void process(Record<byte[], byte[]> record) {
        RecordMetadata metadata = context.recordMetadata().orElseThrow(() ->
                new IllegalStateException("record without topic metadata reached " + definition.name()));
        Channel channel = channelByTopic.get(metadata.topic());
        if (channel == null) {
            throw new IllegalStateException(definition.name() + " fed from undeclared topic " + metadata.topic());
        }
        List<Header> headers = new ArrayList<>();
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            headers.add(new Header(header.key(), header.value()));
        }
        engine.onReceive(new ReceivedMessage(
                channel, metadata.offset(), record.timestamp(), record.key(), record.value(), headers));
        drain();
        engine.flushHolds();
    }

    private void drain() {
        while (true) {
            Optional<DeliverableMessage> next = engine.nextDeliverable();
            if (next.isEmpty()) {
                return;
            }
            DeliverableMessage message = next.get();
            engine.markDelivered(message.channel(), message.position());
            deliver(definition.input(topicByChannel.get(message.channel())), message);
        }
    }

    private <K, V> void deliver(Process.Input<K, V> input, DeliverableMessage message) {
        // Checked at entry, never blanket-reset mid-frame: a reset placed after any
        // application code would erase what that code latched. The delivered payload's
        // own deserializers are application code and run before the handler, so a refusal
        // they latch through a captured reader must fail this step, not vanish.
        rethrowSeamViolation();
        Topic<K, V> declared = input.topic();
        String topic = declared.name();
        // Reserved transport headers are parsley's own carriage, invisible to application
        // logic in both directions (D56): deserializers see exactly the headers the
        // application sent, the same view Delivery presents one frame later.
        List<Header> applicationHeaders = withoutReservedHeaders(message.headers());
        RecordHeaders receivedHeaders = toKafkaHeaders(applicationHeaders);
        K key;
        V value;
        try {
            key = message.key() == null
                    ? null : declared.keySerde().deserializer().deserialize(topic, receivedHeaders, message.key());
            value = message.value() == null
                    ? null : declared.valueSerde().deserializer().deserialize(topic, receivedHeaders, message.value());
        } catch (RuntimeException e) {
            // A reader refusal thrown through the deserializer keeps its own reason: the
            // latch identifies it, and wrapping it as a payload failure would mislabel
            // the stop for status().
            rethrowSeamViolation();
            throw new FailClosedException(
                    FailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                    definition.name() + ": " + topic + "@" + message.position(), e);
        }
        rethrowSeamViolation();

        Delivery<K, V> delivery = Delivery.of(declared, message.channel(), message.position(),
                message.timestamp(), key, value, applicationHeaders);
        Effects effects = input.handler().handle(delivery, state);
        // The reader's refusal was thrown inside the handler's own frame, where an
        // application catch can swallow it; the latch makes the step fail regardless,
        // as docs/failing-closed.md promises for every fail-closed event.
        rethrowSeamViolation();
        if (effects == null) {
            // A deliberate refusal that recurs identically on restart, so it carries its
            // own reason and reaches status() rather than an empty refusalReason.
            throw new FailClosedException(
                    FailClosedException.Reason.HANDLER_RETURNED_NULL_EFFECTS,
                    definition.name() + ": handler for " + topic + " returned null effects; return"
                            + " Effects.none() for a step that changes nothing");
        }
        // Plan, then apply: resolving every effect's declared target and serializing every
        // payload is a pure function of the definition and the returned Effects, so every
        // refusal that can be raised here — undeclared target, unserializable payload,
        // reserved header, exceeded metadata budget — fires before the first write reaches
        // RocksDB or the first record is forwarded, never relying on the EOS abort alone to
        // unwind a half-applied step. Apply then consumes the plan, so an effect cannot
        // reach a store or a sink without having been planned.
        List<PlannedWrite> writes = new ArrayList<>(effects.writes().size());
        for (Effects.Write<?, ?> write : effects.writes()) {
            writes.add(planWrite(write));
        }
        List<PlannedSend> sends = new ArrayList<>(effects.sends().size());
        for (Effects.Send<?, ?> send : effects.sends()) {
            sends.add(planSend(send, send.timestamp().orElse(message.timestamp())));
        }
        for (PlannedWrite write : writes) {
            if (write.value() == null) {
                write.store().delete(write.key());
            } else {
                write.store().put(write.key(), write.value());
            }
        }
        for (PlannedSend send : sends) {
            context.forward(send.record(), send.sinkName());
        }
        // Application code can still run after the post-handler check — a serializer
        // invoked during planning may hold the reader and latch a refusal there. The
        // step's effects have applied, but the EOS abort unwinds them.
        rethrowSeamViolation();
    }

    /** Rethrows a latched seam refusal, clearing the latch so it is raised exactly once. */
    private void rethrowSeamViolation() {
        FailClosedException violation = swallowedSeamViolation;
        if (violation != null) {
            swallowedSeamViolation = null;
            throw violation;
        }
    }

    private static List<Header> withoutReservedHeaders(List<Header> headers) {
        List<Header> application = new ArrayList<>(headers.size());
        for (Header header : headers) {
            if (!header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                application.add(header);
            }
        }
        return application;
    }

    /** One resolved, serialized state write, ready to apply. */
    private record PlannedWrite(KeyValueStore<Bytes, byte[]> store, Bytes key, byte[] value) {}

    /** One resolved, serialized, stamped send, ready to forward. */
    private record PlannedSend(Record<byte[], byte[]> record, String sinkName) {}

    /**
     * The store seam matches by identity where the send seam matches by name: a store read
     * returns a value the caller casts to the passed instance's types, so resolving a
     * look-alike store by name would smuggle a differently-typed codec into the
     * application's own frame. A send is write-only and has no such path back.
     */
    private void requireDeclaredStore(Store<?, ?> store, String access) {
        if (store == null) {
            throw new IllegalArgumentException(definition.name() + ": " + access + " store must be non-null");
        }
        Store<?, ?> declared = definition.store(store.name());
        if (declared != store) {
            throw new FailClosedException(
                    FailClosedException.Reason.STATE_ACCESS_TO_UNDECLARED_STORE,
                    definition.name() + ": " + access + " targets a store not declared by stores(...): "
                            + store.name()
                            + (declared == null ? "" : " (a Store instance other than the declared one)"));
        }
    }

    private PlannedWrite planWrite(Effects.Write<?, ?> write) {
        Store<?, ?> declared = write.store();
        requireDeclaredStore(declared, "state write");
        String serdeTopic = serdeTopicByStore.get(declared.name());
        byte[] keyBytes = serialize(declared.keySerde(), serdeTopic, null, write.key());
        if (keyBytes == null) {
            // The Serializer contract permits signalling failure by returning null; a
            // null store key cannot address an entry, so it must fail the plan with its
            // reason rather than surface as the store's bare NPE during apply.
            throw new FailClosedException(
                    FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                    definition.name() + ": " + declared.name() + " state write key serialized to null;"
                            + " the declared key serde could not encode it");
        }
        byte[] valueBytes = write.value() == null
                ? null : serialize(declared.valueSerde(), serdeTopic, null, write.value());
        return new PlannedWrite(appStores.get(declared.name()), Bytes.wrap(keyBytes), valueBytes);
    }

    private PlannedSend planSend(Effects.Send<?, ?> send, long timestamp) {
        String topic = send.topic().name();
        Topic<?, ?> declared = definition.output(topic);
        if (declared == null) {
            throw new FailClosedException(
                    FailClosedException.Reason.SEND_TO_UNDECLARED_TOPIC,
                    definition.name() + " sent to undeclared topic " + topic);
        }
        RecordHeaders headers = toKafkaHeaders(send.headers());
        // The declared topic's serdes produce the bytes, the way the store seam writes
        // with its declared store: name resolution decides the codec, so a second Topic
        // instance for a declared topic has no serdes to smuggle past sends(...).
        byte[] keyBytes = send.key() == null
                ? null : serialize(declared.keySerde(), topic, headers, send.key());
        byte[] valueBytes = send.value() == null
                ? null : serialize(declared.valueSerde(), topic, headers, send.value());
        // The send's own headers were checked at construction, but the serializers were
        // just handed the mutable collection; re-check before the genuine stamp goes on, so
        // a header-writing serializer fails here instead of poisoning every receiver.
        for (org.apache.kafka.common.header.Header header : headers) {
            if (header.key().startsWith(CausesCodec.RESERVED_HEADER_PREFIX)) {
                throw new FailClosedException(
                        FailClosedException.Reason.RESERVED_HEADER_USED,
                        definition.name() + ": serializer for " + topic + " wrote reserved header '"
                                + header.key() + "'");
            }
        }
        headers.add(new RecordHeader(CausesCodec.HEADER_KEY, engine.causesHeaderForSend()));
        return new PlannedSend(new Record<>(keyBytes, valueBytes, timestamp, headers), ProcessTopology.sinkName(topic));
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
            throw new FailClosedException(
                    FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                    definition.name() + ": " + topic + " payload could not be serialized by the"
                            + " declared serde", e);
        }
    }

    private static RecordHeaders toKafkaHeaders(List<Header> headers) {
        RecordHeaders kafkaHeaders = new RecordHeaders();
        for (Header header : headers) {
            kafkaHeaders.add(new RecordHeader(header.key(), header.value()));
        }
        return kafkaHeaders;
    }

    private final class StreamsState implements State {
        @Override
        public <K, V> V get(Store<K, V> store, K key) {
            try {
                requireDeclaredStore(store, "state read");
            } catch (FailClosedException e) {
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
                throw latched(new FailClosedException(
                        FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                        definition.name() + ": " + store.name() + " state read key could not be serialized"
                                + " by the declared serde", e));
            }
            if (keyBytes == null) {
                // The Serializer contract permits signalling failure by returning null;
                // without this guard that shape surfaces as the store's bare NPE inside
                // the handler's frame, unlatched and swallowable.
                throw latched(new FailClosedException(
                        FailClosedException.Reason.APPLICATION_PAYLOAD_UNSERIALIZABLE,
                        definition.name() + ": " + store.name() + " state read key serialized to null;"
                                + " the declared key serde could not encode it"));
            }
            byte[] valueBytes = appStores.get(store.name()).get(Bytes.wrap(keyBytes));
            if (valueBytes == null) {
                return null;
            }
            try {
                return store.valueSerde().deserializer().deserialize(serdeTopic, valueBytes);
            } catch (RuntimeException e) {
                throw latched(new FailClosedException(
                        FailClosedException.Reason.APPLICATION_PAYLOAD_UNDECODABLE,
                        definition.name() + ": " + store.name() + " stored value could not be decoded"
                                + " by the declared serde", e));
            }
        }

        /** Latches a refusal raised inside the handler's frame, so a catch cannot swallow it. */
        private FailClosedException latched(FailClosedException e) {
            swallowedSeamViolation = e;
            return e;
        }
    }
}
