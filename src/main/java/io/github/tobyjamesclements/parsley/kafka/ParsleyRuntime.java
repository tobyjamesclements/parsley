package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * Owns the Kafka Streams application behind each running process.
 *
 * <p>Topic identity and partition width are resolved once at start, so a process runs against
 * a fixed view of the topics it uses. A change to that view after start is a reason to refuse
 * rather than to adapt.
 *
 * <p>Configuration carrying the guarantee is set here and cannot be overridden:
 * {@code exactly_once_v2}, {@code read_committed}, and no automatic offset reset.
 *
 * @see io.github.tobyjamesclements.parsley.api.Parsley
 */
public final class ParsleyRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ParsleyRuntime.class);
    private static final long TIMEOUT_SECONDS = 30;

    private final Admin admin;
    // Populated by start() and read by status()/healthy()/close() from monitoring threads,
    // so these are concurrent like failuresByProcess; insertion order is preserved for
    // status reporting.
    private final Map<String, KafkaStreams> streamsByProcess =
            java.util.Collections.synchronizedMap(new LinkedHashMap<>());
    private final java.util.concurrent.ConcurrentHashMap<String, Throwable> failuresByProcess =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final List<KafkaStreams> streams = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<AdminFactsSource> factsSources = new java.util.concurrent.CopyOnWriteArrayList<>();

    private final java.util.concurrent.ExecutorService factsExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "parsley-facts");
                thread.setDaemon(true);
                return thread;
            });

    private ParsleyRuntime(Admin admin) {
        this.admin = admin;
    }

    /**
     * Resolves topics, builds a topology per process, and starts each one.
     *
     * @param config      broker connection, identity and metadata budget
     * @param definitions the processes to run, with distinct names
     * @return the running runtime
     * @throws ParsleyFailClosedException if a process cannot start without breaching the
     *         guarantee, for example when messages remain held on a channel it no longer
     *         receives, or when a topic was recreated under a name it has state for
     * @throws IllegalArgumentException if names collide or a topic uses a reserved name
     */
    public static ParsleyRuntime start(ParsleyConfig config, List<ProcessDefinition> definitions) {
        validateDistinctNames(definitions);
        refuseReservedTopicNames(config, definitions);
        Map<String, Object> adminProps = new HashMap<>(config.extraProperties());
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        Admin admin = Admin.create(adminProps);
        ParsleyRuntime runtime = new ParsleyRuntime(admin);
        try {
            Map<String, TopicInfo> topics = runtime.resolveTopics(declaredTopics(definitions));

            long factsClockOrigin = System.nanoTime();
            for (ProcessDefinition definition : definitions) {
                String applicationId = config.applicationIdPrefix() + "-" + definition.name();
                java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> changelog =
                        runtime.describeChangelog(applicationId);
                boolean priorState = changelog.isPresent();

                runtime.refuseStrandedHeldMessages(applicationId, definition, topics, priorState, adminProps);
                runtime.refuseWidthChange(applicationId, definition, topics, changelog);
                runtime.commitInitialPositions(applicationId, definition, topics, priorState, adminProps);
                Map<UUID, String> namesById = new HashMap<>();
                topics.forEach((name, info) -> namesById.put(info.topicId(), name));

                AdminFactsSource factsSource = new AdminFactsSource(admin, applicationId, namesById, adminProps,
                        Math.max(config.factsInterval().toMillis() * 3, 3_000L),
                        () -> (System.nanoTime() - factsClockOrigin) / 1_000_000L);
                runtime.factsSources.add(factsSource);
                KafkaStreams kafkaStreams = new KafkaStreams(
                        ProcessTopology.build(definition, topics, factsSource, config.factsInterval(),
                                runtime.factsExecutor, config.metadataBudgetBytes()),
                        streamsProperties(config, applicationId));
                kafkaStreams.setUncaughtExceptionHandler(exception -> {
                    runtime.recordFailure(definition.name(), exception);
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
                });
                runtime.streams.add(kafkaStreams);
                runtime.streamsByProcess.put(definition.name(), kafkaStreams);
            }
            runtime.streams.forEach(KafkaStreams::start);
            return runtime;
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
    }

    private void recordFailure(String process, Throwable exception) {
        failuresByProcess.merge(process, exception, (existing, latest) ->
                ParsleyFailClosedException.findIn(existing) == null && ParsleyFailClosedException.findIn(latest) != null ? latest : existing);

        Throwable cause = exception;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.clients.consumer.NoOffsetForPartitionException
                    || String.valueOf(cause.getMessage()).contains("invalid partitions")) {
                LOG.error("process {}: the partition shape of its topics changed while it ran; parsley resolves"
                        + " partitions at start(). Restart the application: a width-preserving expansion is"
                        + " re-resolved and pre-committed; a width change refuses with TASK_WIDTH_CHANGED and its"
                        + " remedy (failing closed)", process, exception);
                return;
            }
        }
        LOG.error("process {} failed; shutting its application down (failing closed)", process, exception);
    }

    /**
     * Reports the state of every process.
     *
     * @return the current state of every process, keyed by name, with a refusal reason where
     *         one stopped to preserve the guarantee
     */
    public Map<String, io.github.tobyjamesclements.parsley.api.ProcessStatus> status() {
        Map<String, io.github.tobyjamesclements.parsley.api.ProcessStatus> statuses = new LinkedHashMap<>();
        streamsByProcess.forEach((process, kafkaStreams) -> {
            KafkaStreams.State state = kafkaStreams.state();
            io.github.tobyjamesclements.parsley.api.ProcessStatus.State mapped = switch (state) {
                case RUNNING -> io.github.tobyjamesclements.parsley.api.ProcessStatus.State.RUNNING;
                case REBALANCING -> io.github.tobyjamesclements.parsley.api.ProcessStatus.State.REBALANCING;
                default -> io.github.tobyjamesclements.parsley.api.ProcessStatus.State.STOPPED;
            };
            Throwable failure = failuresByProcess.get(process);
            ParsleyFailClosedException refusal =
                    ParsleyFailClosedException.findIn(failure);
            statuses.put(process, new io.github.tobyjamesclements.parsley.api.ProcessStatus(process, mapped,
                    java.util.Optional.ofNullable(refusal).map(ParsleyFailClosedException::reason),
                    java.util.Optional.ofNullable(failure).map(Throwable::getMessage)));
        });
        return statuses;
    }

    private static void refuseReservedTopicNames(ParsleyConfig config, List<ProcessDefinition> definitions) {
        // Composed changelog names must be distinct across every process: process names
        // are distinct, but composition can still collide ("app-orders" + "audit-log" and
        // "app-orders-audit" + "log" both give app-orders-audit-log-changelog), and a
        // silently deduped collision would have two Streams applications sharing one
        // changelog, each restoring the other's records.
        Map<String, String> ownerByChangelog = new HashMap<>();
        for (ProcessDefinition definition : definitions) {
            String applicationId = config.applicationIdPrefix() + "-" + definition.name();
            registerChangelog(ownerByChangelog,
                    ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE),
                    definition.name());
            for (Store<?, ?> store : definition.stores()) {
                registerChangelog(ownerByChangelog,
                        ProcessTopology.changelogName(applicationId, store.name()),
                        definition.name());
            }
        }
        for (String topic : declaredTopics(definitions)) {
            if (topic.contains(Store.RESERVED_PREFIX) || ownerByChangelog.containsKey(topic)) {
                throw new IllegalArgumentException("topic '" + topic + "' collides with parsley's internal"
                        + " namespace; choose another name");
            }
        }
    }

    private static void registerChangelog(Map<String, String> ownerByChangelog, String changelog, String process) {
        String owner = ownerByChangelog.putIfAbsent(changelog, process);
        if (owner != null) {
            throw new IllegalArgumentException("processes " + owner + " and " + process
                    + " compose the same changelog topic '" + changelog + "'; rename a process or"
                    + " store so every changelog name is distinct");
        }
    }

    private static void validateDistinctNames(List<ProcessDefinition> definitions) {
        Set<String> names = new HashSet<>();
        for (ProcessDefinition definition : definitions) {
            if (!names.add(definition.name())) {
                throw new IllegalArgumentException("duplicate process name " + definition.name());
            }
        }
        if (definitions.isEmpty()) {
            throw new IllegalArgumentException("at least one process must be declared");
        }
    }

    private static Set<String> declaredTopics(List<ProcessDefinition> definitions) {
        Set<String> topics = new HashSet<>();
        for (ProcessDefinition definition : definitions) {
            topics.addAll(definition.receivedTopics());
            topics.addAll(definition.sendTopics());
        }
        return topics;
    }

    private Map<String, TopicInfo> resolveTopics(Set<String> names) {
        try {
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(names).allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<String, TopicInfo> topics = new LinkedHashMap<>();
            descriptions.forEach((name, description) -> {
                if (org.apache.kafka.common.Uuid.ZERO_UUID.equals(description.topicId())) {
                    throw new ParsleyFailClosedException(
                            ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                            "topic '" + name + "' has no topic ID; brokers below the supported 3.7.0 floor cannot"
                                    + " provide channel identity (SPEC Substrate 1, Assumption 2); refusing to start");
                }
                topics.put(name, new TopicInfo(
                        TopicInfo.toJavaUuid(description.topicId()), description.partitions().size()));
            });
            return topics;
        } catch (ParsleyFailClosedException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("declared topics could not be resolved; refusing to start", e);
        }
    }

    private java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> describeChangelog(String applicationId) {
        String changelog = ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE);
        try {
            return java.util.Optional.of(admin.describeTopics(List.of(changelog)).allTopicNames()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(changelog));
        } catch (Exception e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                return java.util.Optional.empty();
            }
            throw new IllegalStateException(applicationId + ": could not determine prior state; refusing to start", e);
        }
    }

    private void refuseWidthChange(String applicationId, ProcessDefinition definition,
                                   Map<String, TopicInfo> topics,
                                   java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> changelog) {
        if (changelog.isEmpty()) {
            return;
        }
        int declaredWidth = 0;
        for (String topic : definition.receivedTopics()) {
            declaredWidth = Math.max(declaredWidth, topics.get(topic).partitions());
        }
        int storedWidth = changelog.get().partitions().size();
        if (storedWidth != declaredWidth) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED,
                    applicationId + ": this process's ordering state was built for " + storedWidth
                            + " task(s) but the declaration now induces " + declaredWidth
                            + " (the widest received topic's partition count changed). The ordering store's"
                            + " changelog cannot change width; restore the previous declaration and partition"
                            + " counts, or reset the process's state and group offsets deliberately.");
        }
    }

    private void refuseStrandedHeldMessages(String applicationId, ProcessDefinition definition,
                                            Map<String, TopicInfo> topics, boolean priorState,
                                            Map<String, Object> clientProps) {
        if (!priorState) {
            return;
        }
        String changelog = ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE);
        Map<String, Object> props = new HashMap<>(clientProps);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        Map<byte[], byte[]> latest = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props,
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer())) {
            List<TopicPartition> parts = consumer.partitionsFor(changelog).stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition())).toList();
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);

            long stallDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (parts.stream().anyMatch(tp -> consumer.position(tp) < ends.get(tp))) {
                var polled = consumer.poll(java.time.Duration.ofMillis(500));
                if (polled.isEmpty()) {
                    if (System.nanoTime() - stallDeadline > 0) {
                        throw new IllegalStateException("no progress reading " + changelog + " for "
                                + TIMEOUT_SECONDS + "s while verifying held messages");
                    }
                } else {
                    stallDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
                    polled.forEach(record -> latest.put(record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": could not verify held messages against the declaration; refusing to start", e);
        }

        Map<String, UUID> resolvedIds = new HashMap<>();
        definition.receivedTopics().forEach(topic -> resolvedIds.put(topic, topics.get(topic).topicId()));
        List<String> identityChanged = io.github.tobyjamesclements.parsley.core.OrderingStateInspector
                .identityChangedTopics(latest, resolvedIds);
        if (!identityChanged.isEmpty()) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.CHANNEL_IDENTITY_CHANGED,
                    applicationId + ": topics " + identityChanged + " now resolve to different identities than"
                            + " this process's state was built against; their read positions for those names cannot"
                            + " be trusted. Reset the process's state and group offsets deliberately to proceed.");
        }
        java.util.Set<io.github.tobyjamesclements.parsley.core.ChannelId> declared = new java.util.TreeSet<>();
        for (String topic : definition.receivedTopics()) {
            TopicInfo info = topics.get(topic);
            for (int partition = 0; partition < info.partitions(); partition++) {
                declared.add(new io.github.tobyjamesclements.parsley.core.ChannelId(info.topicId(), partition));
            }
        }
        java.util.Set<io.github.tobyjamesclements.parsley.core.ChannelId> stranded =
                new java.util.TreeSet<>(io.github.tobyjamesclements.parsley.core.OrderingStateInspector.heldChannels(latest));
        stranded.removeAll(declared);
        if (!stranded.isEmpty()) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES,
                    applicationId + ": received messages remain undelivered on " + stranded
                            + ", which the new declaration no longer receives");
        }
    }

    private void commitInitialPositions(String applicationId, ProcessDefinition definition,
                                        Map<String, TopicInfo> topics, boolean priorState,
                                        Map<String, Object> clientProps) {
        try {
            Map<TopicPartition, OffsetAndMetadata> preCheck =
                    admin.listConsumerGroupOffsets(applicationId).partitionsToOffsetAndMetadata()
                            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (preCheck.keySet().containsAll(receivedPartitions(definition, topics))) {
                return;
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": committed read positions could not be listed; refusing to start", e);
        }

        try (GroupMembershipCommitter committer = new GroupMembershipCommitter(clientProps, applicationId)) {
            committer.join(definition.receivedTopics(), streamsSessionTimeout(clientProps).multipliedBy(2));
            java.util.Set<TopicPartition> all = receivedPartitions(definition, topics);
            Map<TopicPartition, OffsetAndMetadata> committed = committer.committed(all);
            Map<TopicPartition, OffsetSpec> wanted = new HashMap<>();
            for (TopicPartition tp : all) {
                if (committed.get(tp) == null) {
                    Channel.InitialPosition initial = priorState
                            ? Channel.InitialPosition.EARLIEST
                            : definition.input(tp.topic()).channel().initialPosition();
                    wanted.put(tp, initial == Channel.InitialPosition.EARLIEST
                            ? OffsetSpec.earliest() : OffsetSpec.latest());
                }
            }
            if (wanted.isEmpty()) {
                return;
            }
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            admin.listOffsets(wanted, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .forEach((tp, info) -> toCommit.put(tp, new OffsetAndMetadata(info.offset())));
            committer.commit(toCommit);
            LOG.info("{}: committed initial positions for {}", applicationId, toCommit.keySet());
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": initial read positions could not be established; refusing to start", e);
        }
    }

    private static java.util.Set<TopicPartition> receivedPartitions(ProcessDefinition definition,
                                                                    Map<String, TopicInfo> topics) {
        java.util.Set<TopicPartition> all = new java.util.HashSet<>();
        for (String topic : definition.receivedTopics()) {
            for (int partition = 0; partition < topics.get(topic).partitions(); partition++) {
                all.add(new TopicPartition(topic, partition));
            }
        }
        return all;
    }

    private static java.time.Duration streamsSessionTimeout(Map<String, Object> clientProps) {
        for (String key : new String[] {
                StreamsConfig.mainConsumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG),
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG}) {
            Object value = clientProps.get(key);
            if (value != null) {
                return java.time.Duration.ofMillis(Long.parseLong(String.valueOf(value)));
            }
        }
        return java.time.Duration.ofSeconds(45);
    }

    private static Properties streamsProperties(ParsleyConfig config, String applicationId) {
        Properties props = new Properties();
        props.putAll(config.extraProperties());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());

        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "none");
        if (config.stateDir() != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
        }
        return props;
    }

    /**
     * Returns {@code true} while every process is running or rebalancing.
     *
     * @return {@code true} while every process is running or rebalancing
     */
    public boolean healthy() {
        return failuresByProcess.isEmpty() && streams.stream().allMatch(ks -> ks.state().isRunningOrRebalancing());
    }

    /**
     * Closes every process and releases every resource.
     *
     * <p>Each release runs independently, so one failure cannot strand the rest, and the
     * streams close is bounded in time. A wedged application may outlive this call, and
     * process exit reaps it.
     */
    @Override
    public void close() {
        for (KafkaStreams kafkaStreams : streams) {
            try {
                if (!kafkaStreams.close(java.time.Duration.ofSeconds(TIMEOUT_SECONDS))) {
                    LOG.warn("a streams application did not close within {}s; continuing with the remaining"
                            + " resources", TIMEOUT_SECONDS);
                }
            } catch (RuntimeException e) {
                LOG.warn("a streams application failed to close; continuing with the remaining resources", e);
            }
        }
        try {
            factsExecutor.shutdownNow();
        } catch (RuntimeException e) {
            LOG.warn("the facts executor failed to shut down; continuing", e);
        }
        for (AdminFactsSource factsSource : factsSources) {
            try {
                factsSource.close();
            } catch (RuntimeException e) {
                LOG.warn("a facts source failed to close; continuing", e);
            }
        }
        try {
            admin.close();
        } catch (RuntimeException e) {
            LOG.warn("the admin client failed to close", e);
        }
    }
}
