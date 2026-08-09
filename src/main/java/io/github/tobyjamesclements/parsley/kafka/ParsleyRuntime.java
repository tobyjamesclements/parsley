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
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * Owns the substrate wiring for a declared application: one Kafka Streams application per declared process — its own
 * consumer group, so any arrangement of processes and channels is supported, including several processes receiving
 * the same channel (SPEC Structural 2). Owning the {@link KafkaStreams} lifecycle is what makes the guarantees
 * non-overridable: exactly-once processing, read_committed isolation, and no offset auto-reset ever leave this class
 * (SPEC Substrate 3, Safety 8, Structural 9).
 */
public final class ParsleyRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ParsleyRuntime.class);
    private static final long TIMEOUT_SECONDS = 30;

    private final Admin admin;
    private final Map<String, KafkaStreams> streamsByProcess = new LinkedHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, Throwable> failuresByProcess =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final List<KafkaStreams> streams = new ArrayList<>();
    private final List<AdminFactsSource> factsSources = new ArrayList<>();
    /** One background thread gathers position facts for every process, off the stream threads (D54): the facts
     * source serialises rounds anyway, and a slow round must cost liveness latency, not poll-interval headroom. */
    private final java.util.concurrent.ExecutorService factsExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "parsley-facts");
                thread.setDaemon(true);
                return thread;
            });

    private ParsleyRuntime(Admin admin) {
        this.admin = admin;
    }

    public static ParsleyRuntime start(ParsleyConfig config, List<ProcessDefinition> definitions) {
        validateDistinctNames(definitions);
        refuseReservedTopicNames(config, definitions);
        Map<String, Object> adminProps = new HashMap<>(config.extraProperties());
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        Admin admin = Admin.create(adminProps);
        ParsleyRuntime runtime = new ParsleyRuntime(admin);
        try {
            Map<String, TopicInfo> topics = runtime.resolveTopics(declaredTopics(definitions));
            // One origin for every facts clock: readings are differenced against it in raw nanos before scaling
            // to millis, because nanoTime values are comparable only by difference — dividing absolute readings
            // would break the debounce and eviction comparisons at the wrap.
            long factsClockOrigin = System.nanoTime();
            for (ProcessDefinition definition : definitions) {
                String applicationId = config.applicationIdPrefix() + "-" + definition.name();
                java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> changelog =
                        runtime.describeChangelog(applicationId);
                boolean priorState = changelog.isPresent();
                // The stranded-holds scan runs first because its identity check must precede the width
                // comparison: a topic recreated with a different partition count is an identity change, and must
                // be diagnosed as one (ASSESSMENT 1.3) — a width refusal would prescribe restoring a partition
                // count that cannot bring the old channel back.
                runtime.refuseStrandedHeldMessages(applicationId, definition, topics, priorState, adminProps);
                runtime.refuseWidthChange(applicationId, definition, topics, changelog);
                runtime.commitInitialPositions(applicationId, definition, topics, priorState, adminProps);
                Map<UUID, String> namesById = new HashMap<>();
                topics.forEach((name, info) -> namesById.put(info.topicId(), name));
                // The debounce clock is monotonic: a stepped wall clock (NTP) must never collapse the
                // dead-confirmation window into a premature, persisted verdict. The window also has an absolute
                // floor: it debounces broker metadata propagation, whose latency does not shrink with the facts
                // interval, so a small (or sub-millisecond) interval must not collapse it toward zero.
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

    /** The process failed; record why, and give known foreign failures a parsley diagnosis (SPEC Operational 6).
     * A mid-run partition-count change surfaces as the consumer's NoOffsetForPartitionException (a new partition
     * was never pre-committed) or the assignor's invalid-partitions error — neither names a parsley concept, so
     * the remedy is stated here (D59): a full restart re-resolves and pre-commits new partitions, or refuses with
     * TASK_WIDTH_CHANGED where the width moved. */
    private void recordFailure(String process, Throwable exception) {
        // A deliberate refusal is the diagnosis worth keeping: with several stream threads (or racing failure
        // paths) a foreign secondary failure can land first, and first-wins would shadow the refusal an operator
        // needs (SPEC Operational 6). Refusals outrank foreign failures; the first refusal wins among refusals.
        failuresByProcess.merge(process, exception, (existing, latest) ->
                ParsleyFailClosedException.findIn(existing) == null && ParsleyFailClosedException.findIn(latest) != null ? latest : existing);
        // Depth-capped for the same reason findIn is: cause chains can legally be cyclic, and this runs inside
        // the uncaught-exception handler, where an unbounded walk would wedge the shutdown response.
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

    /** SPEC Operational 1: per-process state and stop reason, distinguishable as deliberate or transient. */
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

    /** Declared topic names must stay clear of the runtime's own namespace (SPEC Structural 5's spirit for
     * topics): a declared topic colliding with an induced internal name would corrupt the ordering state's
     * transport (D58). */
    private static void refuseReservedTopicNames(ParsleyConfig config, List<ProcessDefinition> definitions) {
        Set<String> internal = new HashSet<>();
        for (ProcessDefinition definition : definitions) {
            String applicationId = config.applicationIdPrefix() + "-" + definition.name();
            internal.add(changelogName(applicationId, ProcessTopology.ORDERING_STORE));
            definition.stores().forEach(store -> internal.add(changelogName(applicationId, store.name())));
        }
        for (String topic : declaredTopics(definitions)) {
            if (topic.contains("__parsley.") || internal.contains(topic)) {
                throw new IllegalArgumentException("topic '" + topic + "' collides with parsley's internal"
                        + " namespace; choose another name");
            }
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
                    // Brokers without topic IDs report the zero UUID; adopting it would conflate every channel's
                    // identity (SPEC Assumption 2). Such brokers are below the mandated 3.7.0 floor.
                    throw new ParsleyFailClosedException(
                            ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                            "topic '" + name + "' has no topic ID; brokers below the supported 3.7.0 floor cannot"
                                    + " provide channel identity (SPEC Substrate 1, Assumption 2) — refusing to start");
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

    /** The Kafka Streams changelog topic induced by a store of this application — the single owner of the naming
     * rule every guard in this class inspects (it must match Streams' actual internal-topic naming). */
    private static String changelogName(String applicationId, String storeName) {
        return applicationId + "-" + storeName + "-changelog";
    }

    /** This process's ordering-store changelog, when it exists: evidence of prior executions, and the stored task
     * width, from one describe. */
    private java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> describeChangelog(String applicationId) {
        String changelog = changelogName(applicationId, ProcessTopology.ORDERING_STORE);
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

    /**
     * The ordering store's changelog is created with one partition per task and Kafka can never change that count,
     * while the task count follows the declaration's widest topic. A width-changing restart therefore cannot run:
     * Kafka Streams' internal-topic validation (an unspecified behaviour this guard names and pins — see D49)
     * kills the application mid-rebalance with advice to run StreamsResetter, a remedy that would destroy the
     * ordering store — the state Structural 16 exists to protect (ASSESSMENT 1.6). Refuse at start instead, with
     * the accurate condition and remedy. Refusing every width change also keeps the stranded-holds scan sound: the
     * application never runs at a shrunken width against prior state, so entries committed after the scan's
     * snapshot are always caught by the next start's fresh scan.
     */
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

    /**
     * SPEC Structural 16: an execution whose declaration removes a channel on which received messages remain
     * undelivered must be refused. The engine refuses at task initialisation, but a removal can shrink the task set
     * so far that the holding task is never instantiated — so the runtime reads the ordering store's changelog and
     * refuses here when a live held entry names a channel outside the new declaration.
     */
    private void refuseStrandedHeldMessages(String applicationId, ProcessDefinition definition,
                                            Map<String, TopicInfo> topics, boolean priorState,
                                            Map<String, Object> clientProps) {
        if (!priorState) {
            return;
        }
        String changelog = changelogName(applicationId, ProcessTopology.ORDERING_STORE);
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
            // Bounded, with a diagnosis (SPEC Operational 2): a broker becoming unreachable mid-scan must not
            // block startup indefinitely. Progress resets the clock, so a large backlog is fine; stalls are not.
            long stallDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (parts.stream().anyMatch(tp -> consumer.position(tp) < ends.get(tp))) {
                var polled = consumer.poll(java.time.Duration.ofMillis(500));
                if (polled.isEmpty()) {
                    if (System.nanoTime() - stallDeadline > 0) { // by difference: nanoTime may wrap
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
        // A declared name whose recorded identity no longer matches the current resolution means the topic was
        // deleted and recreated under the name (D33): its group offsets and held entries belong to a dead channel.
        // Diagnosing this *before* the stranded-holds comparison matters — the held entries carry the old identity,
        // and reading their channel ids against the new resolution would misreport an identity change as a
        // declaration change, prescribing the wrong remedy (ASSESSMENT 1.3).
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

    /**
     * For received topic-partitions the group has never committed, commit an initial position while the group is
     * empty. Together with {@code auto.offset.reset=none} this pins down Safety 8: the consumer never silently
     * repositions, and the first receipt baseline is explicit (SPEC Structural 12). The *declared* initial position
     * applies only to a genuinely first start: when prior state exists, a missing offset means group-offset expiry,
     * and the only safe restart is earliest — re-fed already-delivered messages are dropped by the engine's session
     * floor, while LATEST would silently skip retained unread messages.
     */
    private void commitInitialPositions(String applicationId, ProcessDefinition definition,
                                        Map<String, TopicInfo> topics, boolean priorState,
                                        Map<String, Object> clientProps) {
        // Fast path, read-only: when every received partition already has a committed offset there is nothing to
        // write, so no group membership is needed — a closed Streams application's members linger in the group
        // until their session times out (Streams does not leave on close), and joining beside them would fail on
        // the assignor protocol. This admin read gates nothing but the join; the authoritative read happens again
        // inside the membership below.
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
        // The read-compute-commit sequence runs inside one group membership (ASSESSMENT 1.5): an admin alter
        // succeeds against any empty group, so a bootstrap paused for an arbitrary duration could overwrite a
        // newer lifetime's offsets. Membership commits are generation-fenced by the broker — a pause long enough
        // for another lifetime to interleave gets this member fenced out, and the stale commit throws instead of
        // landing. Every future pre-commit site must inherit this mechanism (GroupMembershipCommitter).
        try (GroupMembershipCommitter committer = new GroupMembershipCommitter(clientProps, applicationId)) {
            // The deadline must outlast a closed lifetime's lingering Streams members, who hold the group for
            // their session timeout — 45 s by consumer default, but legally raised through extraProperties, so
            // it is derived from the effective value rather than hardcoding the default: twice the session
            // timeout, so a quick restart with missing offsets waits the lingerers out instead of
            // deterministically timing out.
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

    /** Every partition of every received topic, as resolved at start. */
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

    /** The session timeout the application's Streams consumers effectively use: the most specific of the Streams
     * property spellings present in the configuration, or the consumer default. */
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
        // SPEC Substrate 3: exactly-once, read_committed; ParsleyConfig refuses user overrides of either.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // SPEC Safety 8: a read position outside the retained range must kill the task, never silently reset.
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "none");
        if (config.stateDir() != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
        }
        return props;
    }

    /** True while every process's application is running and none has recorded a failure. A stream thread's death
     * is recorded before the client's state machine winds down, so a process cannot report healthy while its
     * threads are already gone (the rebalance-limbo lie of ASSESSMENT 1.10). */
    public boolean healthy() {
        return failuresByProcess.isEmpty() && streams.stream().allMatch(ks -> ks.state().isRunningOrRebalancing());
    }

    @Override
    public void close() {
        // Failure to release one resource must not prevent release of the others (SPEC Operational 3) — and
        // start()'s failure path calls this same method, so a failed startup must not leak what it was releasing.
        for (KafkaStreams kafkaStreams : streams) {
            try {
                // Bounded (SPEC Operational 3): an application wedged in shutdown — observed when a stream thread
                // died failing closed while its topics were being deleted — must not hold the remaining resources
                // hostage; an unbounded close is itself a failure to release.
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
