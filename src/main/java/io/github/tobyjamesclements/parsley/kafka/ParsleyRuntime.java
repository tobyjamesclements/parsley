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
    /**
     * The metadata stamp on every offset the bootstrap commits. Kafka Streams overwrites
     * offset metadata with its own stamp on every commit, so with no prior state any
     * committed offset carrying anything other than this stamp proves the offset was not
     * left by a crashed bootstrap — a prior execution's, or external tooling's — and the
     * ordering state that must have accompanied it is gone. Keying the refusal on our own
     * stamp rather than on Streams' stamp being non-empty fails closed by construction if
     * a future Streams version ever commits empty metadata.
     */
    static final String BOOTSTRAP_OFFSET_STAMP = "parsley.bootstrap";
    /**
     * The evidence standard for concluding a topic gone (D84, D113): this many consistent
     * unknown-topic answers, each {@link #CORROBORATION_BACKOFF} after the last. One
     * spelling for the declared topics, the ordering changelog and the identity check at
     * task initialisation.
     */
    static final int CORROBORATING_ANSWERS = 3;
    static final java.time.Duration CORROBORATION_BACKOFF = java.time.Duration.ofMillis(500);

    private final Admin admin;
    // Populated by start() and read by status()/healthy()/close() from monitoring threads,
    // so these are concurrent like failuresByProcess; insertion order is preserved for
    // status reporting.
    private final Map<String, KafkaStreams> streamsByProcess =
            java.util.Collections.synchronizedMap(new LinkedHashMap<>());
    private final java.util.concurrent.ConcurrentHashMap<String, Throwable> failuresByProcess =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final List<KafkaStreams> streams = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.ConcurrentHashMap<String, ProcessDiagnostics> diagnosticsByProcess =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** Counted down when any process stops or this runtime closes (D111). */
    private final java.util.concurrent.CountDownLatch stopped = new java.util.concurrent.CountDownLatch(1);

    // Package-private for RecordFailureDiagnosticsTest, which drives recordFailure and
    // reads the merge's outcome directly — the failure path never touches the admin
    // client, so the test passes none. Production construction stays inside start().
    ParsleyRuntime(Admin admin) {
        this.admin = admin;
    }

    /**
     * Resolves topics, builds a topology per process, and starts each one.
     *
     * <p>Returns once every process's Kafka Streams application has been started; the
     * host then rebalances and initialises tasks on its own threads, so a refusal raised
     * inside task initialisation — restored state that cannot be trusted, a channel gone
     * while messages remain held — surfaces through {@link #status()} rather than from
     * this call, which throws only for what the bootstrap itself can see.
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

            for (ProcessDefinition definition : definitions) {
                String applicationId = config.applicationIdPrefix() + "-" + definition.name();
                java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> changelog =
                        runtime.describeChangelog(applicationId);
                ChangelogView orderingView = changelog.isPresent()
                        ? runtime.readOrderingChangelog(applicationId, adminProps,
                                changelog.get().partitions().size())
                        : ChangelogView.ABSENT;
                Map<byte[], byte[]> orderingState = orderingView.latest();
                // Prior state means committed ordering records, not the topic's mere
                // existence: a task's first committed step wrote the store's version entry,
                // which compaction retains, so any committed execution leaves records — a
                // changelog emptied of them (deleteRecords, a cleanup-policy excursion) is
                // the same loss shape as a deleted one and must run the same refusal
                // (D84, per partition since D88). The width refusal still keys on the
                // topic, whose partition count outlives its records.
                boolean priorState = !orderingState.isEmpty();
                runtime.refuseStrandedHeldMessages(applicationId, definition, topics, priorState, orderingState);
                runtime.refuseWidthChange(applicationId, definition, topics, changelog);
                Map<TopicPartition, Long> startPositions = runtime.commitInitialPositions(applicationId,
                        definition, topics, priorState, orderingView, adminProps);
                Map<UUID, String> namesById = new HashMap<>();
                topics.forEach((name, info) -> namesById.put(info.topicId(), name));

                // The declared names are what let a task's initialisation tell a deleted
                // received topic from a denied describe (D75); names of upstream topics are
                // learned as tasks initialise (D115).
                AdminTopicIdentitySource identitySource = new AdminTopicIdentitySource(admin, applicationId,
                        namesById, CORROBORATION_BACKOFF);
                ProcessDiagnostics diagnostics = new ProcessDiagnostics();
                runtime.diagnosticsByProcess.put(definition.name(), diagnostics);
                KafkaStreams kafkaStreams = new KafkaStreams(
                        ProcessTopology.build(definition, topics, identitySource, startPositions,
                                config.statusInterval(), config.metadataBudgetBytes(), diagnostics),
                        streamsProperties(config, applicationId));
                java.time.Duration memberBound = bootstrapMemberSessionTimeout(clientPropsFor(config));
                // A refused join is replaced only while another instance's bootstrap member can
                // still be lingering: twice its session timeout from here covers the pre-start
                // wait below and one ungraceful exit. Past that, a member speaking another
                // protocol under this application id is persistent, and the client stops with
                // that diagnosis rather than replacing its thread forever (D108).
                long collisionDeadline = System.nanoTime() + 2 * memberBound.toNanos();
                kafkaStreams.setUncaughtExceptionHandler(exception -> {
                    if (shouldReplaceThread(exception, System.nanoTime(), collisionDeadline)) {
                        // Another instance's bootstrap member is still in the group under the
                        // consumer protocol, so this thread's join was refused. That member
                        // leaves within milliseconds of committing; a replacement thread joins
                        // after it, and nothing this thread did needs undoing — it never held a
                        // task (D108).
                        LOG.warn("process {}: the group join met another instance's bootstrap member;"
                                + " replacing the stream thread to join again", definition.name());
                        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
                    }
                    runtime.recordFailure(definition.name(), isBootstrapMemberCollision(exception)
                            ? persistentProtocolConflict(applicationId, memberBound.multipliedBy(2), exception)
                            : exception);
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
                });
                kafkaStreams.setStateListener((newState, oldState) -> {
                    if (newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.NOT_RUNNING) {
                        runtime.stopped.countDown();
                    }
                });
                runtime.streams.add(kafkaStreams);
                runtime.streamsByProcess.put(definition.name(), kafkaStreams);
                runtime.awaitBootstrapMembersGone(applicationId, memberBound);
            }
            runtime.streams.forEach(KafkaStreams::start);
            return runtime;
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
    }

    private static Map<String, Object> clientPropsFor(ParsleyConfig config) {
        Map<String, Object> props = new HashMap<>(config.extraProperties());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        return props;
    }

    /**
     * Whether a stream thread's failure is the group-protocol collision a concurrent
     * bootstrap of another instance provokes (D48's residual S1, closed by D108): the
     * consumer refuses to join a group whose members speak another protocol, and treats
     * that as fatal.
     */
    static boolean isBootstrapMemberCollision(Throwable exception) {
        Throwable cause = exception;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.common.errors.InconsistentGroupProtocolException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a stream thread's failure is a collision worth replacing the thread for: the
     * protocol conflict of {@link #isBootstrapMemberCollision}, seen before the deadline by
     * which every other instance's bootstrap member must have left (D108). A conflict past
     * the deadline is persistent, and the client stops with its diagnosis instead.
     */
    static boolean shouldReplaceThread(Throwable exception, long nowNanos, long deadlineNanos) {
        return isBootstrapMemberCollision(exception) && nowNanos - deadlineNanos < 0;
    }

    /**
     * The diagnosis for a group join still refused as a protocol conflict once no bootstrap
     * member of another instance can remain: a member speaking another group protocol sits
     * in the group under this application id — a foreign consumer configured with it, or a
     * bootstrap member of an instance that never left — and the substrate, not the process,
     * must be corrected (D108).
     */
    static ParsleyFailClosedException persistentProtocolConflict(String applicationId, java.time.Duration window,
                                                                 Throwable cause) {
        return new ParsleyFailClosedException(ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                applicationId + ": the group join has been refused as a protocol conflict for longer than " + window
                        + ", so a member speaking another group protocol persists in this group: a consumer"
                        + " configured with this application id as its group, or another instance's bootstrap"
                        + " member that never left. Remove it, then restart this instance.", cause);
    }

    /**
     * The session timeout the bootstrap member joins with (D48): the configured consumer
     * session timeout in any Streams spelling, else the committer's ten-second default. An
     * ungraceful bootstrap exit holds the group for exactly this long, which is what bounds
     * both the pre-start wait and the window in which a refused join is replaced (D108).
     */
    static java.time.Duration bootstrapMemberSessionTimeout(Map<String, Object> clientProps) {
        java.util.OptionalLong configured = GroupMembershipCommitter.configuredSessionTimeoutMillis(clientProps);
        return configured.isEmpty()
                ? java.time.Duration.ofSeconds(10)
                : java.time.Duration.ofMillis(configured.getAsLong());
    }

    /**
     * Waits, bounded, until no bootstrap member of another instance sits in the group
     * before this instance's Kafka Streams joins it (D108). Two instances cold-starting
     * together each join as a bootstrap member to commit initial positions; a Streams join
     * arriving while the other's member is still present is refused as a protocol
     * conflict. Members leave within milliseconds of committing, so the wait is usually
     * nothing; an ungraceful exit holds its membership for the session timeout, which
     * bounds this wait, after which the join proceeds and a refused thread is replaced.
     */
    private void awaitBootstrapMembersGone(String applicationId, java.time.Duration bound) {
        boolean gone = awaitMembersGone(
                () -> admin.describeConsumerGroups(List.of(applicationId)).describedGroups()
                        .get(applicationId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).members().stream()
                        .anyMatch(member -> member.clientId().startsWith(GroupMembershipCommitter.CLIENT_ID_PREFIX)),
                System.nanoTime() + bound.toNanos(),
                System::nanoTime,
                millis -> {
                    try {
                        Thread.sleep(millis);
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                });
        if (!gone) {
            LOG.warn("{}: another instance's bootstrap member is still in the group after {}; starting"
                    + " anyway, a refused join replaces its thread", applicationId, bound);
        }
    }

    /**
     * The wait of {@link #awaitBootstrapMembersGone} over its seams: polls {@code memberPresent}
     * every hundred milliseconds until it answers false, and returns true; returns false once
     * {@code nanoTime} passes {@code deadlineNanos} with the member still present. A describe
     * that fails is not evidence either way, and an interrupted sleep ends the wait; both
     * return true, since the join itself is guarded by the thread replacement.
     */
    static boolean awaitMembersGone(java.util.concurrent.Callable<Boolean> memberPresent, long deadlineNanos,
                                    java.util.function.LongSupplier nanoTime,
                                    java.util.function.LongPredicate sleepMillis) {
        while (true) {
            boolean present;
            try {
                present = memberPresent.call();
            } catch (Exception e) {
                return true;
            }
            if (!present) {
                return true;
            }
            if (nanoTime.getAsLong() - deadlineNanos > 0) {
                return false;
            }
            if (!sleepMillis.test(100)) {
                return true;
            }
        }
    }

    /**
     * The named condition a stream thread's uncaught failure evidences, driving the
     * diagnosis {@link #recordFailure} logs (D59/D81/D87).
     */
    enum FailureDiagnosis {
        /** Retention discarded committed read positions before they were read (SPEC Safety 8). */
        POSITIONS_DISCARDED_UNREAD,
        /** A received partition has no committed read position (D81 splits the causes). */
        NO_COMMITTED_POSITION,
        /** A record exceeded a size limit, typically the changelog's max.message.bytes (D87). */
        RECORD_TOO_LARGE,
        /** The partition shape of the process's topics changed while it ran (D59). */
        PARTITION_SHAPE_CHANGED,
        /** A received topic was missing when the host rebalanced: deleted, or deleted and recreated (D115). */
        SOURCE_TOPIC_MISSING,
        /** No named condition; the failure is logged as-is. */
        UNRECOGNISED
    }

    /**
     * Walks a failure's cause chain outward-in and names the first recognised condition.
     *
     * <p>The walk is bounded exactly as {@link ParsleyFailClosedException#findIn}, to guard
     * against a cyclic chain. Within each link the instanceof checks precede the message
     * probe, and the first match anywhere wins: an outer link's condition is named even
     * when a deeper link would match a different branch.
     */
    static FailureDiagnosis classifyFailure(Throwable exception) {
        Throwable cause = exception;
        for (int depth = 0; cause != null && depth < 64; depth++, cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.clients.consumer.OffsetOutOfRangeException) {
                return FailureDiagnosis.POSITIONS_DISCARDED_UNREAD;
            }
            if (cause instanceof org.apache.kafka.clients.consumer.NoOffsetForPartitionException) {
                return FailureDiagnosis.NO_COMMITTED_POSITION;
            }
            if (cause instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
                return FailureDiagnosis.RECORD_TOO_LARGE;
            }
            if (cause instanceof org.apache.kafka.streams.errors.MissingSourceTopicException) {
                return FailureDiagnosis.SOURCE_TOPIC_MISSING;
            }
            if (String.valueOf(cause.getMessage()).contains("invalid partitions")) {
                return FailureDiagnosis.PARTITION_SHAPE_CHANGED;
            }
        }
        return FailureDiagnosis.UNRECOGNISED;
    }

    /**
     * The failure {@code status()} keeps when a process fails more than once: the first
     * recorded failure stands, unless it lacks a fail-closed diagnosis a later failure
     * carries — the refusal is what {@code status()} unwraps for the operator (D55), so a
     * follow-on transient must never bury it, and the first refusal is never displaced by
     * a second.
     */
    static Throwable preferFailClosedDiagnosis(Throwable existing, Throwable latest) {
        return ParsleyFailClosedException.findIn(existing) == null
                && ParsleyFailClosedException.findIn(latest) != null ? latest : existing;
    }

    // Package-private so RecordFailureDiagnosticsTest can pin the merge wiring and the
    // per-diagnosis log lines directly; production reaches it only through the uncaught
    // exception handler start() installs.
    void recordFailure(String process, Throwable exception) {
        // A stop the substrate detected but that recurs identically on restart carries its
        // reason into status() like an engine refusal (D109, Operational 1): a supervisor
        // keyed on refusalReason must not read it as a transient and restart forever.
        FailureDiagnosis diagnosis = classifyFailure(exception);
        Throwable recorded = switch (diagnosis) {
            case POSITIONS_DISCARDED_UNREAD -> new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD,
                    "process " + process + ": the broker no longer retains this process's committed read"
                            + " position; positions were discarded before they were read (SPEC Safety 8)."
                            + " Reset the process's state and group offsets deliberately to proceed.",
                    exception);
            case RECORD_TOO_LARGE -> new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                    "process " + process + ": a record exceeded a size limit, typically a held message's"
                            + " persisted form against the ordering changelog's max.message.bytes; raise that"
                            + " limit and, if needed, producer.max.request.size, then restart.",
                    exception);
            default -> exception;
        };
        failuresByProcess.merge(process, recorded, ParsleyRuntime::preferFailClosedDiagnosis);
        stopped.countDown();

        switch (diagnosis) {
            case POSITIONS_DISCARDED_UNREAD ->
                LOG.error("process {}: the broker no longer retains this process's committed read position;"
                        + " positions were discarded before they were read, and auto.offset.reset=none stops"
                        + " the process rather than skipping the gap (SPEC Safety 8, the"
                        + " POSITIONS_DISCARDED_UNREAD condition). Reset the process's state and group offsets"
                        + " deliberately to proceed (failing closed)", process, exception);
            case NO_COMMITTED_POSITION ->
                LOG.error("process {}: a received partition has no committed read position; either a partition"
                        + " was added while the process ran, or the group's committed offsets were removed"
                        + " mid-run. Restart the application: a width-preserving expansion is re-resolved and"
                        + " pre-committed; a width change refuses with TASK_WIDTH_CHANGED and its remedy"
                        + " (failing closed)", process, exception);
            case RECORD_TOO_LARGE ->
                LOG.error("process {}: a record exceeded a size limit. When the failing topic is this"
                        + " process's ordering changelog, a held message's persisted form — payload, headers"
                        + " and causal metadata together — outgrew the changelog topic's max.message.bytes,"
                        + " which the metadata budget alone does not bound. Raise max.message.bytes on the"
                        + " changelog topic and, if needed, producer.max.request.size via streamsProperty,"
                        + " then restart to deliver the held message (failing closed)", process, exception);
            case PARTITION_SHAPE_CHANGED ->
                LOG.error("process {}: the partition shape of its topics changed while it ran; parsley resolves"
                        + " partitions at start(). Restart the application: a width-preserving expansion is"
                        + " re-resolved and pre-committed; a width change refuses with TASK_WIDTH_CHANGED and its"
                        + " remedy (failing closed)", process, exception);
            case SOURCE_TOPIC_MISSING ->
                LOG.error("process {}: a received topic was missing when the host rebalanced: it was deleted,"
                        + " or deleted and recreated under its name, while the process ran (SPEC Assumption 17)."
                        + " Restart the application: a topic still missing refuses the start until it is"
                        + " restored or removed from the declaration; one recreated under its name refuses with"
                        + " CHANNEL_IDENTITY_CHANGED and its remedy; one that merely lagged in a broker's"
                        + " metadata resumes (failing closed)", process, exception);
            case UNRECOGNISED ->
                LOG.error("process {} failed; shutting its application down (failing closed)", process, exception);
        }
    }

    /**
     * The failure {@link #recordFailure}'s merge retained for {@code process} — the read
     * side of the merge wiring, for tests: {@code status()} surfaces failures only for
     * processes that also have a streams instance, which a direct recordFailure pin does
     * not build.
     */
    Throwable recordedFailure(String process) {
        return failuresByProcess.get(process);
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
            ProcessDiagnostics diagnostics = diagnosticsByProcess.get(process);
            statuses.put(process, new io.github.tobyjamesclements.parsley.api.ProcessStatus(process, mapped,
                    java.util.Optional.ofNullable(refusal).map(ParsleyFailClosedException::reason),
                    java.util.Optional.ofNullable(failure).map(Throwable::getMessage),
                    diagnostics == null ? List.of() : diagnostics.snapshot()));
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
        // Reserved-namespace containment is not re-checked here: every declared topic came
        // through Channel's constructor, which refuses it, so a runtime re-check would be
        // unreachable and unpinnable. Only the composed-name collision can arise at start.
        for (String topic : declaredTopics(definitions)) {
            if (ownerByChangelog.containsKey(topic)) {
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

    /**
     * Maps one resolved description to its {@link TopicInfo}, refusing a substrate that
     * cannot provide channel identity.
     *
     * <p>The substrate reserves {@link org.apache.kafka.common.Uuid#ZERO_UUID} and never
     * assigns it, so a description carrying it means the broker predates topic IDs — below
     * the supported 3.7.0 floor, channel identity does not exist (SPEC Substrate 1,
     * Assumption 2), and D83's whole identity machinery relies on this refusal keeping the
     * zero id out of every resolved view.
     */
    static TopicInfo requireTopicId(String name, TopicDescription description) {
        if (org.apache.kafka.common.Uuid.ZERO_UUID.equals(description.topicId())) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                    "topic '" + name + "' has no topic ID; brokers below the supported 3.7.0 floor cannot"
                            + " provide channel identity (SPEC Substrate 1, Assumption 2); refusing to start");
        }
        return new TopicInfo(
                TopicInfo.toJavaUuid(description.topicId()), description.partitions().size());
    }

    private Map<String, TopicInfo> resolveTopics(Set<String> names) {
        return resolveTopicsCorroborated(
                () -> admin.describeTopics(names).allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                CORROBORATION_BACKOFF);
    }

    /** The describe of every declared topic, behind a seam so tests can script the answers. */
    @FunctionalInterface
    interface TopicsDescribe {
        Map<String, TopicDescription> describe() throws Exception;
    }

    /**
     * Resolves the declared topics, concluding that one does not exist only from three
     * consistent unknown-topic answers half a second apart (D113): a describe is served from
     * one broker's metadata view, which can lag a topic created moments before the start,
     * and a start that trusted a single stale answer refused a topic that existed. Any other
     * failure refuses at once, since nothing about it is a matter of corroboration. The same
     * evidence standard D84 applies to the ordering changelog's describe.
     */
    static Map<String, TopicInfo> resolveTopicsCorroborated(TopicsDescribe describe, java.time.Duration backoff) {
        for (int attempt = 0; ; attempt++) {
            try {
                Map<String, TopicInfo> topics = new LinkedHashMap<>();
                describe.describe().forEach((name, description) -> topics.put(name, requireTopicId(name, description)));
                return topics;
            } catch (ParsleyFailClosedException e) {
                throw e;
            } catch (Exception e) {
                boolean unknown = e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
                if (!unknown || attempt == CORROBORATING_ANSWERS - 1) {
                    throw new IllegalStateException("declared topics could not be resolved; refusing to start", e);
                }
                try {
                    Thread.sleep(backoff.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            "interrupted while resolving the declared topics; refusing to start", interrupted);
                }
            }
        }
    }

    /**
     * Describes the ordering changelog, concluding absence only from corroborated answers.
     *
     * <p>One unknown-topic answer is not proof of absence: a describe is served from a
     * single broker's metadata view, which can lag a recent creation, and a start that
     * trusted one stale answer would misdiagnose a healthy sibling's state as
     * ORDERING_STATE_LOST — with a remedy that deletes that sibling's offsets. Absence is
     * concluded only after three consistent unknown answers spaced half a second apart,
     * the evidence standard every deletion verdict in this runtime takes (D44/D75, D84,
     * D115's identity check at task initialisation). A genuine first start pays the extra
     * describes once.
     */
    private java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> describeChangelog(String applicationId) {
        String changelog = ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE);
        return describeChangelogCorroborated(applicationId, () -> admin.describeTopics(List.of(changelog))
                .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).get(changelog),
                CORROBORATION_BACKOFF);
    }

    /** One describe of the ordering changelog, as the substrate answers it — the seam the
     * corroboration loop retries through, so tests can script the answer sequence. */
    @FunctionalInterface
    interface ChangelogDescribe {
        TopicDescription describe() throws Exception;
    }

    /**
     * The corroboration loop behind {@link #describeChangelog}: absence is concluded only
     * from three consistent unknown-topic answers; any other failure refuses the start
     * rather than concluding anything (D84). Letting a transient generic failure — a
     * timeout, a broker outage — fall through to "absent" would resume a process with
     * prior state as a first start, the exact single-answer trust D84 removed.
     *
     * <p>Production passes the half-second spacing between answers (D84's evidence
     * standard); tests pass ~zero so the loop's decisions are pinned without paying the
     * real backoffs.
     */
    static java.util.Optional<org.apache.kafka.clients.admin.TopicDescription> describeChangelogCorroborated(
            String applicationId, ChangelogDescribe describe, java.time.Duration backoff) {
        for (int attempt = 0; ; attempt++) {
            try {
                return java.util.Optional.of(describe.describe());
            } catch (Exception e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                    throw new IllegalStateException(
                            applicationId + ": could not determine prior state; refusing to start", e);
                }
                if (attempt == CORROBORATING_ANSWERS - 1) {
                    return java.util.Optional.empty();
                }
                try {
                    Thread.sleep(backoff.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(
                            applicationId + ": interrupted while determining prior state; refusing to start",
                            interrupted);
                }
            }
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

    /**
     * A start-time contradiction another attempt resolves: refused loudly, safe to retry,
     * and never dressed as a terminal diagnosis whose remedy would destroy healthy state.
     */
    static final class RetryableStartException extends IllegalStateException {
        RetryableStartException(String message) {
            super(message);
        }
    }

    /**
     * What one end-to-end read of the ordering changelog saw: the latest value per key
     * across every partition, and which partitions held at least one record — the loss
     * shape is per changelog partition, since each task's state lives in its own (D88).
     */
    record ChangelogView(Map<byte[], byte[]> latest, java.util.Set<Integer> partitionsWithRecords) {
        static final ChangelogView ABSENT = new ChangelogView(Map.of(), java.util.Set.of());
    }

    /** Stands in for a held message's body in the bootstrap view: present, content unread. */
    static final byte[] HELD_PRESENCE = new byte[0];

    /**
     * Client properties for the bootstrap's ordering-changelog reader.
     *
     * <p>Two pins here carry refusals rather than tuning. {@code allow.auto.create.topics}
     * is false because the reader's metadata requests must never create the very changelog
     * whose record content start() keys prior state on: against a broker with auto-create
     * enabled, a deletion racing the start would otherwise be resurrected as an empty
     * impostor that passes every prior-state refusal (D82); with the pin, the reader's
     * metadata answer omits the topic and the scan's partition-count check refuses loudly
     * (D88 corrects D82's claimed mechanism — the unknown answer is immediate, not a
     * timeout). {@code auto.offset.reset} is none so a log start advancing mid-scan fails
     * the scan loudly instead of silently resetting to the end and truncating the
     * restored view those refusals read.
     */
    static Map<String, Object> changelogReaderProperties(Map<String, Object> clientProps) {
        Map<String, Object> props = new HashMap<>(clientProps);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        return props;
    }

    /**
     * The isolation the changelog read's end-offset snapshot asks for.
     *
     * <p>READ_UNCOMMITTED is the load-bearing choice, not a default left to chance: this
     * bound must be the log's true end, where the sibling listOffsets in
     * commitInitialPositions deliberately asks for the read-committed view. Stopping at
     * the last stable offset would silently hide committed tail records sitting above a
     * superseded execution's open transaction (D79).
     */
    static ListOffsetsOptions changelogEndOffsetIsolation() {
        return new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED);
    }

    /**
     * Drains an assigned, rewound consumer up to the end-offset snapshot in {@code ends},
     * compacting to the latest value per key and tracking which partitions held records.
     *
     * <p>No progress for {@code stallTimeout} fails the read loudly rather than hanging
     * the start (D79); a partition at its snapshot end is paused — the pause loop's
     * comment carries the rationale. Production passes the 30s deadline that outlasts
     * the default producer transaction timeout.
     */
    static ChangelogView readToEnds(org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer,
                                    String changelog, List<TopicPartition> parts,
                                    Map<TopicPartition, Long> ends, java.time.Duration stallTimeout) {
        Map<byte[], byte[]> latest = new java.util.TreeMap<>(java.util.Arrays::compareUnsigned);
        java.util.Set<Integer> partitionsWithRecords = new HashSet<>();
        long stallDeadline = System.nanoTime() + stallTimeout.toNanos();
        while (parts.stream().anyMatch(tp -> consumer.position(tp) < ends.get(tp))) {
            var polled = consumer.poll(java.time.Duration.ofMillis(500));
            if (polled.isEmpty()) {
                if (System.nanoTime() - stallDeadline > 0) {
                    throw new IllegalStateException("no progress reading " + changelog + " for "
                            + renderDeadline(stallTimeout) + " while reading prior ordering state");
                }
            } else {
                stallDeadline = System.nanoTime() + stallTimeout.toNanos();
                polled.forEach(record -> {
                    // A held message's body is never read here — the view answers which
                    // channels hold something, not what — so it is kept as a presence
                    // marker, and a tombstone still clears it (D110). Retaining every blob
                    // put the whole hold-back backlog on the heap at every start.
                    byte[] value = record.value();
                    latest.put(record.key(), value != null
                            && io.github.tobyjamesclements.parsley.core.OrderingStateInspector.isHeldKey(record.key())
                            ? HELD_PRESENCE : value);
                    partitionsWithRecords.add(record.partition());
                });
            }
            // A partition that reached its snapshot end stops feeding the loop:
            // records past the snapshot would otherwise keep resetting the stall
            // deadline forever while another partition sits pinned below its end,
            // turning the promised loud stall into an indefinite hang.
            for (TopicPartition tp : parts) {
                if (consumer.position(tp) >= ends.get(tp) && !consumer.paused().contains(tp)) {
                    consumer.pause(List.of(tp));
                }
            }
        }
        return new ChangelogView(latest, java.util.Set.copyOf(partitionsWithRecords));
    }

    /**
     * Renders a deadline for the stall diagnosis: whole seconds as "30s" (production's
     * value, byte-for-byte as before), anything finer as "40ms" — {@code toSeconds()}
     * alone printed a sub-second deadline as the meaningless "0s".
     */
    private static String renderDeadline(java.time.Duration deadline) {
        return deadline.toMillis() % 1000 == 0
                ? deadline.toSeconds() + "s"
                : deadline.toMillis() + "ms";
    }

    /**
     * Reads the process's ordering-store changelog end to end, compacted in memory to the
     * latest value per key, tracking which partitions held records.
     *
     * <p>The read targets the log's true end rather than the last stable offset: a
     * superseded execution's producer can leave a transaction open below records a
     * successor committed, and stopping at the stable offset would silently hide those
     * committed tail records from the checks this view feeds. An open transaction resolves
     * within the producer's transaction timeout, which the stall deadline outlasts at the
     * defaults; a transaction configured to outlive the deadline fails the start loudly
     * rather than truncating the view.
     *
     * <p>The reader's own metadata answer is corroborated against the describe this read
     * was keyed on: with auto-create pinned off, a broker whose metadata lags the
     * changelog's creation answers an empty partition list immediately — no retry, no
     * timeout — and trusting it would flip prior state off one stale view, the exact
     * single-answer trust D84 removed from the describe path. A partition-count mismatch
     * refuses as a retryable transient instead of scanning vacuously (D88).
     */
    private ChangelogView readOrderingChangelog(String applicationId, Map<String, Object> clientProps,
                                                int expectedPartitions) {
        String changelog = ProcessTopology.changelogName(applicationId, ProcessTopology.ORDERING_STORE);
        Map<String, Object> props = changelogReaderProperties(clientProps);
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props,
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer())) {
            List<TopicPartition> parts = consumer.partitionsFor(changelog).stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition())).toList();
            requireCorroboratedWidth(applicationId, expectedPartitions, parts.size());
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
            for (TopicPartition tp : parts) {
                latestSpecs.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, Long> ends = new HashMap<>();
            admin.listOffsets(latestSpecs, changelogEndOffsetIsolation()).all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .forEach((tp, info) -> ends.put(tp, info.offset()));

            return readToEnds(consumer, changelog, parts, ends,
                    java.time.Duration.ofSeconds(TIMEOUT_SECONDS));
        } catch (RetryableStartException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": prior ordering state could not be read; refusing to start", e);
        }
    }

    /**
     * Corroborates the changelog reader's own metadata answer against the describe the
     * read was keyed on, refusing a disagreement as a retryable transient.
     *
     * <p>With auto-create pinned off, a broker whose metadata lags the changelog's
     * creation answers an empty partition list immediately — no retry, no timeout — and
     * trusting it would flip prior state off one stale view, the exact single-answer
     * trust D84 removed from the describe path (D88). The refusal must stay retryable:
     * dressing it as a terminal diagnosis would hand the operator a destructive remedy
     * for a broker that merely needs a moment.
     */
    static void requireCorroboratedWidth(String applicationId, int describedPartitions, int readerAnswered) {
        if (readerAnswered != describedPartitions) {
            throw new RetryableStartException(applicationId + ": the ordering changelog is described"
                    + " with " + describedPartitions + " partition(s) but the reader's metadata answered "
                    + readerAnswered + "; a broker's metadata view is lagging. Retry this start.");
        }
    }

    private void refuseStrandedHeldMessages(String applicationId, ProcessDefinition definition,
                                            Map<String, TopicInfo> topics, boolean priorState,
                                            Map<byte[], byte[]> orderingState) {
        if (!priorState) {
            return;
        }
        Map<String, UUID> resolvedIds = new HashMap<>();
        definition.receivedTopics().forEach(topic -> resolvedIds.put(topic, topics.get(topic).topicId()));
        List<String> identityChanged = io.github.tobyjamesclements.parsley.core.OrderingStateInspector
                .identityChangedTopics(orderingState, resolvedIds);
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
                new java.util.TreeSet<>(io.github.tobyjamesclements.parsley.core.OrderingStateInspector
                        .heldChannels(orderingState));
        stranded.removeAll(declared);
        if (!stranded.isEmpty()) {
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES,
                    applicationId + ": received messages remain undelivered on " + stranded
                            + ", which the new declaration no longer receives");
        }
    }

    /**
     * Establishes the position Kafka Streams will feed each received partition from, and
     * returns it: the group's committed position where one exists, else the one the
     * bootstrap commits here.
     *
     * <p>With prior state, a partition whose committed position is missing — expired during
     * a long stop — resumes at the ordering state's covered position plus one (D115): every
     * position at or below the covered one was fed or will never arrive, so that is exactly
     * where the previous execution would have read next. A received partition the ordering
     * state names but never covered — received by an earlier execution that started it at
     * 0 and was never fed from it — resumes at 0, the only position it can show it read
     * from; the substrate's earliest, which may have moved past positions it never read, is
     * not a position it covered. Whether retention still holds the resumed position is the
     * substrate's to decide at the first fetch: under {@code auto.offset.reset=none} a
     * position below the log start refuses the fetch, and {@link #classifyFailure} names it
     * {@code POSITIONS_DISCARDED_UNREAD} (SPEC Safety 8, D9/D81/D109). A channel the state
     * has never named — a genuinely first start, a channel joining the received set, or a
     * bootstrap that crashed before Streams ever ran — takes the substrate's earliest or
     * latest position instead, a one-off query: the declared initial position on a first
     * start, earliest wherever prior state exists (D36).
     *
     * @return per received partition, the position the host feeds first
     */
    private Map<TopicPartition, Long> commitInitialPositions(String applicationId, ProcessDefinition definition,
                                                            Map<String, TopicInfo> topics, boolean priorState,
                                                            ChangelogView orderingView,
                                                            Map<String, Object> clientProps) {
        java.util.Set<TopicPartition> received = receivedPartitions(definition, topics);
        Map<TopicPartition, OffsetAndMetadata> preCheck = awaitStablePreCheck(applicationId, received,
                () -> listStableOffsets(applicationId),
                java.time.Duration.ofMillis(200), java.time.Duration.ofSeconds(5));
        ChangelogRecheck recheck = () -> describeChangelog(applicationId)
                .map(description -> readOrderingChangelog(applicationId, clientProps,
                        description.partitions().size()));
        refuseLostOrderingState(applicationId, orderingView, preCheck, recheck);
        if (preCheck.keySet().containsAll(received)) {
            return startPositions(received, preCheck, Map.of());
        }

        try (GroupMembershipCommitter committer = new GroupMembershipCommitter(clientProps, applicationId)) {
            committer.join(definition.receivedTopics(), streamsSessionTimeout(clientProps).multipliedBy(2));
            Map<TopicPartition, OffsetAndMetadata> committed = committer.committed(received);
            // Re-checked against the member's fetch: the admin listing above silently
            // omits any partition whose offset has a pending transactional commit
            // (partition-level UNSTABLE_OFFSET_COMMIT is skipped, not failed, by the
            // admin client), so a lost-state stamp could hide from the pre-check. The
            // member's committed() is a stable fetch that retries until the transaction
            // resolves, so what it returns is authoritative.
            refuseLostOrderingState(applicationId, orderingView, committed, recheck);
            Map<io.github.tobyjamesclements.parsley.core.ChannelId, Long> covered =
                    io.github.tobyjamesclements.parsley.core.OrderingStateInspector.coveredPositions(orderingView.latest());
            java.util.Set<String> receivedBefore =
                    io.github.tobyjamesclements.parsley.core.OrderingStateInspector.nameBindings(orderingView.latest())
                            .keySet();
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            Map<TopicPartition, OffsetSpec> wanted = new HashMap<>();
            for (TopicPartition tp : received) {
                if (committed.get(tp) != null) {
                    continue;
                }
                Long coveredUpTo = covered.get(new io.github.tobyjamesclements.parsley.core.ChannelId(
                        topics.get(tp.topic()).topicId(), tp.partition()));
                java.util.OptionalLong resume = resumePosition(coveredUpTo, receivedBefore.contains(tp.topic()));
                if (resume.isPresent()) {
                    toCommit.put(tp, new OffsetAndMetadata(resume.getAsLong(), BOOTSTRAP_OFFSET_STAMP));
                    continue;
                }
                Channel.InitialPosition initial = priorState
                        ? Channel.InitialPosition.EARLIEST
                        : definition.input(tp.topic()).channel().initialPosition();
                wanted.put(tp, initial == Channel.InitialPosition.EARLIEST
                        ? OffsetSpec.earliest() : OffsetSpec.latest());
            }
            if (!wanted.isEmpty()) {
                admin.listOffsets(wanted, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .forEach((tp, info) -> toCommit.put(tp,
                                new OffsetAndMetadata(info.offset(), BOOTSTRAP_OFFSET_STAMP)));
            }
            if (toCommit.isEmpty()) {
                return startPositions(received, committed, Map.of());
            }
            committer.commit(toCommit);
            LOG.info("{}: committed initial positions for {}", applicationId, toCommit.keySet());
            return startPositions(received, committed, toCommit);
        } catch (ParsleyFailClosedException | RetryableStartException e) {
            // The retryable transient keeps its own diagnosis: wrapping it in the terminal
            // "could not be established" shape would send the operator to a remedy the
            // next attempt makes destructive.
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": initial read positions could not be established; refusing to start", e);
        }
    }

    /**
     * The position a partition with an expired committed offset resumes at, from the
     * ordering state's coverage of it: the covered position plus one, the next position the
     * previous execution would have read. A partition the state names as received but never
     * covered — started at 0 and never fed, or a pre-D115 execution that recorded coverage
     * of -1 for it — resumes at 0, the one position it can show it read from. Empty where
     * the state never named the topic (the substrate's earliest or latest position is taken
     * instead) and where the coverage is the engine's fed-to-end sentinel, which a channel
     * settled on its topic's deletion carries and which no offset can follow.
     *
     * @param coveredUpTo    the channel's covered position from the ordering state, or null
     * @param receivedBefore whether the ordering state binds the channel's topic name, which
     *                       every execution that received the topic writes
     * @return the position to commit, or empty to query the substrate
     */
    static java.util.OptionalLong resumePosition(Long coveredUpTo, boolean receivedBefore) {
        if (coveredUpTo == null) {
            return receivedBefore ? java.util.OptionalLong.of(0) : java.util.OptionalLong.empty();
        }
        if (io.github.tobyjamesclements.parsley.core.OrderingStateInspector.isFedToEnd(coveredUpTo)) {
            return java.util.OptionalLong.empty();
        }
        return java.util.OptionalLong.of(Math.max(coveredUpTo, -1) + 1);
    }

    /** The received partitions' start positions: the committed ones, overlaid by this bootstrap's commits. */
    private static Map<TopicPartition, Long> startPositions(java.util.Set<TopicPartition> received,
                                                            Map<TopicPartition, OffsetAndMetadata> committed,
                                                            Map<TopicPartition, OffsetAndMetadata> justCommitted) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        for (TopicPartition tp : received) {
            OffsetAndMetadata offset = justCommitted.containsKey(tp) ? justCommitted.get(tp) : committed.get(tp);
            if (offset != null) {
                positions.put(tp, offset.offset());
            }
        }
        return positions;
    }

    /**
     * The second look {@link #refuseLostOrderingState} takes at the ordering changelog
     * immediately before refusing: empty when the changelog does not exist, the re-read
     * view when it does. A seam, so tests can script what the second look finds.
     */
    @FunctionalInterface
    interface ChangelogRecheck {
        java.util.Optional<ChangelogView> reread();
    }

    /**
     * Refuses a start whose group carries committed read positions the bootstrap did not
     * write, while the ordering-changelog partition behind them holds no records.
     *
     * <p>Every committed step writes ordering state and read positions atomically (SPEC
     * Host obligation 3), and a task's first committed step wrote the store's version
     * entry into its own changelog partition, which compaction retains — so a
     * non-bootstrap-stamped offset on partition p with no records in changelog partition
     * p means the state of task p's most recent committed step has been lost (Host
     * obligation 5): resuming would rebuild an empty engine and silently under-express
     * every cause delivered before the loss. The check is per partition (D88 tightens
     * D84): a one-partition record purge is the same loss for its task however healthy
     * the sibling partitions look, and the whole-topic shapes — absent, or emptied — fall
     * out as every partition failing. A first-start bootstrap that crashed after
     * committing initial positions leaves offsets with no records anywhere, but its
     * commits carry {@link #BOOTSTRAP_OFFSET_STAMP}, so bootstrap crash recovery still
     * starts. Every group offset is scanned, not only the declared partitions: a
     * declaration change alongside the state loss must not hide a formerly-received
     * partition's evidence.
     *
     * <p>Before refusing, the changelog is looked at again: the view was read before the
     * offsets were listed, and a pause of arbitrary duration lands between any two
     * statements (SPEC Fault model 2), so a concurrent lifetime of this process can have
     * created records — and committed — in the window. That shape refuses as a retryable
     * transient, not as state loss: a state-loss diagnosis here would tell the operator
     * to delete offsets a healthy sibling just wrote. The recheck is a seam so the
     * decision — which answer shapes refuse, and as what — is testable without a broker.
     */
    static void refuseLostOrderingState(String applicationId, ChangelogView view,
                                        Map<TopicPartition, OffsetAndMetadata> committed,
                                        ChangelogRecheck recheck) {
        for (var entry : committed.entrySet()) {
            OffsetAndMetadata offset = entry.getValue();
            if (offset == null || BOOTSTRAP_OFFSET_STAMP.equals(offset.metadata())) {
                continue;
            }
            int partition = entry.getKey().partition();
            if (view.partitionsWithRecords().contains(partition)) {
                continue;
            }
            java.util.Optional<ChangelogView> now = recheck.reread();
            if (now.isPresent() && now.get().partitionsWithRecords().contains(partition)) {
                throw new RetryableStartException(applicationId + ": ordering records appeared while"
                        + " this start was determining prior state; a concurrent lifetime of this process"
                        + " is starting or running. Retry this start.");
            }
            String shape = now.isPresent()
                    ? "partition " + partition + " of this process's ordering-store changelog holds no"
                            + " ordering records"
                    : "this process's ordering-store changelog does not exist";
            String provenance = offset.metadata().isEmpty()
                    ? "committed outside parsley (external tooling, or pre-seeded offsets)"
                    : "stamped by a previous Kafka Streams execution";
            throw new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.ORDERING_STATE_LOST,
                    applicationId + ": committed read positions exist for " + entry.getKey() + ", "
                            + provenance + ", but " + shape
                            + ". If a prior execution ran, the ordering state of its most recent"
                            + " committed step has been lost (SPEC Host obligation 5) and resuming would"
                            + " silently under-express causes delivered before the loss. Restore the"
                            + " changelog topic and its records, or reset (delete) the process's group"
                            + " offsets deliberately to start fresh.");
        }
    }

    /**
     * Lists the group's committed read positions, requiring stability.
     *
     * <p>requireStable: a pending transactional commit means another lifetime of this
     * process is live right now, and this listing must not act on an offset that lifetime
     * is about to replace.
     */
    private Map<TopicPartition, OffsetAndMetadata> listStableOffsets(String applicationId) {
        try {
            return admin.listConsumerGroupOffsets(applicationId,
                            new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions()
                                    .requireStable(true))
                    .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    applicationId + ": committed read positions could not be listed; refusing to start", e);
        }
    }

    /** One stable listing of the group's committed read positions — the seam every
     * pre-check listing goes through, first look and retries alike, so tests can
     * script the whole answer sequence. */
    @FunctionalInterface
    interface StableOffsetListing {
        Map<TopicPartition, OffsetAndMetadata> list();
    }

    /**
     * Retries a partially-covering stable listing briefly before adopting it.
     *
     * <p>A listing that covers some received partitions but not all is, over a healthy
     * group, usually not missing offsets at all: the stable listing silently omits any
     * partition whose offset has a pending transactional commit (partition-level
     * UNSTABLE_OFFSET_COMMIT is skipped, not failed, by the admin client), and a live
     * sibling under EOS commits every commit interval, so some partition is routinely
     * mid-commit at the listing instant. Concluding "missing" from that snapshot sends
     * the start into the group join, which can only grind against the sibling's
     * protocol until the join deadline and then refuse a legitimate scale-out.
     * Pending commits resolve within the transaction timeout, so a partial listing is
     * retried briefly; one that stays partial past {@code retryBudget} falls through to
     * the join, which remains authoritative (D86). A first start lists nothing for the
     * received set and skips the wait entirely.
     *
     * <p>Every listing — the first included — goes through the one {@code listing} seam,
     * so tests script the whole sequence a start sees. Production passes the 200ms
     * backoff and 5s budget that wait out one EOS commit interval; tests pass ~zero.
     */
    static Map<TopicPartition, OffsetAndMetadata> awaitStablePreCheck(String applicationId,
                                                                      java.util.Set<TopicPartition> received,
                                                                      StableOffsetListing listing,
                                                                      java.time.Duration backoff,
                                                                      java.time.Duration retryBudget) {
        Map<TopicPartition, OffsetAndMetadata> preCheck = listing.list();
        long retryDeadline = System.nanoTime() + retryBudget.toNanos();
        while (preCheckLooksUnstable(received, preCheck.keySet()) && System.nanoTime() - retryDeadline < 0) {
            try {
                Thread.sleep(backoff.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        applicationId + ": interrupted while listing read positions; refusing to start", e);
            }
            preCheck = listing.list();
        }
        return preCheck;
    }

    /**
     * Whether a stable offset listing should be retried before concluding offsets are
     * missing.
     *
     * <p>Partial coverage of the received set is the shape a pending transactional commit
     * produces — the stable listing skips, rather than fails, an unstable partition — where
     * a genuine first start lists nothing for the received set at all, and must not wait.
     */
    static boolean preCheckLooksUnstable(java.util.Set<TopicPartition> received,
                                         java.util.Set<TopicPartition> listed) {
        boolean coversSome = false;
        for (TopicPartition tp : received) {
            if (listed.contains(tp)) {
                coversSome = true;
                break;
            }
        }
        return coversSome && !listed.containsAll(received);
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
        java.util.OptionalLong configured = GroupMembershipCommitter.configuredSessionTimeoutMillis(clientProps);
        return configured.isEmpty()
                ? java.time.Duration.ofSeconds(45)
                : java.time.Duration.ofMillis(configured.getAsLong());
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
     * Waits until any process stops or this runtime closes.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void awaitStopped() throws InterruptedException {
        stopped.await();
    }

    /**
     * Waits, bounded, until any process stops or this runtime closes.
     *
     * @param timeout how long to wait
     * @return {@code true} if a process stopped or the runtime closed within the timeout
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public boolean awaitStopped(java.time.Duration timeout) throws InterruptedException {
        return stopped.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
        stopped.countDown();
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
            if (admin != null) {
                admin.close(java.time.Duration.ofSeconds(TIMEOUT_SECONDS));
            }
        } catch (RuntimeException e) {
            LOG.warn("the admin client failed to close", e);
        }
    }
}
