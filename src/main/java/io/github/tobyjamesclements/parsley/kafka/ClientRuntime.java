package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.ProcessStatus;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;

/**
 * The kafka-clients host (D114): each process runs as one {@link ProcessRunner} over the
 * plain consumer, producer and admin APIs, with no Kafka Streams in the path.
 *
 * <p>What this host does not need from the Streams host: a bootstrap group member to
 * pre-commit initial positions (the consumer seeks on assignment and its first transaction
 * commits them under the generation fence); a probe for trailing never-yielding runs (the
 * consumer's own position after a poll is the read-position report); held messages
 * persisted to a changelog (the read position commits at the head of the hold-back
 * buffer and the log re-feeds the rest); and an end-to-end changelog read before start
 * (each task restores its own partition on assignment).
 *
 * <p>Experimental. Application stores are materialised in memory from their changelogs on
 * every assignment.
 *
 * @see ParsleyRuntime
 */
public final class ClientRuntime implements RuntimeHandle {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRuntime.class);
    private static final long TIMEOUT_SECONDS = 30;
    /** The metadata stamp on every offset this host commits. */
    static final String OFFSET_STAMP = "parsley.clients";
    /** Suffix of the compacted topic holding a process's ordering state. */
    static final String ORDERING_TOPIC_SUFFIX = "-" + Store.RESERVED_PREFIX + "ordering";

    private final Admin admin;
    private final Map<String, ProcessRunner> runners = new LinkedHashMap<>();
    private final List<AdminFactsSource> factsSources = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<String, ProcessDiagnostics> diagnosticsByProcess = new HashMap<>();
    private final java.util.concurrent.CountDownLatch stopped = new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.ExecutorService factsExecutor =
            java.util.concurrent.Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "parsley-facts");
                thread.setDaemon(true);
                return thread;
            });

    private ClientRuntime(Admin admin) {
        this.admin = admin;
    }

    /**
     * Resolves topics, ensures each process's state topics exist at the right width, and
     * starts one thread per process.
     */
    public static ClientRuntime start(ParsleyConfig config, List<ProcessDefinition> definitions) {
        ParsleyRuntime.validateDistinctNames(definitions);
        Map<String, Object> clientProps = new HashMap<>(config.extraProperties());
        clientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        Admin admin = Admin.create(clientProps);
        ClientRuntime runtime = new ClientRuntime(admin);
        try {
            Map<String, TopicInfo> topics = ParsleyRuntime.resolveTopicsCorroborated(
                    () -> admin.describeTopics(ParsleyRuntime.declaredTopics(definitions)).allTopicNames()
                            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    Duration.ofMillis(500));
            long factsClockOrigin = System.nanoTime();
            for (ProcessDefinition definition : definitions) {
                String applicationId = config.applicationIdPrefix() + "-" + definition.name();
                int width = 0;
                for (String topic : definition.receivedTopics()) {
                    width = Math.max(width, topics.get(topic).partitions());
                }
                String orderingTopic = applicationId + ORDERING_TOPIC_SUFFIX;
                runtime.ensureStateTopic(applicationId, orderingTopic, width, true);
                Map<String, String> changelogByStore = new HashMap<>();
                for (Store<?, ?> store : definition.stores()) {
                    String changelog = ProcessTopology.changelogName(applicationId, store.name());
                    changelogByStore.put(store.name(), changelog);
                    runtime.ensureStateTopic(applicationId, changelog, width, false);
                }
                Map<UUID, String> namesById = new HashMap<>();
                topics.forEach((name, info) -> namesById.put(info.topicId(), name));
                AdminFactsSource factsSource = new AdminFactsSource(admin, applicationId, namesById, clientProps,
                        Math.max(config.factsInterval().toMillis() * 3, 3_000L),
                        () -> (System.nanoTime() - factsClockOrigin) / 1_000_000L);
                runtime.factsSources.add(factsSource);
                ProcessDiagnostics diagnostics = new ProcessDiagnostics();
                runtime.diagnosticsByProcess.put(definition.name(), diagnostics);
                runtime.runners.put(definition.name(), new ProcessRunner(definition, applicationId, topics,
                        orderingTopic, changelogByStore, clientProps, config.metadataBudgetBytes(),
                        config.factsInterval(), admin, factsSource, runtime.factsExecutor, diagnostics,
                        runtime.stopped::countDown));
            }
            runtime.runners.values().forEach(ProcessRunner::start);
            return runtime;
        } catch (RuntimeException e) {
            runtime.close();
            throw e;
        }
    }

    /**
     * Creates a compacted state topic at the task width when absent, and refuses one that
     * exists at another width: ordering state is partitioned by task, and a task count the
     * topic cannot follow is the width change {@code TASK_WIDTH_CHANGED} names.
     */
    private void ensureStateTopic(String applicationId, String topic, int width, boolean ordering) {
        Optional<TopicDescription> existing = describe(topic);
        if (existing.isEmpty()) {
            NewTopic newTopic = new NewTopic(topic, Optional.of(width), Optional.empty())
                    .configs(Map.of("cleanup.policy", "compact"));
            try {
                admin.createTopics(List.of(newTopic)).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                LOG.info("{}: created {} with {} partition(s)", applicationId, topic, width);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException(applicationId + ": could not create " + topic, e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(applicationId + ": interrupted creating " + topic, e);
            } catch (Exception e) {
                throw new IllegalStateException(applicationId + ": could not create " + topic, e);
            }
            for (int attempt = 0; existing.isEmpty() && attempt < 20; attempt++) {
                existing = describe(topic);
                if (existing.isEmpty()) {
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(applicationId + ": interrupted describing " + topic, e);
                    }
                }
            }
            if (existing.isEmpty()) {
                throw new IllegalStateException(applicationId + ": " + topic + " was created but cannot be described");
            }
        }
        int stored = existing.get().partitions().size();
        if (stored != width) {
            throw new ParsleyFailClosedException(ParsleyFailClosedException.Reason.TASK_WIDTH_CHANGED,
                    applicationId + ": " + (ordering ? "this process's ordering state" : topic) + " was built for "
                            + stored + " task(s) but the declaration now induces " + width
                            + " (the widest received topic's partition count changed). Restore the previous"
                            + " declaration and partition counts, or reset the process's state and group offsets"
                            + " deliberately.");
        }
    }

    private Optional<TopicDescription> describe(String topic) {
        return ParsleyRuntime.describeChangelogCorroborated(topic,
                () -> admin.describeTopics(List.of(topic)).allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topic),
                Duration.ofMillis(500));
    }

    /** Consumer properties with the guarantee-bearing pins applied. */
    static Map<String, Object> consumerProperties(Map<String, Object> clientProps, String applicationId) {
        Map<String, Object> props = new HashMap<>(clientProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationId + "-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CoPartitionAssignor.class.getName());
        return props;
    }

    /** Properties for the group-less consumer that restores task state from its topics. */
    static Map<String, Object> restoreConsumerProperties(Map<String, Object> clientProps) {
        Map<String, Object> props = new HashMap<>(clientProps);
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return props;
    }

    /** Transactional producer properties. The transaction timeout matches Streams' EOS default. */
    static Map<String, Object> producerProperties(Map<String, Object> clientProps, String transactionalId) {
        Map<String, Object> props = new HashMap<>(clientProps);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10_000);
        return props;
    }

    /**
     * Names the condition a process thread's failure evidences, as {@code ParsleyRuntime}
     * does for a stream thread: a stop the substrate detected but that recurs identically on
     * restart carries its reason into {@code status()}.
     */
    static Throwable diagnose(String process, Throwable exception) {
        return switch (ParsleyRuntime.classifyFailure(exception)) {
            case POSITIONS_DISCARDED_UNREAD -> new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.POSITIONS_DISCARDED_UNREAD,
                    "process " + process + ": the broker no longer retains this process's committed read"
                            + " position; positions were discarded before they were read (SPEC Safety 8)."
                            + " Reset the process's state and group offsets deliberately to proceed.",
                    exception);
            case RECORD_TOO_LARGE -> new ParsleyFailClosedException(
                    ParsleyFailClosedException.Reason.SUBSTRATE_MISCONFIGURED,
                    "process " + process + ": a record exceeded a size limit; raise the topic's"
                            + " max.message.bytes and, if needed, max.request.size, then restart.",
                    exception);
            default -> exception;
        };
    }

    @Override
    public boolean healthy() {
        return runners.values().stream().allMatch(ProcessRunner::running);
    }

    @Override
    public Map<String, ProcessStatus> status() {
        Map<String, ProcessStatus> statuses = new LinkedHashMap<>();
        runners.forEach((process, runner) -> {
            Throwable failure = runner.failure();
            ParsleyFailClosedException refusal = ParsleyFailClosedException.findIn(failure);
            statuses.put(process, new ProcessStatus(process, runner.state(),
                    Optional.ofNullable(refusal).map(ParsleyFailClosedException::reason),
                    Optional.ofNullable(failure).map(Throwable::getMessage),
                    diagnosticsByProcess.get(process).snapshot()));
        });
        return statuses;
    }

    @Override
    public void awaitStopped() throws InterruptedException {
        stopped.await();
    }

    @Override
    public boolean awaitStopped(Duration timeout) throws InterruptedException {
        return stopped.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        stopped.countDown();
        for (ProcessRunner runner : runners.values()) {
            try {
                runner.stop(Duration.ofSeconds(TIMEOUT_SECONDS));
            } catch (RuntimeException e) {
                LOG.warn("a process failed to stop; continuing with the remaining resources", e);
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
            admin.close(Duration.ofSeconds(TIMEOUT_SECONDS));
        } catch (RuntimeException e) {
            LOG.warn("the admin client failed to close", e);
        }
    }
}
