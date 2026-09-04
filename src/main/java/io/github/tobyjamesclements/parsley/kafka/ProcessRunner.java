package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.ProcessStatus;
import io.github.tobyjamesclements.parsley.api.Store;
import io.github.tobyjamesclements.parsley.core.ChannelId;
import io.github.tobyjamesclements.parsley.core.OrderingStateInspector;
import io.github.tobyjamesclements.parsley.core.ParsleyFailClosedException;
import io.github.tobyjamesclements.parsley.core.PositionFacts;

/**
 * One process under the kafka-clients host: a consumer thread over the received topics,
 * a transactional producer, and one {@link ProcessTask} per assigned partition (D114).
 *
 * <p>The loop is the plain consume-transform-produce pattern. Each poll feeds its records
 * to the tasks they belong to; the consumer's positions after the poll are the read-position
 * report; a transaction opens on the first write or send of a step and, at the commit
 * interval, carries the staged state writes and the read positions to the broker under the
 * group's generation fence. A step that fails aborts the transaction and stops the process,
 * failing closed. A rebalance commits on revoke and rebuilds every task from its committed
 * state on assignment, so nothing the previous incarnation held in memory outlives it.
 */
final class ProcessRunner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessRunner.class);
    private static final Duration POLL = Duration.ofMillis(100);
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);
    private static final Duration RESTORE_STALL = Duration.ofSeconds(30);

    private final ProcessDefinition definition;
    private final String applicationId;
    private final Map<String, TopicInfo> topics;
    private final String orderingTopic;
    private final Map<String, String> changelogByStore;
    private final int metadataBudgetBytes;
    private final Duration factsInterval;
    private final Admin admin;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaConsumer<byte[], byte[]> restoreConsumer;
    private final KafkaProducer<byte[], byte[]> producer;
    private final FactsSource factsSource;
    private final Executor factsExecutor;
    private final ProcessDiagnostics diagnostics;
    private final Thread thread;
    private final Runnable onStopped;

    private final Map<Integer, ProcessTask> tasks = new TreeMap<>();
    private final Map<TopicPartition, Long> lastReported = new HashMap<>();
    private boolean transactionOpen;
    private long transactionOpenedAtNanos;

    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile ProcessStatus.State state = ProcessStatus.State.REBALANCING;
    private volatile Throwable failure;

    private final AtomicReference<PositionFacts> pendingFacts = new AtomicReference<>();
    private final AtomicBoolean gatherInFlight = new AtomicBoolean();
    private long lastGatherLaunchNanos;

    ProcessRunner(ProcessDefinition definition, String applicationId, Map<String, TopicInfo> topics,
                  String orderingTopic, Map<String, String> changelogByStore, Map<String, Object> clientProps,
                  int metadataBudgetBytes, Duration factsInterval, Admin admin, FactsSource factsSource,
                  Executor factsExecutor, ProcessDiagnostics diagnostics, Runnable onStopped) {
        this.onStopped = onStopped;
        this.definition = definition;
        this.applicationId = applicationId;
        this.topics = topics;
        this.orderingTopic = orderingTopic;
        this.changelogByStore = changelogByStore;
        this.metadataBudgetBytes = metadataBudgetBytes;
        this.factsInterval = factsInterval;
        this.admin = admin;
        this.factsSource = factsSource;
        this.factsExecutor = factsExecutor;
        this.diagnostics = diagnostics;
        this.consumer = new KafkaConsumer<>(ClientRuntime.consumerProperties(clientProps, applicationId),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer());
        this.restoreConsumer = new KafkaConsumer<>(ClientRuntime.restoreConsumerProperties(clientProps),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer());
        this.producer = new KafkaProducer<>(
                ClientRuntime.producerProperties(clientProps, applicationId + "-" + UUID.randomUUID()),
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        this.thread = new Thread(this, "parsley-" + applicationId);
    }

    void start() {
        thread.start();
    }

    /** Asks the loop to stop and waits, bounded, for it to release its clients. */
    void stop(Duration timeout) {
        stopRequested.set(true);
        consumer.wakeup();
        try {
            if (!thread.join(timeout)) {
                LOG.warn("{}: process thread did not stop within {}", applicationId, timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    ProcessStatus.State state() {
        return state;
    }

    Throwable failure() {
        return failure;
    }

    boolean running() {
        return failure == null && state != ProcessStatus.State.STOPPED;
    }

    void awaitStopped() throws InterruptedException {
        stopped.await();
    }

    boolean awaitStopped(Duration timeout) throws InterruptedException {
        return stopped.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void run() {
        try {
            producer.initTransactions();
            consumer.subscribe(definition.receivedTopics(), new Listener());
            while (!stopRequested.get()) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(POLL);
                } catch (WakeupException e) {
                    break;
                }
                if (stopRequested.get()) {
                    break;
                }
                feed(records);
                reportPositions();
                gatherAndApplyFacts();
                maybeCommit();
            }
            if (transactionOpen) {
                commit();
            }
        } catch (Throwable t) {
            fail(t);
        } finally {
            shutdown();
        }
    }

    private void feed(ConsumerRecords<byte[], byte[]> records) {
        for (TopicPartition tp : sortedPartitions(records)) {
            ProcessTask task = tasks.get(tp.partition());
            if (task == null) {
                throw new IllegalStateException(applicationId + ": records for unassigned " + tp);
            }
            for (var record : records.records(tp)) {
                task.receive(record);
            }
        }
    }

    private static List<TopicPartition> sortedPartitions(ConsumerRecords<byte[], byte[]> records) {
        List<TopicPartition> partitions = new ArrayList<>(records.partitions());
        partitions.sort(java.util.Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        return partitions;
    }

    /**
     * The read-position report: the consumer's position on each received partition after
     * the poll. Reported only where it moved, so an idle process costs nothing here.
     */
    private void reportPositions() {
        for (ProcessTask task : tasks.values()) {
            boolean moved = false;
            for (TopicPartition tp : task.partitions().values()) {
                long position = consumer.position(tp);
                Long last = lastReported.put(tp, position);
                if (last == null || last != position) {
                    moved = true;
                }
            }
            if (moved) {
                task.reportPositions(consumer::position);
            }
        }
    }

    private void gatherAndApplyFacts() {
        PositionFacts facts = pendingFacts.getAndSet(null);
        if (facts != null) {
            for (ProcessTask task : tasks.values()) {
                task.onFacts(facts);
            }
            publishStatus();
        }
        long now = System.nanoTime();
        if (tasks.isEmpty() || now - lastGatherLaunchNanos < factsInterval.toNanos()
                || !gatherInFlight.compareAndSet(false, true)) {
            return;
        }
        lastGatherLaunchNanos = now;
        Set<ChannelId> received = new TreeSet<>();
        Set<ChannelId> frontier = new TreeSet<>();
        for (ProcessTask task : tasks.values()) {
            received.addAll(task.engine().receivedChannelSet());
            frontier.addAll(task.engine().frontierSnapshot().byChannel().keySet());
        }
        try {
            factsExecutor.execute(() -> {
                try {
                    pendingFacts.set(factsSource.gather(received, Map.of(), frontier));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.warn("{}: position facts unavailable, retrying next round", applicationId, e);
                } finally {
                    gatherInFlight.set(false);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            gatherInFlight.set(false);
        }
    }

    private void publishStatus() {
        for (ProcessTask task : tasks.values()) {
            diagnostics.publish(task.snapshot());
        }
    }

    /** Opens the transaction a step's first write or send will commit in. */
    private void ensureTransaction() {
        if (!transactionOpen) {
            producer.beginTransaction();
            transactionOpen = true;
            transactionOpenedAtNanos = System.nanoTime();
        }
    }

    private void maybeCommit() {
        if (transactionOpen && System.nanoTime() - transactionOpenedAtNanos >= COMMIT_INTERVAL.toNanos()) {
            commit();
        }
    }

    /**
     * Commits the open transaction: every task's staged state writes, then the read
     * positions under the group's generation, then the transaction itself. A commit the
     * broker refuses for a stale generation is aborted and the tasks discarded; the
     * rebalance that bumped the generation rebuilds them from committed state.
     */
    private void commit() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        try {
            for (ProcessTask task : tasks.values()) {
                task.flushWrites();
                offsets.putAll(task.offsetsToCommit(consumer::position));
            }
            producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
            producer.commitTransaction();
            transactionOpen = false;
        } catch (CommitFailedException | InvalidProducerEpochException e) {
            LOG.warn("{}: commit refused by the group coordinator; discarding the tasks for the rebalance",
                    applicationId, e);
            abortAndDiscard();
        }
    }

    private void abortAndDiscard() {
        if (transactionOpen) {
            try {
                producer.abortTransaction();
            } catch (RuntimeException e) {
                LOG.warn("{}: abort failed", applicationId, e);
            }
            transactionOpen = false;
        }
        discardTasks();
    }

    private void discardTasks() {
        for (ProcessTask task : tasks.values()) {
            diagnostics.retire(task.partition());
        }
        tasks.clear();
        lastReported.clear();
    }

    private void fail(Throwable t) {
        Throwable diagnosed = ClientRuntime.diagnose(definition.name(), t);
        failure = diagnosed;
        LOG.error("process {} failed; stopping it (failing closed)", definition.name(), diagnosed);
        if (transactionOpen) {
            try {
                producer.abortTransaction();
            } catch (RuntimeException e) {
                LOG.warn("{}: abort after failure also failed", applicationId, e);
            }
            transactionOpen = false;
        }
    }

    private void shutdown() {
        state = ProcessStatus.State.STOPPED;
        stopped.countDown();
        onStopped.run();
        discardTasks();
        try {
            consumer.close(Duration.ofSeconds(10));
        } catch (RuntimeException e) {
            LOG.warn("{}: consumer close failed", applicationId, e);
        }
        try {
            producer.close(Duration.ofSeconds(10));
        } catch (RuntimeException e) {
            LOG.warn("{}: producer close failed", applicationId, e);
        }
        try {
            restoreConsumer.close(Duration.ofSeconds(10));
        } catch (RuntimeException e) {
            LOG.warn("{}: restore consumer close failed", applicationId, e);
        }
    }

    /**
     * The rebalance protocol: commit on revoke, discard, rebuild on assign. Both callbacks
     * run on this thread inside {@code poll}, so a refusal raised while rebuilding a task —
     * an identity that changed, state that cannot be trusted — propagates out of the poll
     * and stops the process.
     */
    private final class Listener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            state = ProcessStatus.State.REBALANCING;
            if (transactionOpen) {
                commit();
            }
            discardTasks();
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            state = ProcessStatus.State.REBALANCING;
            abortAndDiscard();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            Map<Integer, List<TopicPartition>> byTask = new TreeMap<>();
            for (TopicPartition tp : partitions) {
                byTask.computeIfAbsent(tp.partition(), p -> new ArrayList<>()).add(tp);
            }
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
            for (var entry : byTask.entrySet()) {
                tasks.put(entry.getKey(), buildTask(entry.getKey(), entry.getValue(), committed));
            }
            publishStatus();
            state = ProcessStatus.State.RUNNING;
        }
    }

    private ProcessTask buildTask(int partition, List<TopicPartition> assigned,
                                  Map<TopicPartition, OffsetAndMetadata> committed) {
        Map<byte[], byte[]> orderingView = readPartition(orderingTopic, partition);
        Map<String, Map<byte[], byte[]>> storeViews = new HashMap<>();
        for (Store<?, ?> store : definition.stores()) {
            storeViews.put(store.name(), readPartition(changelogByStore.get(store.name()), partition));
        }
        boolean priorState = orderingView.values().stream().anyMatch(java.util.Objects::nonNull);
        refuseLostOrderingState(partition, assigned, committed, priorState);
        refuseStrandedHeldMessages(partition, orderingView);

        // Initial positions are resolved to concrete offsets here rather than through
        // seekToBeginning/seekToEnd: those defer to the consumer's reset policy, which is
        // pinned to none, and the first transaction's offset commit is what establishes the
        // position under the group's generation fence — no bootstrap member needed.
        Map<TopicPartition, Long> resumeNextRead = new HashMap<>();
        Set<TopicPartition> wantEarliest = new HashSet<>();
        Set<TopicPartition> wantLatest = new HashSet<>();
        for (TopicPartition tp : assigned) {
            OffsetAndMetadata offset = committed.get(tp);
            if (offset != null) {
                resumeNextRead.put(tp, offset.offset());
                continue;
            }
            Channel.InitialPosition initial = priorState
                    ? Channel.InitialPosition.EARLIEST
                    : definition.input(tp.topic()).channel().initialPosition();
            (initial == Channel.InitialPosition.EARLIEST ? wantEarliest : wantLatest).add(tp);
        }
        if (!wantEarliest.isEmpty()) {
            resumeNextRead.putAll(consumer.beginningOffsets(wantEarliest));
        }
        if (!wantLatest.isEmpty()) {
            // Under read_committed the consumer's end offset is the last stable offset, the
            // same read-committed view the Streams host's bootstrap asked the admin client for.
            resumeNextRead.putAll(consumer.endOffsets(wantLatest));
        }
        resumeNextRead.forEach(consumer::seek);
        return new ProcessTask(definition, topics, partition, orderingTopic, orderingView, storeViews,
                changelogByStore, resumeNextRead, metadataBudgetBytes, producer, ProcessRunner.this::ensureTransaction);
    }

    /**
     * Committed read positions with no ordering records behind them mean the state of the
     * task's most recent committed step is gone (SPEC Host obligation 5): every commit
     * writes state and positions atomically, and the first ever wrote the version entry,
     * which compaction retains.
     */
    private void refuseLostOrderingState(int partition, List<TopicPartition> assigned,
                                         Map<TopicPartition, OffsetAndMetadata> committed, boolean priorState) {
        if (priorState) {
            return;
        }
        for (TopicPartition tp : assigned) {
            if (committed.get(tp) != null) {
                throw new ParsleyFailClosedException(ParsleyFailClosedException.Reason.ORDERING_STATE_LOST,
                        applicationId + ": committed read positions exist for " + tp + " but partition " + partition
                                + " of " + orderingTopic + " holds no ordering records. The ordering state of the"
                                + " most recent committed step has been lost (SPEC Host obligation 5) and resuming"
                                + " would silently under-express causes delivered before the loss. Restore the"
                                + " topic and its records, or reset (delete) the process's group offsets"
                                + " deliberately to start fresh.");
            }
        }
    }

    /**
     * A channel this declaration no longer receives, whose committed read position sits at
     * or below the coverage the previous execution recorded, still had messages held when
     * it was dropped: their re-feed is the only copy, and no task will ask for it (SPEC
     * Structural 16).
     */
    private void refuseStrandedHeldMessages(int partition, Map<byte[], byte[]> orderingView) {
        Map<ChannelId, Long> covered = OrderingStateInspector.coveredPositions(orderingView);
        Map<String, UUID> bindings = OrderingStateInspector.nameBindings(orderingView);
        Map<UUID, String> nameById = new HashMap<>();
        bindings.forEach((name, id) -> nameById.put(id, name));
        Set<ChannelId> declared = new HashSet<>();
        for (String topic : definition.receivedTopics()) {
            declared.add(new ChannelId(topics.get(topic).topicId(), partition));
        }
        Set<TopicPartition> candidates = new HashSet<>();
        Map<TopicPartition, ChannelId> channelOf = new HashMap<>();
        covered.forEach((channel, fed) -> {
            String name = nameById.get(channel.topicId());
            if (!declared.contains(channel) && name != null && fed != Long.MAX_VALUE) {
                TopicPartition tp = new TopicPartition(name, channel.partition());
                candidates.add(tp);
                channelOf.put(tp, channel);
            }
        });
        if (candidates.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(candidates);
        List<ChannelId> stranded = new ArrayList<>();
        committed.forEach((tp, offset) -> {
            if (offset != null && offset.offset() <= covered.get(channelOf.get(tp))) {
                stranded.add(channelOf.get(tp));
            }
        });
        if (!stranded.isEmpty()) {
            throw new ParsleyFailClosedException(ParsleyFailClosedException.Reason.CHANNEL_REMOVED_WITH_HELD_MESSAGES,
                    applicationId + ": received messages remain undelivered on " + stranded
                            + ", which the new declaration no longer receives");
        }
    }

    /**
     * Reads one partition of a compacted topic to its true end, compacted to the latest
     * value per key. The end is the log's, not the last stable offset: a superseded
     * incarnation's open transaction resolves within its timeout, which the stall deadline
     * outlasts, and stopping short would hide committed records above it (D79).
     */
    private Map<byte[], byte[]> readPartition(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        try {
            Map<TopicPartition, Long> ends = new HashMap<>();
            admin.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
                    .all().get(30, TimeUnit.SECONDS).forEach((p, info) -> ends.put(p, info.offset()));
            restoreConsumer.assign(List.of(tp));
            restoreConsumer.resume(restoreConsumer.paused());
            restoreConsumer.seekToBeginning(List.of(tp));
            return ParsleyRuntime.readToEnds(restoreConsumer, topic, List.of(tp), ends, RESTORE_STALL).latest();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(applicationId + ": could not read " + tp + " to restore task state", e);
        }
    }
}
