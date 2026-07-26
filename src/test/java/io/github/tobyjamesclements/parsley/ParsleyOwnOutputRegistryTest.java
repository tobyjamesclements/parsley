package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParsleyOwnOutputRegistry} and {@link ParsleyOwnOutputInterceptor} — the
 * producer-ack machinery behind the {@code ownOutputs} clock: the max-per-coordinate
 * ack fold, the config-key registry hand-off, and the
 * crossing-wait primitive whose contract is that it returns normally only on quiescence and
 * throws — failing the caller's EOS transaction — on timeout or on an acknowledgement failure.
 */
class ParsleyOwnOutputRegistryTest {

    private static final String OUT = "C2";
    private static final String REGISTRY_ID = "test-registry-" + ParsleyOwnOutputRegistryTest.class.hashCode();

    @AfterEach
    void unregister() {
        ParsleyOwnOutputRegistry.unregister(REGISTRY_ID);
    }

    /** A registry tracking {@code OUT}, registered under {@link #REGISTRY_ID} for interceptor lookup. */
    private static ParsleyOwnOutputRegistry registeredRegistry() {
        ParsleyOwnOutputRegistry registry = new ParsleyOwnOutputRegistry(Set.of(OUT));
        ParsleyOwnOutputRegistry.register(REGISTRY_ID, registry);
        return registry;
    }

    /** An interceptor configured to resolve the registry registered under {@link #REGISTRY_ID}. */
    private static ParsleyOwnOutputInterceptor configuredInterceptor() {
        ParsleyOwnOutputInterceptor interceptor = new ParsleyOwnOutputInterceptor();
        interceptor.configure(Map.of(ParsleyOwnOutputRegistry.CONFIG_KEY, REGISTRY_ID));
        return interceptor;
    }

    /** A successful-ack metadata for {@code (topic, partition, offset)}, as the producer builds it. */
    private static RecordMetadata metadata(String topic, int partition, long offset) {
        return new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 0L, 0, 0);
    }

    /** The registry's acked snapshot as a plain map, for assertions. */
    private static Map<TopicPartition, Long> ackedOf(ParsleyOwnOutputRegistry registry) {
        Map<TopicPartition, Long> acked = new HashMap<>();
        registry.forEachAcked((topic, partition, offset) ->
                acked.put(new TopicPartition(topic, partition), offset));
        return acked;
    }

    /**
     * Blocks until {@code waiter} is parked in the crossing wait's timed monitor wait. The tests
     * below call this from the simulated network thread before firing an acknowledgement, so the
     * ack provably arrives while the waiter is blocked — the only timed wait the test's main
     * thread ever enters is the tracker's {@code wait(ms)}, making the state check unambiguous.
     * The spin is bounded so a registry bug that never blocks cannot park this thread forever;
     * on expiry the caller proceeds and the test fails on its own assertions instead.
     */
    private static void awaitParkedInCrossingWait(Thread waiter) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiter.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadlineNanos) {
            Thread.onSpinWait();
        }
    }

    /**
     * The registry keeps the maximum successfully acked offset per coordinate: later, lower folds
     * (a slower sibling producer's ack) never lower an entry, and partitions are independent.
     */
    @Test
    void foldKeepsTheMaxAckedOffsetPerCoordinate() {
        ParsleyOwnOutputRegistry registry = new ParsleyOwnOutputRegistry(Set.of(OUT));

        registry.fold(OUT, 0, 5);
        registry.fold(OUT, 0, 3);
        registry.fold(OUT, 1, 7);
        registry.fold(OUT, 0, 9);

        Map<TopicPartition, Long> acked = ackedOf(registry);
        assertEquals(9L, acked.get(new TopicPartition(OUT, 0)),
                "the coordinate must report the maximum acked offset, never a later lower fold");
        assertEquals(7L, acked.get(new TopicPartition(OUT, 1)),
                "each partition is an independent own-output coordinate");
    }

    /**
     * The interceptor publishes a committed ack's exact coordinate into the registry it resolved
     * from its configuration — the production hand-off — and a
     * failed ack (non-null exception) folds nothing.
     */
    @Test
    void interceptorPublishesOnlyCommittedAcksToItsConfiguredRegistry() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();

        interceptor.onSend(new ProducerRecord<>(OUT, 0, "k", "v"));
        interceptor.onAcknowledgement(metadata(OUT, 0, 17), null);
        interceptor.onSend(new ProducerRecord<>(OUT, 0, "k", "v"));
        interceptor.onAcknowledgement(metadata(OUT, 0, -1L), new RuntimeException("aborted"));

        assertEquals(Map.of(new TopicPartition(OUT, 0), 17L), ackedOf(registry),
                "exactly the committed ack's coordinate must fold; a failed ack folds nothing");
        interceptor.close();
    }

    /**
     * An interceptor configured without a registry id — or whose id resolves to nothing, the
     * TopologyTestDriver case — is a permanent no-op: sends and acks pass through untracked
     * rather than failing the producer.
     */
    @Test
    void interceptorWithoutAResolvableRegistryIsANoOp() {
        ParsleyOwnOutputInterceptor unconfigured = new ParsleyOwnOutputInterceptor();
        unconfigured.configure(Map.of());
        ParsleyOwnOutputInterceptor unresolvable = new ParsleyOwnOutputInterceptor();
        unresolvable.configure(Map.of(ParsleyOwnOutputRegistry.CONFIG_KEY, "no-such-registry"));
        assertNull(ParsleyOwnOutputRegistry.lookup("no-such-registry"),
                "precondition: the id must not resolve");

        for (ParsleyOwnOutputInterceptor interceptor : new ParsleyOwnOutputInterceptor[]{unconfigured, unresolvable}) {
            interceptor.onSend(new ProducerRecord<>(OUT, 0, "k", "v"));
            interceptor.onAcknowledgement(metadata(OUT, 0, 3), null);
            interceptor.close();
        }
        // Reaching here without an exception is the assertion; nothing was registered to fold into.
    }

    /**
     * Sends to a topic outside the tracked sink set — changelog and repartition writes share the
     * stream producer — are ignored outright: they neither fold into the acked snapshot nor count
     * as pending, so a crossing wait never stalls on a changelog flush.
     */
    @Test
    void interceptorIgnoresUntrackedTopics() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();

        interceptor.onSend(new ProducerRecord<>("app-store-changelog", 0, "k", "v"));
        interceptor.onAcknowledgement(metadata("app-store-changelog", 0, 12), null);

        assertTrue(ackedOf(registry).isEmpty(),
                "an untracked topic's ack must not enter the own-output snapshot");
        // With the changelog send still "in flight" (no ack needed — it was never tracked), the
        // crossing wait must see nothing pending and return at once.
        registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 10);
        interceptor.close();
    }

    /**
     * A thread that has never sent has no bound tracker, and a thread whose only pending send is
     * to the very coordinate being stamped needs no wait (partition FIFO plus per-producer stamp
     * monotonicity cover same-partition ordering): both return immediately.
     */
    @Test
    void crossingWaitReturnsImmediatelyWithNothingPendingElsewhere() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 10);

        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, 0, "k", "v"));
        registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 10);
        interceptor.close();
    }

    /**
     * The business-forward form of the wait — an <em>empty</em> exclusion set — waits for full
     * quiescence: even a pending send to a coordinate a marker stamp could have excluded holds it,
     * because a business forward's destination partition is unknowable at stamp time and
     * over-waiting is the safe direction, since it can only delay. The pending send's ack releases it.
     */
    @Test
    void emptyExclusionWaitsOnEveryPendingCoordinate() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, 0, "k", "v"));

        assertThrows(CausalPendingAckException.class,
                () -> registry.awaitQuiescentExcept(Set.of(), 50),
                "with no excludable destination, any pending send must hold the wait");

        interceptor.onAcknowledgement(metadata(OUT, 0, 2), null);
        registry.awaitQuiescentExcept(Set.of(), 10);
        interceptor.close();
    }

    /**
     * The crossing wait blocks on a pending send to a different partition of the same sink topic
     * (the funnel case) and is released by that send's committed ack arriving from the
     * producer network thread.
     */
    @Test
    void crossingWaitBlocksUntilTheOtherCoordinatesAckArrives() throws Exception {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, 1, "k", "v"));

        Thread waiter = Thread.currentThread();
        CountDownLatch ackFired = new CountDownLatch(1);
        Thread ackThread = new Thread(() -> {
            awaitParkedInCrossingWait(waiter);
            interceptor.onAcknowledgement(metadata(OUT, 1, 4), null);
            ackFired.countDown();
        }, "test-ack-network-thread");
        ackThread.start();

        registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 5_000);
        assertTrue(ackFired.await(1, TimeUnit.SECONDS),
                "the wait must have been released by the ack, not by anything else");
        assertEquals(Map.of(new TopicPartition(OUT, 1), 4L), ackedOf(registry),
                "the releasing ack's coordinate must have folded");
        ackThread.join();
        interceptor.close();
    }

    /**
     * The never-stamp-and-proceed rule, timeout side: a crossing wait that runs out of time must
     * throw — failing the caller's EOS transaction — never return normally to a stamp that could
     * under-claim the still-unacknowledged coordinate.
     */
    @Test
    void crossingWaitTimeoutThrowsRatherThanProceedingToStamp() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, 1, "k", "v"));

        assertThrows(CausalPendingAckException.class,
                () -> registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 50),
                "an expired crossing wait must fail the transaction, never stamp-and-proceed");
        interceptor.close();
    }

    /**
     * The never-stamp-and-proceed rule, abort side: aborting a transaction fails each
     * pending send with exactly one exception-carrying callback, so the wait releases — and must
     * release by THROWING, dying with the transaction instead of stamping over a failed send.
     */
    @Test
    void ackFailureDuringTheCrossingWaitThrowsSoTheWaitDiesWithTheTransaction() throws Exception {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, 1, "k", "v"));

        Thread waiter = Thread.currentThread();
        Thread abortThread = new Thread(() -> {
            awaitParkedInCrossingWait(waiter);
            interceptor.onAcknowledgement(metadata(OUT, 1, -1L), new RuntimeException("aborted"));
        }, "test-abort-network-thread");
        abortThread.start();

        assertThrows(CausalPendingAckException.class,
                () -> registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 5_000),
                "a failed pending ack must surface as a throw, not as a satisfied wait");
        assertFalse(ackedOf(registry).containsKey(new TopicPartition(OUT, 1)),
                "the aborted send must not have folded an own-output coordinate");
        abortThread.join();
        interceptor.close();
    }

    /**
     * A send whose partition was still unassigned at {@code onSend} (no Parsley path produces one,
     * but the tracking must stay conservative) counts as pending against every coordinate — the
     * crossing wait cannot exclude it as "same partition", so it waits, and the send's ack (now
     * carrying the resolved partition) releases it.
     */
    @Test
    void unknownPartitionPendingBlocksConservativelyUntilItsAckResolvesIt() {
        ParsleyOwnOutputRegistry registry = registeredRegistry();
        ParsleyOwnOutputInterceptor interceptor = configuredInterceptor();
        interceptor.onSend(new ProducerRecord<>(OUT, null, "k", "v"));

        assertThrows(CausalPendingAckException.class,
                () -> registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 50),
                "an unknown-partition pending send might target another partition — the wait must "
                        + "hold it (conservatively) rather than exclude it");

        interceptor.onAcknowledgement(metadata(OUT, 0, 6), null);
        registry.awaitQuiescentExcept(Set.of(new TopicPartition(OUT, 0)), 10);
        assertEquals(Map.of(new TopicPartition(OUT, 0), 6L), ackedOf(registry),
                "the resolving ack must fold at its real coordinate");
        interceptor.close();
    }
}
