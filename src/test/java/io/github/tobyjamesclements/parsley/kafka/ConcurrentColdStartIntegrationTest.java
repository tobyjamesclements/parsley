package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.Parsley;
import io.github.tobyjamesclements.parsley.api.ParsleyConfig;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.api.ProcessStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that several instances of one application can cold-start together (D48's
 * residual S1, closed by D108). Each finds offsets missing and joins the group as a
 * bootstrap member; a Streams join that meets another instance's still-open member is
 * refused as a protocol conflict, which the consumer treats as fatal. The runtime now waits
 * for other instances' members to leave before starting Streams and replaces a refused
 * thread rather than shutting the client down. Also pins what {@code Parsley.start}
 * returns into: the host's rebalance, not a running process.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class ConcurrentColdStartIntegrationTest {
    private static KafkaClusterTestKit cluster;
    private static Admin admin;

    @TempDir
    static Path stateDir;

    @BeforeAll
    static void startCluster() throws Exception {
        cluster = ClusterTestSupport.startCluster(Map.of("group.min.session.timeout.ms", "1000"));
        admin = Admin.create(Map.of("bootstrap.servers", cluster.bootstrapServers()));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        ClusterTestSupport.stopCluster(cluster, admin);
    }

    private static ParsleyConfig config(String prefix, String instance) {
        return ParsleyConfig.builder(cluster.bootstrapServers(), prefix)
                .stateDir(stateDir.resolve(prefix + "-" + instance).toString())
                .factsInterval(Duration.ofMillis(500))
                .build();
    }

    private static ProcessDefinition definition(String topic) {
        Channel<String, String> in = Channel.of(topic, Serdes.String(), Serdes.String());
        return ProcessDefinition.named("cc").receives(in, (d, s) -> Effects.none()).build();
    }

    @Test
    void startReturnsBeforeTheProcessIsRunning() throws Exception {
        admin.createTopics(List.of(new NewTopic("cc-single", 2, (short) 1))).all().get(30, TimeUnit.SECONDS);
        try (Parsley parsley = Parsley.start(config("ccs", "only"), definition("cc-single"))) {
            ProcessStatus.State immediately = parsley.status().get("cc").state();
            System.out.println("STATE IMMEDIATELY AFTER start(): " + immediately);
            assertEquals(ProcessStatus.State.REBALANCING, immediately,
                    "start returns once the host has been started, which is its rebalance; the Javadoc says so");
        }
    }

    /**
     * Eight concurrent cold starts of two instances each. The collision window is the
     * milliseconds between one instance's member leaving and the other's Streams join, so
     * it is probabilistic: before D108 it killed one instance's client in three of eight
     * rounds here. Every instance must now come up healthy.
     */
    @Test
    void twoInstancesColdStartingTogetherBothComeUpHealthy() throws Exception {
        int rounds = 8;
        List<String> failures = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            String topic = "cc-" + round;
            String prefix = "ccp" + round;
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(30, TimeUnit.SECONDS);
            CountDownLatch go = new CountDownLatch(1);
            Parsley[] instances = new Parsley[2];
            Throwable[] startFailures = new Throwable[2];
            Thread[] threads = new Thread[2];
            for (int i = 0; i < 2; i++) {
                int index = i;
                threads[i] = new Thread(() -> {
                    try {
                        go.await();
                        instances[index] = Parsley.start(config(prefix, "i" + index), definition(topic));
                    } catch (Throwable t) {
                        startFailures[index] = t;
                    }
                });
                threads[i].start();
            }
            go.countDown();
            for (Thread thread : threads) {
                thread.join(120_000);
            }
            try {
                for (int i = 0; i < 2; i++) {
                    if (startFailures[i] != null) {
                        failures.add("round " + round + " instance " + i + " start threw: " + startFailures[i]);
                    }
                }
                // Let the Streams clients reach RUNNING or die; a fatal join surfaces within seconds.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (System.nanoTime() < deadline) {
                    boolean settled = true;
                    for (Parsley p : instances) {
                        if (p == null) {
                            continue;
                        }
                        ProcessStatus status = p.status().get("cc");
                        if (status.state() == ProcessStatus.State.REBALANCING) {
                            settled = false;
                        }
                    }
                    if (settled) {
                        break;
                    }
                    Thread.sleep(200);
                }
                for (int i = 0; i < 2; i++) {
                    if (instances[i] == null) {
                        continue;
                    }
                    ProcessStatus status = instances[i].status().get("cc");
                    if (!instances[i].healthy()) {
                        failures.add("round " + round + " instance " + i + " unhealthy: state=" + status.state()
                                + " refusal=" + status.refusalReason() + " detail=" + status.failureDetail()
                                .map(d -> d.length() > 300 ? d.substring(0, 300) : d));
                    }
                }
            } finally {
                for (Parsley p : instances) {
                    if (p != null) {
                        p.close();
                    }
                }
            }
        }
        System.out.println("CONCURRENT COLD START: " + failures.size() + " failure(s) in " + rounds + " rounds");
        failures.forEach(f -> System.out.println("  " + f));
        assertTrue(failures.isEmpty(), "a concurrent cold start must leave every instance healthy: " + failures);
    }
}
