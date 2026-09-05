package io.github.tobyjamesclements.parsley;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the protocol names no host facility and no runtime type.
 *
 * <p>This is what lets the simulator drive a real {@link ProcessEngine} with no broker, no
 * clock and no thread, which is where the causal-order evidence comes from, and it is SPEC
 * Structural 7's decision unit standing on its own. Until the packages were collapsed the
 * boundary was a directory: the protocol lived in a subpackage and a recursive scan fenced
 * it. One package cannot be fenced that way, so the split is declared here instead, and the
 * declaration is checked three ways.
 *
 * <p>{@link #everyMainSourceIsClassified()} is the one that keeps the other two honest. A
 * file added to the package belongs to neither list until someone puts it in one, and that
 * test fails until they do, so a new protocol source cannot arrive unfenced.
 */
class ProtocolPurityTest {

    private static final Path MAIN = Path.of(
            "src", "main", "java", "io", "github", "tobyjamesclements", "parsley");

    /**
     * The protocol: the causal frontier, its wire codec, the hold-back buffer, the pure
     * deliverability decision, the engine over an ordering store, and the diagnosis an
     * operator reads from stored state. These are fenced.
     */
    private static final Set<String> PROTOCOL = Set.of(
            "Causes",
            "CausesCodec",
            "ChannelId",
            "Deliverability",
            "DeliverableMessage",
            "FailClosedException",
            "Header",
            "IdentityReport",
            "OrderingStateCodec",
            "OrderingStateInspector",
            "OrderingStore",
            "ProcessEngine",
            "ReceivedMessage",
            "Sabotage");

    /**
     * The declaration surface and the Kafka Streams runtime behind it. These may name
     * whatever the host requires, and the protocol may not name them.
     */
    private static final Set<String> RUNTIME = Set.of(
            "AdminTopicIdentitySource",
            "Channel",
            "Delivery",
            "Effects",
            "GroupMembershipCommitter",
            "Handler",
            "Parsley",
            "ParsleyConfig",
            "Process",
            "ProcessNode",
            "ProcessStatus",
            "ProcessTopology",
            "ResolvedTopic",
            "State",
            "Store",
            "StreamsOrderingStore",
            "StreamsRuntime",
            "TopicIdentitySource",
            "TopicIdentityVerdicts");

    /** Clock, randomness, network, filesystem and substrate references the protocol may not name. */
    private static final List<String> HOST_FACILITIES = List.of(
            "org.apache.kafka",
            "java.net.",
            "java.nio.channels",
            "java.nio.file",
            "java.io.",
            "java.time.",
            "java.util.Date",
            "java.util.Random",
            "java.util.concurrent",
            "ThreadLocalRandom",
            "SecureRandom",
            "UUID.randomUUID",
            "Math.random",
            "Thread.sleep",
            "System.currentTimeMillis",
            "System.nanoTime",
            "System.getenv",
            "System.getProperty",
            "Instant.now",
            "Clock.");

    /**
     * Runtime types whose names are unambiguous enough to scan for in prose. The rest of
     * {@link #RUNTIME} is left out on purpose: Parsley, Channel, Store, Delivery, State and
     * Process are ordinary English in a comment, and a protocol file naming one of those in
     * prose is not the drift this catches. Every entry here is a Kafka Streams host class
     * whose name appears in a protocol source only if the protocol reaches for the host.
     */
    private static final List<String> RUNTIME_TYPES = List.of(
            "AdminTopicIdentitySource",
            "GroupMembershipCommitter",
            "ParsleyConfig",
            "ProcessNode",
            "ProcessTopology",
            "ResolvedTopic",
            "StreamsOrderingStore",
            "StreamsRuntime",
            "TopicIdentitySource",
            "TopicIdentityVerdicts");

    /** Every main source is declared protocol or runtime, so a new file cannot arrive unfenced. */
    @Test
    void everyMainSourceIsClassified() throws IOException {
        Set<String> declared = new TreeSet<>(PROTOCOL);
        declared.addAll(RUNTIME);
        assertEquals(new TreeSet<>(declared), new TreeSet<>(mainSourceNames()),
                "every source in " + MAIN + " must be listed as protocol or runtime in this test."
                        + " A new file is fenced as protocol until it is listed as runtime, and"
                        + " listing it as runtime is the deliberate act that says it may name the"
                        + " host");
    }

    /** No protocol source names a clock, randomness, the network, the filesystem or the substrate. */
    @Test
    void protocolSourcesNameNoHostFacility() throws IOException {
        for (String name : PROTOCOL) {
            String source = Files.readString(MAIN.resolve(name + ".java"));
            for (String facility : HOST_FACILITIES) {
                assertTrue(!source.contains(facility),
                        name + ".java must not use \"" + facility + "\": the protocol decides from"
                                + " its arguments alone, which is what lets the simulator drive it"
                                + " with no broker, no clock and no thread");
            }
        }
    }

    /**
     * No protocol source names a Kafka Streams runtime type. Collapsing the packages removed
     * the import that used to make such a reference visible, so it is named here instead.
     */
    @Test
    void protocolSourcesNameNoRuntimeType() throws IOException {
        for (String name : PROTOCOL) {
            String source = Files.readString(MAIN.resolve(name + ".java"));
            for (String runtimeType : RUNTIME_TYPES) {
                assertTrue(!source.contains(runtimeType),
                        name + ".java must not name " + runtimeType + ": the protocol runs under a"
                                + " simulator as readily as under Kafka Streams, and the dependency"
                                + " runs from the runtime to the protocol, never back");
            }
        }
    }

    /** The names of every main source, package-info aside, which declares no type. */
    private static List<String> mainSourceNames() throws IOException {
        assertTrue(Files.isDirectory(MAIN), MAIN + " must be present for this scan");
        try (Stream<Path> files = Files.list(MAIN)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(file -> file.endsWith(".java"))
                    .filter(file -> !file.equals("package-info.java"))
                    .map(file -> file.substring(0, file.length() - ".java".length()))
                    .collect(Collectors.toList());
        }
    }
}
