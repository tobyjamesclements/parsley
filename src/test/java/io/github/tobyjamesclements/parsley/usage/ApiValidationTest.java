package io.github.tobyjamesclements.parsley.usage;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import io.github.tobyjamesclements.parsley.Topic;
import io.github.tobyjamesclements.parsley.Effects;
import io.github.tobyjamesclements.parsley.FailClosedException;
import io.github.tobyjamesclements.parsley.Header;
import io.github.tobyjamesclements.parsley.Parsley;
import io.github.tobyjamesclements.parsley.ParsleyConfig;
import io.github.tobyjamesclements.parsley.Process;
import io.github.tobyjamesclements.parsley.ProcessStatus;
import io.github.tobyjamesclements.parsley.Store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Establishes that the declaration surface refuses what would weaken the guarantee.
 *
 * <p>Reserved names, reserved headers and owned configuration keys are rejected at
 * construction, before any broker is contacted, and null components are refused with
 * {@code IllegalArgumentException} everywhere (the one-rule taxonomy).
 */
class ApiValidationTest {

    private static Topic<String, String> channel(String topic) {
        return Topic.of(topic, Serdes.String(), Serdes.String());
    }

    private static Store<String, String> store(String name) {
        return Store.of(name, Serdes.String(), Serdes.String());
    }

    /** Reserved store names are unconstructible. */
    @Test
    void reservedStoreNamesAreUnconstructible() {
        assertThrows(IllegalArgumentException.class,
                () -> store("__parsley.anything"),
                "application state may never alias ordering state (SPEC Structural 8)");
    }

    /** Store names containing the reserved namespace anywhere are unconstructible. */
    @Test
    void storeNamesContainingTheReservedNamespaceAnywhereAreUnconstructible() {
        assertThrows(IllegalArgumentException.class,
                () -> store("q-__parsley.ordering"),
                "an embedded \"__parsley.\" composes a changelog byte-identical to the ordering"
                        + " changelog of a sibling process (here one named \"p-q\"), landing"
                        + " application state in the guarantee-bearing topic");
    }

    /** Topic topics containing the reserved namespace are refused at declaration. */
    @Test
    void reservedNamespaceTopicsAreUnconstructible() {
        assertThrows(IllegalArgumentException.class,
                () -> channel("__parsley.ordering"),
                "the reserved namespace is one rule at one site: what start() would refuse must"
                        + " already be unconstructible");
        assertThrows(IllegalArgumentException.class,
                () -> channel("x-p-__parsley.ordering-changelog"),
                "containment, not prefix: an internal changelog name embeds the namespace"
                        + " mid-string");
    }

    /** Reserved headers are unconstructible. */
    @Test
    void reservedHeadersAreUnconstructible() {
        Topic<String, String> channel = channel("t");
        FailClosedException e =
                assertThrows(FailClosedException.class,
                        () -> Effects.builder().send(channel, "k", "v",
                                List.of(new Header("parsley.causes", new byte[0]))).build(),
                        "application headers may never impersonate causal metadata (SPEC Structural 5)");
        assertEquals(FailClosedException.Reason.RESERVED_HEADER_USED, e.reason(),
                "the refusal names its condition (SPEC Operational 6) and fails the step through the seam");
    }

    /** A topic colliding with a composed changelog name is refused before any broker contact. */
    @Test
    void topicCollidingWithAComposedChangelogNameIsRefusedBeforeAnyBrokerContact() {
        Process p = Process.named("p")
                .receives(channel("x-p-s-changelog"), (d, s) -> Effects.none())
                .stores(store("s"))
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("unreachable:1", "x").build(), p),
                "a declared topic equal to a composed store-changelog name is refused at"
                        + " declaration time, even without the reserved namespace in it");
    }

    /** Distinct processes composing one changelog topic name are refused. */
    @Test
    void composedChangelogNameCollisionAcrossProcessesIsRefused() {
        Process p1 = Process.named("orders")
                .receives(channel("in1"), (d, s) -> Effects.none())
                .stores(store("audit-log"))
                .build();
        Process p2 = Process.named("orders-audit")
                .receives(channel("in2"), (d, s) -> Effects.none())
                .stores(store("log"))
                .build();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("unreachable:1", "app").build(), p1, p2),
                "\"app-orders\" + \"audit-log\" and \"app-orders-audit\" + \"log\" compose the"
                        + " same changelog topic; silently deduping it would have two Streams"
                        + " applications restoring each other's records");
        assertTrue(e.getMessage().contains("processes orders and orders-audit"),
                "the refusal names both colliding processes — \"orders\" alone is a substring"
                        + " of \"orders-audit\", so the pin must require the owner's own"
                        + " mention: " + e.getMessage());
    }

    /**
     * Two processes sharing one name would compose the same application id and therefore
     * the same consumer group and changelog topics, each restoring the other's records —
     * the identical-name degenerate of the composition collision
     * {@link #composedChangelogNameCollisionAcrossProcessesIsRefused} pins. The
     * duplicate is refused by name as the first statement of start, before any broker
     * contact, which is why the unreachable bootstrap never matters here.
     */
    @Test
    void duplicateProcessNamesAreRefusedBeforeAnyBrokerContact() {
        Process p1 = Process.named("orders")
                .receives(channel("in1"), (d, s) -> Effects.none())
                .build();
        Process p2 = Process.named("orders")
                .receives(channel("in2"), (d, s) -> Effects.none())
                .build();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("unreachable:1", "app").build(), p1, p2),
                "two processes named \"orders\" would run two Streams applications under one"
                        + " application id, sharing a consumer group and changelog topics");
        assertTrue(e.getMessage().contains("duplicate process name"),
                "the refusal names its condition, distinct from the composed-collision"
                        + " message: " + e.getMessage());
        assertTrue(e.getMessage().contains("orders"),
                "the refusal names the duplicated process so the operator knows which"
                        + " declaration to fix: " + e.getMessage());
    }

    /**
     * The varargs signature makes {@code Parsley.start(config)} compile with zero
     * processes; it must refuse at the entry point rather than return a handle owning
     * nothing, whose {@code healthy()} would be vacuously true forever and whose
     * {@code status()} would break its own never-empty promise. Null array and null
     * elements are pinned separately ({@link #nullProcessArrayIsRefusedByStart}); this is
     * the empty case, refused before any broker contact.
     */
    @Test
    void startWithZeroProcessesIsRefused() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("unreachable:1", "app").build()),
                "an empty start would otherwise return a runtime that reports healthy while"
                        + " running nothing");
        assertTrue(e.getMessage().contains("at least one process"),
                "the refusal says what is missing: " + e.getMessage());
    }

    /** Guarantee bearing configuration is unoverridable. */
    @Test
    void guaranteeBearingConfigurationIsUnoverridable() {
        ParsleyConfig.Builder builder = ParsleyConfig.builder("broker:9092", "app");
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("processing.guarantee", "at_least_once"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("consumer.isolation.level", "read_uncommitted"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.auto.offset.reset", "latest"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("group.id", "other"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("processing.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.production.exception.handler", "continue"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.deserialization.exception.handler", "continue"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("producer.interceptor.classes", "com.example.HeaderStripper"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.interceptor.classes", "com.example.Interceptor"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("default.timestamp.extractor", "LogAndSkipOnInvalidTimestamp"));

        // The membership protocol selects the fencing semantics the bootstrap's
        // initial-position commit is argued on; swapping it is guarantee-bearing.
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("group.protocol", "consumer"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.group.protocol", "consumer"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("group.remote.assignor", "uniform"));

        // Streams pins the plain bootstrap.servers from its own config but applies
        // prefixed consumer overrides on top without re-pinning, so a prefixed spelling
        // would point a consumer at a different cluster than the one start() resolved
        // topic identities against.
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("main.consumer.bootstrap.servers", "elsewhere:9092"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("restore.consumer.bootstrap.servers", "elsewhere:9092"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.streamsProperty("bootstrap.servers", "elsewhere:9092"));
    }

    /** Processes must receive something. */
    @Test
    void processesMustReceiveSomething() {
        assertThrows(IllegalArgumentException.class, () -> Process.named("p").build(),
                "a process with no received channels can never deliver");
    }

    /** Null serdes are refused at declaration, not at first use on the stream thread. */
    @Test
    void nullSerdesAreRefusedAtDeclaration() {
        assertThrows(IllegalArgumentException.class, () -> Topic.of("t", null, Serdes.String()),
                "a null key serde would otherwise surface as an NPE on the stream thread");
        assertThrows(IllegalArgumentException.class, () -> Topic.of("t", Serdes.String(), null),
                "a null value serde would otherwise surface as an NPE on the stream thread");
        assertThrows(IllegalArgumentException.class, () -> Store.of("s", null, Serdes.String()),
                "a null store key serde would otherwise surface at the first state access");
        assertThrows(IllegalArgumentException.class, () -> Store.of("s", Serdes.String(), null),
                "a null store value serde would otherwise surface at the first state access");
    }

    /** A null starting position is refused rather than silently meaning LATEST. */
    @Test
    void nullStartingPositionIsRefusedNotDefaulted() {
        assertThrows(IllegalArgumentException.class, () -> channel("t").startingAt(null),
                "null compared unequal to EARLIEST at commit time, which would silently skip"
                        + " every retained message");
    }

    /** A send topic declared through two different channel instances is refused. */
    @Test
    void sendTopicDeclaredThroughTwoInstancesIsRefused() {
        Topic<String, String> declared = channel("out");
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none())
                .sends(declared);
        assertThrows(IllegalArgumentException.class, () -> builder.sends(channel("out")),
                "two instances for one topic leave it ambiguous which declared serdes the"
                        + " topic's sends carry");
        assertEquals(List.of(declared), builder.sends(declared).build().outputs(),
                "a repeat of the declared instance itself stays idempotent");
    }

    /** A refused sends call commits none of its arguments. */
    @Test
    void refusedSendsCallCommitsNothing() {
        Topic<String, String> other = channel("other-out");
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none())
                .sends(channel("out"));
        assertThrows(IllegalArgumentException.class, () -> builder.sends(other, channel("out")),
                "the look-alike of the declared channel is refused");
        assertEquals(List.of("out"), builder.build().outputs().stream().map(Topic::name).toList(),
                "sends(...) is all-or-nothing: a refusal mid-list must not leave earlier"
                        + " arguments committed");
    }

    /** A null send channel is refused at construction. */
    @Test
    void nullSendChannelIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(null, "k", "v"),
                "a null channel would otherwise fail at commit time inside the step");
    }

    /** A null headers list is refused at construction. */
    @Test
    void nullHeadersListIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(channel("t"), "k", "v", null),
                "null headers would otherwise fail as a bare NPE in the copy");
    }

    /** A null element among effect headers is refused with a message. */
    @Test
    void nullElementAmongEffectHeadersIsRefusedWithAMessage() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(channel("t"), "k", "v",
                        Arrays.asList(new Header("h", new byte[0]), null)),
                "the javadoc promises IllegalArgumentException; List.copyOf would throw a bare"
                        + " NPE before the reserved-prefix loop ran");
        assertTrue(e.getMessage().contains("null element"), "the refusal names the mistake: " + e.getMessage());
    }

    /** A null store on put is refused at construction. */
    @Test
    void nullPutStoreIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().put(null, "k", "v"),
                "a null store would otherwise fail at commit time inside the step");
    }

    /** A null store on delete is refused at construction. */
    @Test
    void nullDeleteStoreIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().delete(null, "k"),
                "a null store would otherwise fail at commit time inside the step");
    }

    /** A null state write key on put is refused at construction. */
    @Test
    void nullPutKeyIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().put(store("s"), null, "v"),
                "a null key serializes to null bytes and RocksDB throws \"key cannot be null\""
                        + " on the stream thread");
    }

    /** A null state write key on delete is refused at construction. */
    @Test
    void nullDeleteKeyIsRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().delete(store("s"), null),
                "delete passes a null value deliberately, but its key must still address an entry");
    }

    /**
     * A null value on put is refused pointing at delete(): {@code Effects.Write}
     * accepts a null value deliberately, because null <em>is</em> delete()'s
     * representation and tombstones pass through the seam unencoded, so without
     * this guard {@code put(store, key, null)} constructs a Write byte-identical
     * to {@code delete(store, key)} — silently removing the entry the caller meant to
     * write instead of refusing the mistake (the declaration-site rule).
     */
    @Test
    void nullPutValueIsRefusedPointingAtDelete() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().put(store("s"), "k", null),
                "put with a null value would otherwise be indistinguishable from delete():"
                        + " a silent tombstone in place of the intended write");
        assertTrue(e.getMessage().contains("use delete() to remove a key"),
                "the refusal points at the API the caller meant: " + e.getMessage());
    }

    /** A null received channel is refused at declaration. */
    @Test
    void nullReceivedChannelIsRefusedAtDeclaration() {
        assertThrows(IllegalArgumentException.class,
                () -> Process.named("p").receives(null, (d, s) -> Effects.none()),
                "a null channel must fail at the declaration site, not as an NPE inside the builder");
    }

    /** A null handler is refused at declaration, not at first delivery. */
    @Test
    void nullHandlerIsRefusedAtDeclaration() {
        assertThrows(IllegalArgumentException.class,
                () -> Process.named("p").receives(channel("t"), null),
                "a null handler would otherwise surface as an NPE on the stream thread at first"
                        + " delivery — the exact failure mode the declaration-site rule eliminated for serdes");
    }

    /**
     * A second {@code receives} for one topic is refused naming the process and the
     * topic: the builder registers inputs with {@code putIfAbsent}, so without this
     * refusal the second declaration's handler would be dropped on the floor while the
     * first kept handling the topic — application logic silently never invoked, the
     * exact late-or-silent failure the declaration-site rule exists to prevent.
     */
    @Test
    void secondReceivesForOneTopicIsRefused() {
        Process.Builder builder = Process.named("shipper")
                .receives(channel("orders"), (d, s) -> Effects.none());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> builder.receives(channel("orders"), (d, s) -> Effects.none()),
                "a second receives() for one topic would otherwise silently keep the first"
                        + " handler and discard the second");
        assertTrue(e.getMessage().contains("shipper already receives orders"),
                "the refusal names the process and the topic so the operator knows which"
                        + " declaration to remove: " + e.getMessage());
    }

    /**
     * A duplicate store name is refused naming the process and the store: without this
     * refusal {@code putIfAbsent} keeps the first {@code Store} instance, so both
     * declarations would silently alias one state store and one changelog topic while
     * the second declaration's serdes were never consulted (the declaration-site
     * rule; the cross-instance ambiguity mirrors the sends() case
     * {@link #sendTopicDeclaredThroughTwoInstancesIsRefused} pins).
     */
    @Test
    void duplicateStoreDeclarationIsRefused() {
        Process.Builder builder = Process.named("shipper")
                .receives(channel("orders"), (d, s) -> Effects.none())
                .stores(store("inventory"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> builder.stores(store("inventory")),
                "a duplicate store name would otherwise alias one state store and one"
                        + " changelog topic under two declarations");
        assertTrue(e.getMessage().contains("shipper already declares store inventory"),
                "the refusal names the process and the store: " + e.getMessage());
    }

    /** A null element among sends varargs is refused. */
    @Test
    void nullElementAmongSendsVarargsIsRefused() {
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none());
        assertThrows(IllegalArgumentException.class,
                () -> builder.sends(new Topic<?, ?>[] {null}),
                "a null element must be refused per the taxonomy, not surface as a bare NPE");
    }

    /** A null element among stores varargs is refused. */
    @Test
    void nullElementAmongStoresVarargsIsRefused() {
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none());
        assertThrows(IllegalArgumentException.class,
                () -> builder.stores(new Store<?, ?>[] {null}),
                "a null element must be refused per the taxonomy, not surface as a bare NPE");
    }

    /**
     * A null sends varargs array — reachable through an explicit cast or a propagated
     * null array variable — is refused with the taxonomy's exception and a message
     * naming the process, where the loop over the array would otherwise throw a bare
     * unattributed NPE (the one-rule taxonomy; the null-element case is pinned
     * separately by {@link #nullElementAmongSendsVarargsIsRefused}).
     */
    @Test
    void nullSendsChannelArrayIsRefused() {
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> builder.sends((Topic<?, ?>[]) null),
                "a null topic array must be refused per the taxonomy, not surface as the"
                        + " builder's bare NPE iterating it");
        assertTrue(e.getMessage().contains("p: sends requires a non-null topic array"),
                "the refusal names the process and the mistake: " + e.getMessage());
    }

    /**
     * A null stores varargs array is refused with the taxonomy's exception and a
     * message naming the process, mirroring {@link #nullSendsChannelArrayIsRefused}:
     * one rule for null components across the whole declaration surface.
     */
    @Test
    void nullStoresArrayIsRefused() {
        Process.Builder builder = Process.named("p")
                .receives(channel("in"), (d, s) -> Effects.none());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> builder.stores((Store<?, ?>[]) null),
                "a null store array must be refused per the taxonomy, not surface as the"
                        + " builder's bare NPE iterating it");
        assertTrue(e.getMessage().contains("p: stores requires a non-null store array"),
                "the refusal names the process and the mistake: " + e.getMessage());
    }

    /** A null streams property key is refused. */
    @Test
    void nullStreamsPropertyKeyIsRefused() {
        assertThrows(IllegalArgumentException.class,
                () -> ParsleyConfig.builder("broker:9092", "p").streamsProperty(null, "v"),
                "a null property key would otherwise NPE inside the deny-list check");
    }

    /** A null streams property value is refused naming its key. */
    @Test
    void nullStreamsPropertyValueIsRefusedNamingItsKey() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ParsleyConfig.builder("broker:9092", "p").streamsProperty("client.id", null),
                "a null value would otherwise survive to Map.copyOf in the constructor and throw"
                        + " an NPE with a null message naming no property");
        assertTrue(e.getMessage().contains("client.id"), "the refusal names the property: " + e.getMessage());
    }

    /**
     * Null and blank bootstrap servers are refused at the builder with one message
     * naming the parameter: a blank string would otherwise ride into every Streams
     * configuration and fail much later as a Kafka client ConfigException naming no
     * parsley declaration site (declaration mistakes fail where they are written).
     */
    @Test
    void blankBootstrapServersAreRefused() {
        for (String bad : new String[] {null, "", "   "}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ParsleyConfig.builder(bad, "app"),
                    "bootstrapServers " + (bad == null ? "null" : "\"" + bad + "\"")
                            + " must fail at the builder, not later inside the Kafka client");
            assertTrue(e.getMessage().contains("bootstrapServers must be non-blank"),
                    "the refusal names the parameter and the rule: " + e.getMessage());
        }
    }

    /**
     * A zero or negative metadata budget is refused naming the parameter: the budget
     * bounds the causal metadata every message may carry (enforced on receipt and
     * on send), so a non-positive bound would pass build() only to refuse the very
     * first send's metadata with a growth diagnosis when the actual mistake is a
     * declaration typo.
     */
    @Test
    void nonPositiveMetadataBudgetIsRefused() {
        for (int bad : new int[] {0, -1}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> ParsleyConfig.builder("broker:9092", "p").metadataBudgetBytes(bad),
                    "metadataBudgetBytes(" + bad + ") admits no metadata at all and must fail"
                            + " at the declaration, not at the first step");
            assertTrue(e.getMessage().contains("metadataBudgetBytes must be positive"),
                    "the refusal names the parameter and the rule: " + e.getMessage());
        }
    }

    /** A negative or null send timestamp is refused at construction. */
    @Test
    void negativeOrNullSendTimestampsAreRefusedAtConstruction() {
        Topic<String, String> out = channel("out");
        assertThrows(IllegalArgumentException.class,
                () -> Effects.builder().send(out, "k", "v", -1L),
                "a negative timestamp cannot be a record timestamp");
        assertThrows(IllegalArgumentException.class,
                () -> new Effects.Send<>(out, "k", "v", java.util.List.of(), null),
                "absence is expressed through the empty OptionalLong, never through null");
        assertEquals(java.util.OptionalLong.empty(),
                Effects.builder().send(out, "k", "v").build().sends().get(0).timestamp(),
                "a send without a timestamp inherits the delivered one");
        assertEquals(java.util.OptionalLong.of(7L),
                Effects.builder().send(out, "k", "v", 7L).build().sends().get(0).timestamp());
    }

    /** Null status components are refused at construction. */
    @Test
    void nullStatusComponentsAreRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessStatus(null, ProcessStatus.Lifecycle.RUNNING,
                        java.util.Optional.empty(), java.util.Optional.empty()),
                "absence is expressed through the empty Optionals, never through null");
    }

    /**
     * Blank names are refused at declaration. Kafka's own topic-name rule is left to Kafka:
     * a malformed declared topic cannot exist, so resolution refuses the start, and a
     * malformed store, process or prefix fails changelog creation inside Streams.
     */
    @Test
    void blankNamesAreRefusedAtDeclaration() {
        assertThrows(IllegalArgumentException.class, () -> channel(" "), "a blank topic names nothing");
        assertThrows(IllegalArgumentException.class, () -> channel(null), "a null topic names nothing");
        assertThrows(IllegalArgumentException.class, () -> store(" "), "a blank store name names nothing");
        assertThrows(IllegalArgumentException.class, () -> Process.named(" "),
                "a blank process name would compose an application id with nothing to identify it");
        assertThrows(IllegalArgumentException.class, () -> ParsleyConfig.builder("broker:9092", " "),
                "a blank prefix would compose an application id with nothing to identify it");
    }

    /** A null config is refused by start with a message. */
    @Test
    void nullConfigIsRefusedByStart() {
        Process p = Process.named("p")
                .receives(channel("t"), (d, s) -> Effects.none())
                .build();
        assertThrows(IllegalArgumentException.class, () -> Parsley.start(null, p),
                "a null config must fail at the entry point per the taxonomy, not NPE inside"
                        + " the runtime's name validation");
    }

    /** A null process array is refused by start with a message. */
    @Test
    void nullProcessArrayIsRefusedByStart() {
        assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("broker:9092", "p").build(),
                        (Process[]) null),
                "a null varargs array must be refused per the taxonomy, not surface as"
                        + " List.of's bare NPE");
    }

    /** A null process element is refused by start with a message. */
    @Test
    void nullProcessElementIsRefusedByStart() {
        Process p = Process.named("p")
                .receives(channel("t"), (d, s) -> Effects.none())
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> Parsley.start(ParsleyConfig.builder("broker:9092", "p").build(), p, null),
                "a null element must be refused per the taxonomy, not surface as List.of's"
                        + " bare NPE");
    }

    /** Process names may not contain the reserved namespace. */
    @Test
    void processNamesMayNotContainTheReservedNamespace() {
        assertThrows(IllegalArgumentException.class, () -> Process.named("x__parsley.y"),
                "a process name containing __parsley. mints application ids, consumer groups"
                        + " and changelog topics inside parsley's own namespace — the namespace"
                        + " rule must not depend on which component carries the occurrence");
    }

    /** The application id prefix may not contain the reserved namespace. */
    @Test
    void applicationIdPrefixMayNotContainTheReservedNamespace() {
        assertThrows(IllegalArgumentException.class,
                () -> ParsleyConfig.builder("broker:9092", "x__parsley.y"),
                "a prefix containing __parsley. mints application ids and changelog topics"
                        + " inside parsley's own namespace");
    }

    /** Declaration order is preserved across the definition. */
    @Test
    void declarationOrderIsPreservedAcrossTheDefinition() {
        Process.Builder builder = Process.named("p");
        List<String> received = new java.util.ArrayList<>();
        List<String> sent = new java.util.ArrayList<>();
        List<String> stored = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            String in = "in-" + i;
            String out = "out-" + i;
            String store = "store-" + i;
            builder.receives(channel(in), (d, s) -> Effects.none())
                    .sends(channel(out))
                    .stores(store(store));
            received.add(in);
            sent.add(out);
            stored.add(store);
        }
        Process definition = builder.build();
        assertEquals(received, definition.inputs().stream().map(input -> input.topic().name()).toList(),
                "inputs() feeds the topology's sources array; a per-JVM iteration order"
                        + " makes the generated topology nondeterministic across restarts");
        assertEquals(sent, definition.outputs().stream().map(Topic::name).toList(),
                "outputs() feeds the topology's sinks in declaration order");
        assertEquals(stored, definition.stores().stream().map(Store::name).toList(),
                "stores() feeds addStateStore ordering and composed changelog names in"
                        + " declaration order");
    }
}
