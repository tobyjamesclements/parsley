package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Worked example — the <b>Commander</b> architecture (Bobby Calderwood, "Commander: Better
 * Distributed Applications through CQRS and Event Sourcing", 2015) on Parsley. Clients write
 * immutable commands; a commander stage validates each command against per-key state and emits
 * events — facts, including rejections — to an event log; projectors fold the event log into
 * read models. The write side never mutates a read model directly.
 *
 * <p>What causal delivery adds to the pattern is the ordering the pattern otherwise leaves to
 * luck. A projection derives from an event, which derives from a command, so the projection
 * transitively claims the command; any consumer of both a command topic and a read model
 * therefore delivers the command first, and read-your-writes across the CQRS split becomes a
 * topology property rather than a polling loop. That is
 * {@code commandsPrecedeTheirProjectionsAtASharedConsumer} below, demonstrated with the gate
 * actually holding a record.
 *
 * <p>Documented in the guide at {@code docs/guide/examples.md}; the shape it composes is the
 * base-and-derived triangle of {@code docs/guide/topologies.md#diamond}.
 */
class ExampleCommanderTest {

    // ---------------------------------------------------------------- domain

    /** A client's intent. Immutable, and never a fact: the commander may reject it. */
    record Command(String type, long amount) {}

    /** A fact the commander decided. A rejection is an event too, not an exception. */
    record Event(String type, long amount, String reason) {}

    /** The commander's write-side state per account. */
    record Account(boolean open, long balance) {}

    /** A read model: what a query would answer. */
    record View(long balance, int applied) {}

    private static <T> Codec<T> pipeCodec(java.util.function.Function<T, String> to,
                                          java.util.function.Function<String[], T> from) {
        return Codec.of(v -> to.apply(v).getBytes(StandardCharsets.UTF_8),
                b -> from.apply(new String(b, StandardCharsets.UTF_8).split("\\|", -1)));
    }

    private static final Codec<Command> COMMAND = pipeCodec(
            c -> c.type() + "|" + c.amount(),
            p -> new Command(p[0], Long.parseLong(p[1])));
    private static final Codec<Event> EVENT = pipeCodec(
            e -> e.type() + "|" + e.amount() + "|" + e.reason(),
            p -> new Event(p[0], Long.parseLong(p[1]), p[2]));
    private static final Codec<Account> ACCOUNT = pipeCodec(
            a -> a.open() + "|" + a.balance(),
            p -> new Account(Boolean.parseBoolean(p[0]), Long.parseLong(p[1])));
    private static final Codec<View> VIEW = pipeCodec(
            v -> v.balance() + "|" + v.applied(),
            p -> new View(Long.parseLong(p[0]), Integer.parseInt(p[1])));

    private static final Topic<String, Command> COMMANDS =
            Topic.of("commands", Codec.utf8(), COMMAND);
    private static final Topic<String, Event> EVENTS = Topic.of("events", Codec.utf8(), EVENT);
    private static final Topic<String, View> VIEWS = Topic.of("views", Codec.utf8(), VIEW);
    private static final Topic<String, String> AUDIT =
            Topic.of("audit", Codec.utf8(), Codec.utf8());

    // ---------------------------------------------------------------- stages

    /**
     * The write side: a pure decision function from (state, command) to the next state and the
     * events it produced. Invalid commands yield a rejection event and leave the state
     * untouched — a domain failure is a fact, not a task failure
     * ({@code docs/guide/error-handling.md}).
     */
    private static Stage commander() {
        return Stage.named("commander")
                .state(ACCOUNT, () -> new Account(false, 0))
                .on(COMMANDS, (Account acct, Message<String, Command> m) -> {
                    Command c = m.value();
                    if (!acct.open() && !c.type().equals("open")) {
                        return Step.of(acct, EVENTS.send(m.key(),
                                new Event("rejected", c.amount(), "account-not-open")));
                    }
                    return switch (c.type()) {
                        case "open" -> acct.open()
                                ? Step.of(acct, EVENTS.send(m.key(),
                                        new Event("rejected", 0, "already-open")))
                                : Step.of(new Account(true, 0),
                                        EVENTS.send(m.key(), new Event("opened", 0, "")));
                        case "deposit" -> Step.of(new Account(true, acct.balance() + c.amount()),
                                EVENTS.send(m.key(), new Event("deposited", c.amount(), "")));
                        case "withdraw" -> c.amount() > acct.balance()
                                ? Step.of(acct, EVENTS.send(m.key(),
                                        new Event("rejected", c.amount(), "insufficient-funds")))
                                : Step.of(new Account(true, acct.balance() - c.amount()),
                                        EVENTS.send(m.key(), new Event("withdrawn", c.amount(), "")));
                        default -> Step.of(acct, EVENTS.send(m.key(),
                                new Event("rejected", c.amount(), "unknown-command")));
                    };
                })
                .into(EVENTS)
                .build();
    }

    /**
     * The read side: a fold over the event log alone. It never sees a command, which is what
     * makes the read model rebuildable — replaying the events reconstructs it exactly
     * ({@code viewsRebuildByReplayingTheEventLog}). It publishes only when the model actually
     * changes, so a rejection costs a fold and no downstream traffic.
     */
    private static Stage projector() {
        return Stage.named("projector")
                .state(VIEW, () -> new View(0, 0))
                .on(EVENTS, (View view, Message<String, Event> m) -> {
                    Event e = m.value();
                    View next = switch (e.type()) {
                        case "deposited" -> new View(view.balance() + e.amount(), view.applied() + 1);
                        case "withdrawn" -> new View(view.balance() - e.amount(), view.applied() + 1);
                        case "opened" -> new View(0, view.applied() + 1);
                        default -> view; // a rejection changes no balance
                    };
                    return next.equals(view) ? Step.of(next) : Step.of(next, VIEWS.send(m.key(), next));
                })
                .into(VIEWS)
                .build();
    }

    /**
     * A separate application that consumes both the command log and the read model — a
     * reconciler, an auditor, or any service that must never see a projection before the
     * command behind it. Kafka Streams allows one source node per topic, so a second consumer
     * of {@code commands} is its own application, exactly as
     * {@code docs/guide/topologies.md#fan-out} describes.
     */
    private static Stage reconciler() {
        return Stage.named("reconciler")
                .on(COMMANDS, m -> List.of(AUDIT.send(m.key(), "command:" + m.value().type())))
                .on(VIEWS, m -> List.of(AUDIT.send(m.key(), "view:" + m.value().balance())))
                .into(AUDIT)
                .build();
    }

    // ---------------------------------------------------------------- wiring

    @TempDir
    Path stateDir;

    private Properties props(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(appId).toString());
        return props;
    }

    private static TestInputTopic<byte[], byte[]> in(TopologyTestDriver d, String topic) {
        return d.createInputTopic(topic, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private static TestOutputTopic<byte[], byte[]> out(TopologyTestDriver d, String topic) {
        return d.createOutputTopic(topic, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /** Sends one client command; commands from plain clients carry no stamp and claim nothing. */
    private static void command(TestInputTopic<byte[], byte[]> commands, String key, Command c) {
        commands.pipeInput(Codec.utf8().encode(key), COMMAND.encode(c));
    }

    // ---------------------------------------------------------------- tests

    /**
     * The pattern end to end: commands become events, events become a projection. The event
     * log is the system of record, and the view is a fold of it.
     */
    @Test
    void commandsBecomeEventsAndEventsBecomeAProjection() {
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(commander(), projector()).testTopology(), props("commander-app"))) {
            TestInputTopic<byte[], byte[]> commands = in(app, "commands");
            command(commands, "acct-1", new Command("open", 0));
            command(commands, "acct-1", new Command("deposit", 100));
            command(commands, "acct-1", new Command("withdraw", 30));

            assertEquals(List.of(new Event("opened", 0, ""),
                            new Event("deposited", 100, ""),
                            new Event("withdrawn", 30, "")),
                    out(app, "events").readValuesToList().stream().map(EVENT::decode).toList(),
                    "every accepted command must produce exactly one fact, in command order");
            assertEquals(List.of(new View(0, 1), new View(100, 2), new View(70, 3)),
                    out(app, "views").readValuesToList().stream().map(VIEW::decode).toList(),
                    "the read model must be the running fold of the event log");
        }
    }

    /**
     * A command the write side refuses is a rejection <em>event</em>: the state is untouched,
     * the projection ignores it, and the client learns the outcome from the same log it reads
     * everything else from. Nothing throws, so nothing is retried.
     */
    @Test
    void invalidCommandsAreRejectedAsFactsNotFailures() {
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(commander(), projector()).testTopology(), props("commander-reject"))) {
            TestInputTopic<byte[], byte[]> commands = in(app, "commands");
            command(commands, "acct-1", new Command("deposit", 10));   // not open yet
            command(commands, "acct-1", new Command("open", 0));
            command(commands, "acct-1", new Command("open", 0));       // already open
            command(commands, "acct-1", new Command("withdraw", 500)); // no funds

            assertEquals(List.of(new Event("rejected", 10, "account-not-open"),
                            new Event("opened", 0, ""),
                            new Event("rejected", 0, "already-open"),
                            new Event("rejected", 500, "insufficient-funds")),
                    out(app, "events").readValuesToList().stream().map(EVENT::decode).toList(),
                    "each rejection must be a fact naming its reason");
            assertEquals(List.of(new View(0, 1)),
                    out(app, "views").readValuesToList().stream().map(VIEW::decode).toList(),
                    "only the accepted command may move the read model");
        }
    }

    /**
     * The property that makes CQRS worth its split: a read model is derived, so it can be
     * dropped and rebuilt. A fresh projector replaying the captured event log reaches exactly
     * the view the original run reached — the fold is pure and the delivery order is the
     * log's own. This is also the late-joiner shape: a new consumer needs no coordination, it
     * just starts consuming.
     */
    @Test
    void viewsRebuildByReplayingTheEventLog() {
        List<TestRecord<byte[], byte[]>> eventLog;
        List<View> original;
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(commander(), projector()).testTopology(), props("commander-source"))) {
            TestInputTopic<byte[], byte[]> commands = in(app, "commands");
            command(commands, "acct-1", new Command("open", 0));
            command(commands, "acct-1", new Command("deposit", 40));
            command(commands, "acct-2", new Command("open", 0));
            command(commands, "acct-1", new Command("deposit", 2));
            eventLog = out(app, "events").readRecordsToList();
            original = out(app, "views").readValuesToList().stream().map(VIEW::decode).toList();
        }

        // A new deployment of the read side alone, fed the durable log — no commander, no
        // command topic, no coordination with the original run.
        try (TopologyTestDriver rebuild = new TopologyTestDriver(
                Parsley.of(projector()).testTopology(), props("commander-rebuild"))) {
            TestInputTopic<byte[], byte[]> events = in(rebuild, "events");
            eventLog.forEach(events::pipeInput);
            assertEquals(original,
                    out(rebuild, "views").readValuesToList().stream().map(VIEW::decode).toList(),
                    "replaying the event log must rebuild the identical read model");
        }
    }

    /**
     * Read-your-writes across the CQRS split, as an ordering property rather than a polling
     * loop. A projection transitively claims the command that caused it, so a consumer of both
     * the command log and the read model delivers the command first — even when the projection
     * reaches it first, which is exactly the race the pattern otherwise loses.
     *
     * <p>The out-of-order feed is the point: the view record is offered to the reconciler
     * before its command, the gate holds it (no output at all), and the arrival of the command
     * releases both in causal order. This is the gate doing its job, observable end to end.
     */
    @Test
    void commandsPrecedeTheirProjectionsAtASharedConsumer() {
        TestRecord<byte[], byte[]> projection;
        try (TopologyTestDriver app = new TopologyTestDriver(
                Parsley.of(commander(), projector()).testTopology(), props("commander-write"))) {
            command(in(app, "commands"), "acct-1", new Command("open", 0));
            List<TestRecord<byte[], byte[]>> views = out(app, "views").readRecordsToList();
            assertEquals(1, views.size(), "the open command must produce one projection");
            projection = views.get(0);
            assertTrue(CausalHeaders.read(projection.headers())
                            .get(Stage.testChannel("commands", 0)) >= 0,
                    "the projection's stamp must claim the command two hops upstream");
        }

        try (TopologyTestDriver consumer = new TopologyTestDriver(
                Parsley.of(reconciler()).testTopology(), props("commander-read"))) {
            TestOutputTopic<byte[], byte[]> audit = out(consumer, "audit");

            // The projection arrives first — the classic read-model-ahead-of-its-command race.
            in(consumer, "views").pipeInput(projection);
            assertTrue(audit.isEmpty(),
                    "the projection must be held: its cause has not been delivered here yet");

            // The command it derives from arrives, and both deliver, cause first.
            command(in(consumer, "commands"), "acct-1", new Command("open", 0));
            assertEquals(List.of("command:open", "view:0"),
                    audit.readValuesToList().stream().map(Codec.utf8()::decode).toList(),
                    "the held projection must be released after its command, never before");
        }
    }
}
