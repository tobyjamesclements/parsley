package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.github.tobyjamesclements.parsley.JepsenEdn.kw;
import static io.github.tobyjamesclements.parsley.JepsenEdn.map;

/**
 * The topology the Jepsen harness runs: fan-out, fan-in, a cycle and a self-channel, over
 * topics of equal width so every process has several tasks and causality crosses partitions.
 *
 * <pre>
 *   src ──▶ splitter ──▶ a ──▶ joiner ──▶ c ──▶ cycler ──▶ d ──▶ selfer ──▶ self ──▶ selfer
 *                   └──▶ b ──▶ joiner        ▲          └──▶ loop ──▶ joiner        (self-channel)
 *                                             └──────── (cycle) ────────┘
 * </pre>
 *
 * <p>Every handler forwards deterministically from the delivered value alone, so the effects
 * of a delivery are a pure function of {@code (process, topic, uid)}, which is what lets a
 * checker predict every send. Every value is its own uid: an external producer chooses one
 * and each forward appends {@code >process>topic}, so a record is matched to the delivery
 * that produced it by its uid, and an indeterminate send to the record it may have produced.
 * Keys are the uid too, so the default partitioner spreads each hop over the partitions.
 *
 * <p>Every delivery also sends one trace record to {@link #TRACE}, which no process receives.
 * The trace send is an effect of the step, so it commits atomically with the delivery: an
 * aborted step leaves no trace record. A trace record names the process, the task, the
 * delivered channel and offset, the uid, and the effects the step sent; its own
 * {@code parsley.causes} header is the task's expressed frontier at that step.
 */
final class JepsenTopology {
    static final String SRC = "src";
    static final String A = "a";
    static final String B = "b";
    static final String C = "c";
    static final String D = "d";
    static final String LOOP = "loop";
    static final String SELF = "self";
    static final String TRACE = "trace";

    static final List<String> TOPICS = List.of(SRC, A, B, C, D, LOOP, SELF, TRACE);

    static final String SPLITTER = "splitter";
    static final String JOINER = "joiner";
    static final String CYCLER = "cycler";
    static final String SELFER = "selfer";

    /** Hops after which nothing is forwarded, which bounds the cycle and the self-channel. */
    static final int MAX_HOPS = 6;

    private JepsenTopology() {
    }

    static Topic<String, String> topic(String name) {
        return Topic.of(name, Serdes.String(), Serdes.String());
    }

    /**
     * The declaration by topic name, with {@code dropped} topics left out of every
     * received set — the shape of the "declaration dropping a topic that holds messages"
     * fault.
     */
    static Map<String, JepsenExport.ProcessDecl> declaration(Set<String> dropped) {
        Map<String, JepsenExport.ProcessDecl> decl = new LinkedHashMap<>();
        decl.put(SPLITTER, new JepsenExport.ProcessDecl(SPLITTER, without(List.of(SRC), dropped), List.of(A, B, TRACE)));
        decl.put(JOINER, new JepsenExport.ProcessDecl(JOINER, without(List.of(A, B, LOOP), dropped), List.of(C, TRACE)));
        decl.put(CYCLER, new JepsenExport.ProcessDecl(CYCLER, without(List.of(C), dropped), List.of(D, LOOP, TRACE)));
        decl.put(SELFER, new JepsenExport.ProcessDecl(SELFER, without(List.of(D, SELF), dropped), List.of(SELF, TRACE)));
        return decl;
    }

    private static List<String> without(List<String> topics, Set<String> dropped) {
        List<String> kept = new ArrayList<>();
        for (String topic : topics) {
            if (!dropped.contains(topic)) {
                kept.add(topic);
            }
        }
        if (kept.isEmpty()) {
            throw new IllegalArgumentException("dropping " + dropped + " leaves a process receiving nothing");
        }
        return kept;
    }

    /** How many hops a uid has travelled: each forward appends {@code >process>topic}. */
    static int hops(String uid) {
        int count = 0;
        for (int i = 0; i < uid.length(); i++) {
            if (uid.charAt(i) == '>') {
                count++;
            }
        }
        return count / 2;
    }

    static String forwardedUid(String uid, String process, String topic) {
        return uid + ">" + process + ">" + topic;
    }

    /**
     * The forwarding rule: the effects of delivering {@code uid} on {@code topic} at
     * {@code process}, excluding the trace record. Deterministic in its arguments, and
     * bounded by {@link #MAX_HOPS}. {@code String.hashCode} is specified, so the choice is
     * the same in every JVM and in the checker.
     */
    static List<JepsenExport.Effect> effects(String process, String topic, String uid) {
        if (hops(uid) >= MAX_HOPS) {
            return List.of();
        }
        int hash = Math.floorMod(uid.hashCode(), 6);
        List<JepsenExport.Effect> effects = new ArrayList<>();
        switch (process) {
            case SPLITTER -> {
                effects.add(new JepsenExport.Effect(A, forwardedUid(uid, process, A)));
                effects.add(new JepsenExport.Effect(B, forwardedUid(uid, process, B)));
            }
            case JOINER -> effects.add(new JepsenExport.Effect(C, forwardedUid(uid, process, C)));
            case CYCLER -> {
                effects.add(new JepsenExport.Effect(D, forwardedUid(uid, process, D)));
                if (hash % 2 == 0) {
                    effects.add(new JepsenExport.Effect(LOOP, forwardedUid(uid, process, LOOP)));
                }
            }
            case SELFER -> {
                if (hash % 3 != 0) {
                    effects.add(new JepsenExport.Effect(SELF, forwardedUid(uid, process, SELF)));
                }
            }
            default -> throw new IllegalArgumentException("unknown process " + process);
        }
        return effects;
    }

    static String traceKey(String process, int partition) {
        return JepsenExport.taskName(process, partition);
    }

    /** The trace record's value: EDN naming the delivery and the effects of its step. */
    static String traceValue(String process, Delivery<String, String> delivery, List<JepsenExport.Effect> effects) {
        List<Object> fx = new ArrayList<>();
        for (JepsenExport.Effect effect : effects) {
            fx.add(List.of(effect.topic(), effect.uid()));
        }
        return JepsenEdn.write(map("p", process, "t", (long) delivery.partition(), "topic", delivery.topic().name(),
                "id", delivery.channel().topicId().toString(), "part", (long) delivery.partition(),
                "off", delivery.position(), "uid", delivery.value(), "fx", fx));
    }

    /** One decoded trace value. */
    record TraceValue(String process, int task, String topic, java.util.UUID topicId, int partition, long offset,
                      String uid, List<JepsenExport.Effect> effects) {
    }

    static TraceValue parseTraceValue(String value) {
        Object edn = JepsenEdn.read(value);
        List<JepsenExport.Effect> effects = new ArrayList<>();
        for (Object pair : JepsenEdn.asList(JepsenEdn.get(edn, "fx"))) {
            List<Object> items = JepsenEdn.asList(pair);
            effects.add(new JepsenExport.Effect(items.get(0).toString(), items.get(1).toString()));
        }
        return new TraceValue(JepsenEdn.string(edn, "p"), (int) JepsenEdn.integer(edn, "t"),
                JepsenEdn.string(edn, "topic"), java.util.UUID.fromString(JepsenEdn.string(edn, "id")),
                (int) JepsenEdn.integer(edn, "part"), JepsenEdn.integer(edn, "off"), JepsenEdn.string(edn, "uid"),
                effects);
    }

    /**
     * Builds the processes, each handler forwarding by {@link #effects} and sending its trace
     * record. Every process declares the trace topic as sent and none receives it.
     */
    static List<Process> processes(Set<String> dropped) {
        Map<String, Topic<String, String>> topics = new LinkedHashMap<>();
        for (String name : TOPICS) {
            topics.put(name, topic(name));
        }
        List<Process> processes = new ArrayList<>();
        declaration(dropped).forEach((name, decl) -> {
            Process.Builder builder = Process.named(name);
            for (String received : decl.receives()) {
                Topic<String, String> topic = topics.get(received);
                builder.receives(topic, (delivery, state) -> handle(name, topics, delivery));
            }
            Topic<?, ?>[] sends = decl.sends().stream().map(topics::get).toArray(Topic<?, ?>[]::new);
            builder.sends(sends);
            processes.add(builder.build());
        });
        return processes;
    }

    private static Effects handle(String process, Map<String, Topic<String, String>> topics,
                                  Delivery<String, String> delivery) {
        String uid = delivery.value();
        List<JepsenExport.Effect> effects = effects(process, delivery.topic().name(), uid);
        Effects.Builder builder = Effects.builder();
        for (JepsenExport.Effect effect : effects) {
            builder.send(topics.get(effect.topic()), effect.uid(), effect.uid());
        }
        builder.send(topics.get(TRACE), traceKey(process, delivery.partition()), traceValue(process, delivery, effects));
        return builder.build();
    }

    /** Creates every topic of the topology with the same width, waiting for the broker. */
    static void createTopics(Admin admin, int partitions, short replication, Map<String, String> configs)
            throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : TOPICS) {
            topics.add(new NewTopic(name, partitions, replication).configs(configs));
        }
        admin.createTopics(topics).all().get(60, TimeUnit.SECONDS);
    }}
