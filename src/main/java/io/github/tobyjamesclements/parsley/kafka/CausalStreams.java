package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Production wiring and runtime for a {@link CausalStage}:
 *
 * <ul>
 *   <li>requires and enforces {@code processing.guarantee=exactly_once_v2} and
 *       {@code isolation.level=read_committed} — the protocol's transactional state and
 *       density adaptation are defined against them;</li>
 *   <li>registers {@link OwnOutputs.Interceptor} on the application's producers
 *       (acknowledgement capture for the own-outputs clock and the crossing wait);</li>
 *   <li>installs the position-capturing client supplier ({@link Positions}) — the consumer's
 *       position advance past transaction markers is the protocol's liveness signal;</li>
 *   <li>resolves topic identity and sink end offsets through an admin client built from the
 *       same properties.</li>
 * </ul>
 */
public final class CausalStreams implements AutoCloseable {

    private final KafkaStreams streams;
    private final Admin admin;

    private CausalStreams(KafkaStreams streams, Admin admin) {
        this.streams = streams;
        this.admin = admin;
    }

    public static CausalStreams start(CausalStage<?, ?, ?, ?> stage, Properties props) {
        Properties p = new Properties();
        p.putAll(props);
        Object guarantee = p.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
        if (guarantee != null && !StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee)) {
            throw new IllegalArgumentException("parsley requires processing.guarantee=exactly_once_v2");
        }
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");
        appendInterceptor(p);

        Map<String, Object> adminConfig = new HashMap<>();
        for (String name : p.stringPropertyNames()) {
            adminConfig.put(name, p.getProperty(name));
        }
        Admin admin = Admin.create(Map.of("bootstrap.servers",
                adminConfig.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)));

        stage.wire(
                TopicIds.fromAdmin(admin),
                (clientId, ids, sinkTopics) -> new InterceptorSendTracker(
                        clientId, ids, admin, sinkTopics, Duration.ofSeconds(30)));

        KafkaStreams ks = new KafkaStreams(stage.topology(), p, Positions.capturingClientSupplier());
        ks.start();
        return new CausalStreams(ks, admin);
    }

    private static void appendInterceptor(Properties p) {
        String key = StreamsConfig.producerPrefix("interceptor.classes");
        String existing = p.getProperty(key);
        String cls = OwnOutputs.Interceptor.class.getName();
        p.put(key, existing == null || existing.isBlank() ? cls : existing + "," + cls);
    }

    public KafkaStreams streams() {
        return streams;
    }

    @Override
    public void close() {
        streams.close();
        admin.close();
    }
}
