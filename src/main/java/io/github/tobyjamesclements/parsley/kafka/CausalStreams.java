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
 *   <li>installs the position-capturing client supplier ({@link Positions}) — the consumer's
 *       position advance past transaction markers is the protocol's liveness signal;</li>
 *   <li>resolves topic identity and sink end offsets through an admin client built from the
 *       same properties. Own outputs are claimed in sequence space (see
 *       {@link AdminSendTracker}), so no acknowledgement capture is wired.</li>
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

        Map<String, Object> adminConfig = new HashMap<>();
        for (String name : p.stringPropertyNames()) {
            adminConfig.put(name, p.getProperty(name));
        }
        Admin admin = Admin.create(Map.of("bootstrap.servers",
                adminConfig.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)));

        stage.wire(
                TopicIds.fromAdmin(admin),
                (clientId, ids, sinkTopics) -> new AdminSendTracker(ids, admin, sinkTopics));

        KafkaStreams ks = new KafkaStreams(stage.topology(), p, Positions.capturingClientSupplier());
        ks.start();
        return new CausalStreams(ks, admin);
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
