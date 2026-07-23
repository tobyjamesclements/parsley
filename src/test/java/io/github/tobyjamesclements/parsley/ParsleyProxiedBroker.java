package io.github.tobyjamesclements.parsley;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.jspecify.annotations.Nullable;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A real Kafka broker whose client traffic is routed, in its entirety, through a Toxiproxy so a
 * test can stall or sever the client-broker path mid-run. The broker gets an extra listener whose
 * <em>advertised</em> address is the proxy's external endpoint, so metadata responses point clients
 * back at the proxy — every connection made from {@link #proxiedBootstrap()} (bootstrap, metadata
 * refresh, produce, fetch, coordinator, transactions) traverses the proxy, not just the initial
 * bootstrap hop. {@link #directBootstrap()} is the broker's ordinary external listener, untouched
 * by any toxic, for the test's own producers/consumers/admin.
 *
 * <p>This class is the narrow seam over the wide toxiproxy client API (the repo routes wide
 * third-party interfaces through narrow Parsley seams): tests get the two bootstrap strings and
 * four fault verbs — {@link #stallAllTraffic()} / {@link #restoreTraffic()} (connections stay open
 * but no bytes flow, so acks stall indefinitely) and {@link #severConnections()} /
 * {@link #restoreConnections()} (connections are closed and refused, a broker outage) — and never
 * touch a proxy or toxic handle directly. Both restore verbs are idempotent so they are safe in
 * {@code finally} blocks.
 *
 * <p>Implements {@link Startable} so a single {@code @Container} field manages the network, the
 * proxy container, and the broker container as one unit; the proxy starts first because the
 * broker's advertised listener address is resolved from the proxy's mapped port at broker start.
 */
final class ParsleyProxiedBroker implements Startable {

    private static final String BROKER_ALIAS = "kafka";
    /** The broker-side listener the proxy forwards to, reachable only inside the Docker network. */
    private static final String PROXIED_LISTENER = BROKER_ALIAS + ":19092";
    /** The port toxiproxy listens on inside its container; its mapped port is the client-facing endpoint. */
    private static final int PROXY_LISTEN_PORT = 8666;

    private final Network network = Network.newNetwork();

    private final ToxiproxyContainer toxiproxy =
            new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                    .withNetwork(network);

    private final KafkaContainer kafka =
            new KafkaContainer(ParsleyBrokerImage.get())
                    .withNetwork(network)
                    .withNetworkAliases(BROKER_ALIAS)
                    .withListener(PROXIED_LISTENER, this::proxiedBootstrap);

    private @Nullable Proxy proxy;

    @Override
    public void start() {
        toxiproxy.start();
        try {
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            proxy = client.createProxy(BROKER_ALIAS, "0.0.0.0:" + PROXY_LISTEN_PORT, PROXIED_LISTENER);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create the toxiproxy route to " + PROXIED_LISTENER, e);
        }
        kafka.start();
    }

    @Override
    public void stop() {
        kafka.stop();
        toxiproxy.stop();
        network.close();
    }

    /**
     * The bootstrap address whose every connection traverses the proxy: the proxy's external
     * host:port, which is also what the broker advertises for the proxied listener.
     */
    String proxiedBootstrap() {
        return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PROXY_LISTEN_PORT);
    }

    /** The broker's ordinary external listener, bypassing the proxy — for the test's own clients. */
    String directBootstrap() {
        return kafka.getBootstrapServers();
    }

    /**
     * Stalls all proxied traffic indefinitely, in both directions, on existing and new
     * connections: connections stay open but no bytes flow, so in-flight produce requests never
     * receive their acknowledgements (a timeout toxic with {@code timeout=0} never closes the
     * connection — the truest form of "acks stalled").
     */
    void stallAllTraffic() {
        try {
            liveProxy().toxics().timeout("stall-upstream", ToxicDirection.UPSTREAM, 0);
            liveProxy().toxics().timeout("stall-downstream", ToxicDirection.DOWNSTREAM, 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to stall proxied broker traffic", e);
        }
    }

    /** Removes every active toxic, restoring healthy proxied traffic. Idempotent. */
    void restoreTraffic() {
        try {
            for (Toxic toxic : liveProxy().toxics().getAll()) {
                toxic.remove();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to restore proxied broker traffic", e);
        }
    }

    /**
     * Severs the proxied path as a broker outage would: existing connections are closed and new
     * ones refused until {@link #restoreConnections()}.
     */
    void severConnections() {
        try {
            liveProxy().disable();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to sever proxied broker connections", e);
        }
    }

    /** Reopens the proxied path after {@link #severConnections()}. Idempotent. */
    void restoreConnections() {
        try {
            liveProxy().enable();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to restore proxied broker connections", e);
        }
    }

    private Proxy liveProxy() {
        Proxy live = proxy;
        if (live == null) {
            throw new IllegalStateException("the proxied broker has not been started");
        }
        return live;
    }
}
