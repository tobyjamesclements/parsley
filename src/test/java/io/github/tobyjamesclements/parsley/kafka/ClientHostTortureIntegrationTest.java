package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.api.ParsleyConfig;

/** The host-torture contract, run against the kafka-clients host (D115). */
class ClientHostTortureIntegrationTest extends HostTortureIntegrationTest {
    @Override
    ParsleyConfig.Host host() {
        return ParsleyConfig.Host.KAFKA_CLIENTS;
    }
}
