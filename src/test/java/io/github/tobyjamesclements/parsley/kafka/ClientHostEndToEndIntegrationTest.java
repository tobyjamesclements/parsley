package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.api.ParsleyConfig;

/**
 * The end-to-end contract, run against the kafka-clients host (D114): the same causal
 * chains, restarts, migrations, aborted-run reports, truncation and mid-step crash cases
 * the Streams host passes, on its own embedded broker.
 */
class ClientHostEndToEndIntegrationTest extends EndToEndIntegrationTest {
    @Override
    ParsleyConfig.Host host() {
        return ParsleyConfig.Host.KAFKA_CLIENTS;
    }
}
