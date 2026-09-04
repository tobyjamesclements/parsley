package io.github.tobyjamesclements.parsley.kafka;

import java.time.Duration;
import java.util.Map;

import io.github.tobyjamesclements.parsley.api.ProcessStatus;

/**
 * A running set of processes under one host, as {@code Parsley} drives it.
 *
 * <p>Two hosts implement it: {@link ParsleyRuntime} over Kafka Streams and
 * {@link ClientRuntime} over the plain kafka-clients APIs (D114). The public API sees
 * neither.
 */
public interface RuntimeHandle extends AutoCloseable {
    /** @return {@code true} while every process is running or rebalancing */
    boolean healthy();

    /** @return the current state of every process, keyed by name */
    Map<String, ProcessStatus> status();

    /** Waits until any process stops or this runtime closes. */
    void awaitStopped() throws InterruptedException;

    /** Waits, bounded, until any process stops or this runtime closes. */
    boolean awaitStopped(Duration timeout) throws InterruptedException;

    /** Stops every process and releases every resource. */
    @Override
    void close();
}
