package io.github.tobyjamesclements.parsley;

import java.time.Duration;
import java.util.List;

/**
 * The narrow seam over the single-partition {@code epoch-events} log — pure I/O, no protocol knowledge.
 * The leaderless epoch protocol needs two capabilities a Kafka Streams {@link
 * org.apache.kafka.streams.processor.api.Processor} cannot offer (its only egress is {@code
 * context.forward(...)} to wired children): <em>append</em> an event to the shared log, and <em>read the
 * whole log in total order</em> on every node so the deterministic {@link ParsleyEpochLog} fold agrees
 * everywhere. This interface is exactly those two operations; {@link ParsleyEpochRuntime} owns the fold,
 * the round-owner logic, and the poll loop that pumps this seam.
 *
 * <p>Keeping the read a caller-driven {@link #poll} (rather than an internal thread or callback) lets the
 * runtime own the single background thread and lets a test double stay a trivially synchronous in-memory
 * list. The concrete broker implementation is {@link ParsleyKafkaEpochTransport}; tests use a hand-rolled
 * in-memory double.
 *
 * <p>Not required to be thread-safe: a single runtime thread appends and polls one transport.
 */
interface ParsleyEpochTransport extends AutoCloseable {

    /**
     * Appends {@code event} to the log, returning once it is durably acknowledged. The log is
     * single-partition, so appends are totally ordered by offset; the protocol relies on this order, not
     * on any record key. Appends are idempotent at the protocol level (dedup by {@code epochId} for
     * commits; a re-observed join/request/publication folds to the same state).
     */
    void append(EpochEvent event);

    /**
     * Returns events appended since the previous {@code poll}, in log order, waiting up to {@code
     * timeout} for at least one to become available (an empty list if none did). Advances an internal
     * cursor, so each event is returned exactly once to this transport instance — the runtime folds the
     * returned events in order. A fresh transport begins at the log's start, so the first polls replay
     * the entire history (the log is low-volume, transition-only, and never offset-committed, so every
     * node deterministically folds from the beginning).
     */
    List<EpochEvent> poll(Duration timeout);

    /** Releases the underlying clients / resources. */
    @Override
    void close();
}
