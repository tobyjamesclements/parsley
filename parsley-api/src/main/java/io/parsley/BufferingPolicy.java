package io.parsley;

/**
 * Defines what happens to a record when its {@link BufferLimit} fires.
 *
 * <p>{@code BufferingPolicy} is a sealed interface with three variants. Each variant pairs a
 * {@link BufferLimit} (the <em>when</em>) with an eviction strategy (the <em>what</em>):
 *
 * <ul>
 *   <li>{@link Ignore} — forward the evicted record downstream and report a
 *       {@link CausalViolationReason#LIMIT_REACHED} violation via the violation handler
 *   <li>{@link Drop} — silently discard the record and report a violation
 *   <li>{@link DeadLetter} — route the record to a named dead-letter destination and report
 *       a violation
 * </ul>
 *
 * <p>Use the static factory methods to construct instances:
 * {@link #ignore}, {@link #drop}, {@link #deadLetter}.
 */
public sealed interface BufferingPolicy permits
        BufferingPolicy.Ignore,
        BufferingPolicy.Drop,
        BufferingPolicy.DeadLetter {

    /**
     * Creates an {@link Ignore} policy: forward evicted records out-of-order with a violation.
     *
     * @param limit the eviction trigger
     * @return a new {@code Ignore} policy
     */
    static BufferingPolicy ignore(BufferLimit limit) {
        return new Ignore(limit);
    }

    /**
     * Creates a {@link Drop} policy: discard evicted records with a violation.
     *
     * @param limit the eviction trigger
     * @return a new {@code Drop} policy
     */
    static BufferingPolicy drop(BufferLimit limit) {
        return new Drop(limit);
    }

    /**
     * Creates a {@link DeadLetter} policy: route evicted records to {@code destination} with
     * a violation.
     *
     * @param limit       the eviction trigger
     * @param destination the dead-letter destination name (e.g. a Kafka topic or
     *                    Kinesis stream)
     * @return a new {@code DeadLetter} policy
     */
    static BufferingPolicy deadLetter(BufferLimit limit, String destination) {
        return new DeadLetter(limit, destination);
    }

    /**
     * Forwards the evicted record downstream out-of-causal-order and reports a
     * {@link CausalViolationReason#LIMIT_REACHED} violation.
     *
     * <p>Choose this policy when message loss is unacceptable and occasional causal disorder
     * is preferable to dropping data.
     *
     * @param limit the limit that triggers eviction
     */
    record Ignore(BufferLimit limit) implements BufferingPolicy { }

    /**
     * Discards the evicted record and reports a {@link CausalViolationReason#LIMIT_REACHED}
     * violation. The record is never forwarded downstream.
     *
     * <p>Choose this policy when causal ordering is strictly required and message loss is
     * acceptable.
     *
     * @param limit the limit that triggers eviction
     */
    record Drop(BufferLimit limit) implements BufferingPolicy { }

    /**
     * Routes the evicted record to {@code destination} (the dead-letter destination) and
     * reports a {@link CausalViolationReason#LIMIT_REACHED} violation. The record is not
     * forwarded on the primary stream.
     *
     * <p>Broker adapters that use this policy require a dead-letter sink to perform the
     * routing — see the adapter's documentation.
     *
     * @param limit       the limit that triggers eviction
     * @param destination the destination name to route evicted records to (e.g. a Kafka
     *                    topic or Kinesis stream)
     */
    record DeadLetter(BufferLimit limit, String destination) implements BufferingPolicy { }
}
