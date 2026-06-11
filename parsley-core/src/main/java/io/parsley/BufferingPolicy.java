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
 *   <li>{@link DeadLetter} — route the record to a named dead-letter topic and report a violation
 * </ul>
 *
 * <p>Use the static factory methods to construct instances:
 * {@link #ignore}, {@link #drop}, {@link #deadLetter}.
 */
public sealed interface BufferingPolicy
        permits BufferingPolicy.Ignore, BufferingPolicy.Drop, BufferingPolicy.DeadLetter {

    /**
     * Forwards the evicted record downstream out-of-causal-order and reports a
     * {@link CausalViolationReason#LIMIT_REACHED} violation.
     *
     * <p>Choose this policy when message loss is unacceptable and occasional causal disorder
     * is preferable to dropping data.
     *
     * @param limit the limit that triggers eviction
     */
    record Ignore(BufferLimit limit) implements BufferingPolicy {}

    /**
     * Discards the evicted record and reports a {@link CausalViolationReason#LIMIT_REACHED}
     * violation. The record is never forwarded downstream.
     *
     * <p>Choose this policy when causal ordering is strictly required and message loss is
     * acceptable.
     *
     * @param limit the limit that triggers eviction
     */
    record Drop(BufferLimit limit) implements BufferingPolicy {}

    /**
     * Routes the evicted record to {@code topic} (the dead-letter topic) and reports a
     * {@link CausalViolationReason#LIMIT_REACHED} violation. The record is not forwarded
     * on the primary stream.
     *
     * <p>When using this policy with {@link io.parsley.streams.CausalProcessorSupplier}, use
     * the 4-argument constructor that accepts a dead-letter sink.
     *
     * @param limit the limit that triggers eviction
     * @param topic the Kafka topic name to route evicted records to
     */
    record DeadLetter(BufferLimit limit, String topic) implements BufferingPolicy {}

    /**
     * Creates an {@link Ignore} policy: forward evicted records out-of-order with a violation.
     *
     * @param limit the eviction trigger
     * @return a new {@code Ignore} policy
     */
    static BufferingPolicy ignore(BufferLimit limit) { return new Ignore(limit); }

    /**
     * Creates a {@link Drop} policy: discard evicted records with a violation.
     *
     * @param limit the eviction trigger
     * @return a new {@code Drop} policy
     */
    static BufferingPolicy drop(BufferLimit limit) { return new Drop(limit); }

    /**
     * Creates a {@link DeadLetter} policy: route evicted records to {@code topic} with a violation.
     *
     * @param limit the eviction trigger
     * @param topic the dead-letter Kafka topic name
     * @return a new {@code DeadLetter} policy
     */
    static BufferingPolicy deadLetter(BufferLimit limit, String topic) { return new DeadLetter(limit, topic); }
}
