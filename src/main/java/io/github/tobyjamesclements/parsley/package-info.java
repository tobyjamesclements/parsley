/**
 * The declaration surface and the Kafka Streams runtime behind it.
 *
 * <p>An application declares {@link io.github.tobyjamesclements.parsley.Channel} typed
 * topics, {@link io.github.tobyjamesclements.parsley.Store} typed stores, and one
 * {@link io.github.tobyjamesclements.parsley.Process} per process. Logic is a
 * {@link io.github.tobyjamesclements.parsley.Handler}, which receives a
 * {@link io.github.tobyjamesclements.parsley.Delivery} and returns
 * {@link io.github.tobyjamesclements.parsley.Effects}.
 *
 * <p>A handler is given no producer, no timer and no clock. Everything it changes travels
 * back through its return value, which is what allows state, sends and read positions to
 * commit in one transaction.
 *
 * <p>One declared process becomes one Kafka Streams application. Sources and sinks carry raw
 * bytes, and application serdes are applied inside the processor after the delivery decision,
 * so a payload that fails to decode cannot bypass the gate. The runtime that does this is
 * package-private, and no documented operation runs the topology without exactly-once
 * semantics.
 *
 * <p>Topic identity and partition width are resolved at start, and identity is checked again
 * at each task initialisation. Positions are meaningful only against the log they were
 * assigned in, so a topic recreated under a name a process has state for is a reason to
 * refuse. Nothing is asked of the broker between deliveries. A cause names the position of a
 * committed record, and receiving that record is what satisfies it (D115).
 *
 * <p>Configuration carrying the guarantee is fixed by the runtime and cannot be overridden:
 * {@code exactly_once_v2}, {@code read_committed}, and no automatic offset reset.
 *
 * @see io.github.tobyjamesclements.parsley.Parsley#start
 */
package io.github.tobyjamesclements.parsley;
