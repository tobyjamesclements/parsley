/**
 * The declaration surface and the Kafka Streams runtime behind it.
 *
 * <p>An application declares {@link io.github.tobyjamesclements.parsley.Topic} typed
 * topics, {@link io.github.tobyjamesclements.parsley.Store} typed stores, and one
 * {@link io.github.tobyjamesclements.parsley.Process} per process. Logic is a
 * {@link io.github.tobyjamesclements.parsley.Handler}, which receives a
 * {@link io.github.tobyjamesclements.parsley.Delivery} and a
 * {@link io.github.tobyjamesclements.parsley.State}, and returns
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
 * <p>Fourteen types are public. Ten are the declaration surface named above, with
 * {@link io.github.tobyjamesclements.parsley.Parsley} and
 * {@link io.github.tobyjamesclements.parsley.ParsleyConfig} to run it and
 * {@link io.github.tobyjamesclements.parsley.ProcessStatus} to observe it. The other four
 * an application or an operator handles rather than declares:
 * {@link io.github.tobyjamesclements.parsley.Header} on a delivered or sent message,
 * {@link io.github.tobyjamesclements.parsley.FailClosedException} and its reason when a
 * process refuses, and
 * {@link io.github.tobyjamesclements.parsley.OrderingStateInspector} with
 * {@link io.github.tobyjamesclements.parsley.Channel} to read what a stopped process was
 * holding. Everything else is package-private.
 *
 * <p>Package-private types divide into two groups that must not be confused. The protocol
 * is host-free: the causal frontier, its wire codec, the hold-back buffer, the pure
 * deliverability decision and the engine over an ordering store. It names no clock, no
 * thread and no Kafka type, which is what lets a deterministic simulator drive the real
 * engine with no broker. The runtime is everything that touches Kafka Streams. A test
 * scans this package and holds the line between them.
 *
 * @see io.github.tobyjamesclements.parsley.Parsley#start
 */
package io.github.tobyjamesclements.parsley;
