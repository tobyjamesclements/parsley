/**
 * Causal delivery order for Kafka Streams processors: the declaration surface an application
 * writes, and the Kafka Streams adapter that runs it.
 *
 * <p>An application declares {@link Channel} typed topics, {@link Store} typed stores, and one
 * {@link ProcessDefinition} per process. Logic is a {@link Handler}, which receives a
 * {@link Delivery} and returns {@link Effects}. A handler is given no producer, no timer and
 * no clock. Everything it changes travels back through its return value, which is what allows
 * state, sends and read positions to commit in one transaction.
 *
 * <p>The adapter is package-private beside it: {@link Parsley} is the only way in. One process
 * becomes one Kafka Streams application. Sources and sinks carry raw bytes, and application
 * serdes are applied inside the processor after the delivery decision, so a payload that fails
 * to decode cannot bypass the gate. Topic identity and partition width are resolved at start,
 * and identity is checked again at each task initialisation. Positions are meaningful only
 * against the log they were assigned in, so a topic recreated under a name a process has state
 * for is a reason to refuse. Nothing is asked of the broker between deliveries: a cause names
 * the position of a committed record, and receiving that record is what satisfies it (D115).
 *
 * <p>Configuration carrying the guarantee is fixed here: {@code exactly_once_v2},
 * {@code read_committed}, and no automatic offset reset.
 *
 * @see Parsley#start
 */
package io.github.tobyjamesclements.parsley;
