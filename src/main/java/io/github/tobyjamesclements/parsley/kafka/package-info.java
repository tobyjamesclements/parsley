/**
 * The Kafka Streams adapter.
 *
 * <p>One process becomes one Kafka Streams application. Sources and sinks carry raw bytes,
 * and application serdes are applied inside the processor after the delivery decision, so a
 * payload that fails to decode cannot bypass the gate.
 *
 * <p>Topic identity and partition width are resolved at start, and identity is checked again
 * at each task initialisation. Positions are meaningful only against the log they were
 * assigned in, so a topic recreated under a name this process has state for is a reason to
 * refuse. Nothing is asked of the broker between deliveries: a cause names the position of a
 * committed record, and receiving that record is what satisfies it (D115).
 *
 * <p>Configuration carrying the guarantee is fixed here: {@code exactly_once_v2},
 * {@code read_committed}, and no automatic offset reset.
 */
package io.github.tobyjamesclements.parsley.kafka;
