package io.github.tobyjamesclements.parsley;

/**
 * One delivered tick: a record the stage runtime emits to the stage's own tick channel at the
 * declared interval and consumes back through the causal gate like any other record. The
 * timestamp is the emitting task's wall-clock time at emission, so wall time reaches pure
 * logic only as data on a delivered record.
 *
 * <p>A tick's stamp claims the emitting task's consumed history at the moment of emission — a
 * consistent cut in the vector-clock sense — so any consumer of an emission a tick causes is
 * gated behind everything the tick's task had delivered when it fired. A record, so tick
 * logic is testable with plain equality: construct one directly with the timestamp under
 * test.
 *
 * @param timestamp the emitting task's wall-clock time at emission, in epoch milliseconds
 */
public record Tick(long timestamp) {}
