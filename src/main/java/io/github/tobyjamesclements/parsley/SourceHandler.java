package io.github.tobyjamesclements.parsley;

import org.apache.kafka.streams.processor.api.Record;

/**
 * The user logic for one source topic of a stage, invoked once per causally delivered record.
 * Each source carries its own handler, typed independently — a stage's inputs need not share
 * types.
 *
 * <p>By the time a record reaches its handler, every cause the stage consumes has already
 * been delivered here. Emit through the {@link StageContext}; outputs are stamped and routed
 * by the stage.
 *
 * <p>One handler instance is shared by every task and stream thread running the stage — keep
 * handlers stateless (or thread-safe). Per-task mutable state belongs in state stores
 * declared via {@code Builder.stores} and reached through {@link StageContext#store}, which
 * is scoped to the executing task.
 */
@FunctionalInterface
public interface SourceHandler<K, V> {

    void handle(Record<K, V> record, StageContext context);
}
