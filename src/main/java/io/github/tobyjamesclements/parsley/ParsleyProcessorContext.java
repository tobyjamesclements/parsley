package io.github.tobyjamesclements.parsley;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A Decorator (GoF) over the real {@link ProcessorContext} handed to a decorating causal processor's
 * delegate, stamping the current outbound vector timestamp ({@code completeness ∪ ownOutputs} —
 * {@link ParsleyChannels#stamp()}) onto every forwarded record's headers and delegating everything
 * else verbatim.
 *
 * <p>This is what makes outgoing messages causally stamped without the user stamping anything by
 * hand: within a topology {@code forward} is internal routing, and Kafka Streams sinks propagate a
 * {@link Record}'s headers onto the produced {@code ProducerRecord}, so dependencies stamped here
 * ride the headers all the way to the output topic.
 *
 * <p>The stamp itself is attached by {@link ParsleyCausalBroadcast#broadcast} — the single stamping
 * site every outbound record passes through, protocol markers included — which reads the completeness
 * <strong>live</strong> at stamp time, so a forward during record admission sees the post-admit
 * completeness and a forward from a punctuator sees the completeness as of fire time. Stamping is
 * idempotent — any existing {@link ParsleyHeader#CAUSAL_DEPENDENCIES} header is replaced, never
 * accumulated — and never mutates the incoming record's headers (a fresh header set is built and
 * applied via {@link Record#withHeaders}).
 *
 * <p><strong>The one-arg {@link #forward(Record)} targets every name in {@code sinkNodeNames}
 * explicitly — it never broadcasts.</strong> A stage's processor node may have more than one child
 * (its business sink(s) and, occasionally, an incompatibly-typed sibling such as a raw-bytes side
 * topic); the zero-arg {@code ProcessorContext.forward} sends to <em>every</em> child of the current
 * node unconditionally, so a business record broadcast that way could also reach an incompatible
 * sibling and throw a runtime {@code ClassCastException} on its serializer. Addressing every forward
 * by name is therefore a correctness requirement, not a style choice, the moment a stage has more than
 * one child of any kind.
 *
 * <p>Note: scheduled punctuators forward through this same proxy, so their forwards are stamped with
 * no special-casing. Punctuators must only <em>read</em> the completeness (never advance it), which
 * preserves the causal-broadcast core's persist-frontier-before-forward invariant on the punctuator path.
 *
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class ParsleyProcessorContext<KOut, VOut> implements ProcessorContext<KOut, VOut> {

    private final ProcessorContext<KOut, VOut> delegate;
    // The L2 module whose broadcast() request attaches the stamp. Wildcard-typed: its generics are the
    // stage's INPUT key/value types, irrelevant to stamping this context's outbound KOut/VOut records
    // (broadcast() is generic per record).
    private final ParsleyCausalBroadcast<?, ?> broadcast;
    private final Supplier<Optional<RecordMetadata>> deliveredMetadata;
    // Every business sink this stage declared, or empty to fall back to the plain broadcast forward()
    // Kafka Streams itself provides. Non-empty only when a second, incompatibly-typed child has been
    // added as a sibling of this processor's business sink(s) — see the class javadoc — so the
    // overwhelming majority of callers keep today's exact broadcast behaviour with zero change.
    private final List<String> sinkNodeNames;
    // Counts business forward() calls since the last resetForwardCount(); read by ParsleyProcessor
    // to detect non-emitting delegate invocations and emit a watermark in their place.
    private int forwardCount = 0;

    ParsleyProcessorContext(ProcessorContext<KOut, VOut> delegate,
                             ParsleyCausalBroadcast<?, ?> broadcast,
                             Supplier<Optional<RecordMetadata>> deliveredMetadata,
                             List<String> sinkNodeNames) {
        this.delegate = delegate;
        this.broadcast = broadcast;
        this.deliveredMetadata = deliveredMetadata;
        this.sinkNodeNames = sinkNodeNames;
    }

    /**
     * Resets the business-forward counter to zero. Called by {@link ParsleyProcessor} before
     * invoking {@code delegate.process(...)} for each delivered record, so the count reflects
     * only the forwards the delegate makes for that specific input.
     */
    void resetForwardCount() {
        forwardCount = 0;
    }

    /**
     * Returns the number of business {@link #forward} calls made since the last
     * {@link #resetForwardCount()}. Zero means the delegate did not forward anything for the
     * current input record, triggering watermark emission in {@link ParsleyProcessor}.
     */
    int forwardCount() {
        return forwardCount;
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record) {
        forwardCount++;
        Record<K, V> stamped = stamp(record);
        if (sinkNodeNames.isEmpty()) {
            delegate.forward(stamped);
            return;
        }
        for (String name : sinkNodeNames) {
            delegate.forward(stamped, name);
        }
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record, String childName) {
        forwardCount++;
        delegate.forward(stamp(record), childName);
    }

    private <K extends KOut, V extends VOut> Record<K, V> stamp(Record<K, V> record) {
        return broadcast.broadcast(record);
    }

    // --- everything below delegates verbatim to the real context ---

    @Override
    public String applicationId() {
        return delegate.applicationId();
    }

    @Override
    public TaskId taskId() {
        return delegate.taskId();
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        // While the decorator is delivering a (possibly buffered-then-drained) record, report that
        // record's true source coordinate rather than the Streams record that triggered delivery.
        Optional<RecordMetadata> delivered = deliveredMetadata.get();
        return delivered.isPresent() ? delivered : delegate.recordMetadata();
    }

    @Override
    public Serde<?> keySerde() {
        return delegate.keySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return delegate.valueSerde();
    }

    @Override
    public File stateDir() {
        return delegate.stateDir();
    }

    @Override
    public StreamsMetrics metrics() {
        return delegate.metrics();
    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
        return delegate.getStateStore(name);
    }

    @Override
    public Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
        return delegate.schedule(interval, type, callback);
    }

    @Override
    public Cancellable schedule(Instant startTime, Duration interval, PunctuationType type, Punctuator callback) {
        return delegate.schedule(startTime, interval, type, callback);
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return delegate.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(String prefix) {
        return delegate.appConfigsWithPrefix(prefix);
    }

    @Override
    public long currentSystemTimeMs() {
        return delegate.currentSystemTimeMs();
    }

    @Override
    public long currentStreamTimeMs() {
        return delegate.currentStreamTimeMs();
    }
}
