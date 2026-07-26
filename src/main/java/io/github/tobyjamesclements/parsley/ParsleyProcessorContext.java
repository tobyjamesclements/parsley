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
 * A Decorator over the real {@link ProcessorContext} handed to a causal processor's delegate,
 * stamping the current outbound timestamp ({@link ParsleyChannels#stamp()}) onto every forwarded
 * record's headers and delegating everything else verbatim. This is what stamps outgoing records
 * without the user doing anything by hand: Kafka Streams sinks propagate a {@link Record}'s headers
 * onto the produced {@code ProducerRecord}, so the dependencies ride to the output topic.
 *
 * <p>The stamp is attached by {@link ParsleyCausalBroadcast#broadcast}, the single stamping site,
 * which reads the completeness live at forward time, so a forward during admission sees the post-admit
 * value and a forward from a punctuator sees it as of fire time. Stamping is idempotent (any existing
 * {@link ParsleyHeader#CAUSAL_CLOCK} header is replaced) and never mutates the incoming record.
 *
 * <p>The one-arg {@link #forward(Record)} addresses every name in {@code sinkNodeNames} explicitly
 * rather than broadcasting, because the zero-arg {@code forward} reaches every child of the node,
 * which could send a business record to an incompatibly-typed sibling and throw
 * {@code ClassCastException} on its serializer. Punctuators forward through this same proxy and must
 * only read the completeness, never advance it, preserving the persist-frontier-before-forward
 * ordering on the punctuator path.
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
    // to detect non-emitting delegate invocations and emit a null message in their place.
    private int forwardCount = 0;
    // Fails the forward fast if the topic-identity watch has detected a mid-run recreation —
    // a punctuator-driven forward is the one stamped path that does not pass through
    // ParsleyProcessor.process()'s own check first.
    private final Runnable identityCheck;

    ParsleyProcessorContext(ProcessorContext<KOut, VOut> delegate,
                             ParsleyCausalBroadcast<?, ?> broadcast,
                             Supplier<Optional<RecordMetadata>> deliveredMetadata,
                             List<String> sinkNodeNames,
                             Runnable identityCheck) {
        this.delegate = delegate;
        this.broadcast = broadcast;
        this.deliveredMetadata = deliveredMetadata;
        this.sinkNodeNames = sinkNodeNames;
        this.identityCheck = identityCheck;
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
     * current input record, triggering null-message emission in {@link ParsleyProcessor}.
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
        identityCheck.run();
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
