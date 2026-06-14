package io.parsley.stream;

import io.parsley.VectorClock;
import io.parsley.internal.Attributes;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link ProcessorContext} that wraps the real context handed to a decorating causal processor's
 * delegate, stamping the current causal frontier onto every forwarded record's headers and
 * delegating everything else verbatim.
 *
 * <p>This is what makes outgoing messages causally stamped without the user touching a
 * {@code CausalProducer}: within a topology {@code forward} is internal routing, and Kafka Streams
 * sinks propagate a {@link Record}'s headers onto the produced {@code ProducerRecord}, so a clock
 * stamped here rides the headers all the way to the output topic.
 *
 * <p>The frontier is read <strong>live</strong> through a {@link Supplier} at stamp time, so a
 * forward during record admission sees the post-admit frontier and a forward from a punctuator sees
 * the frontier as of fire time. Stamping is idempotent — any existing
 * {@link Attributes#VECTOR_CLOCK} header is removed before the current clock is added — and never
 * mutates the incoming record's headers (a fresh header set is built and applied via
 * {@link Record#withHeaders}).
 *
 * <p>Note: scheduled punctuators forward through this same proxy, so their forwards are stamped with
 * no special-casing. Punctuators must only <em>read</em> the frontier (never advance it), which
 * preserves the engine's persist-frontier-before-forward invariant on the punctuator path.
 *
 * @param <KOut> the forwarded key type
 * @param <VOut> the forwarded value type
 */
final class StampingProcessorContext<KOut, VOut> implements ProcessorContext<KOut, VOut> {

    private final ProcessorContext<KOut, VOut> delegate;
    private final Supplier<VectorClock> frontier;

    StampingProcessorContext(ProcessorContext<KOut, VOut> delegate, Supplier<VectorClock> frontier) {
        this.delegate = delegate;
        this.frontier = frontier;
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record) {
        delegate.forward(stamp(record));
    }

    @Override
    public <K extends KOut, V extends VOut> void forward(Record<K, V> record, String childName) {
        delegate.forward(stamp(record), childName);
    }

    private <K extends KOut, V extends VOut> Record<K, V> stamp(Record<K, V> record) {
        Headers stamped = new RecordHeaders();
        for (Header header : record.headers()) {
            if (!header.key().equals(Attributes.VECTOR_CLOCK)) {
                stamped.add(header);
            }
        }
        stamped.add(new RecordHeader(Attributes.VECTOR_CLOCK, frontier.get().toBytes()));
        return record.withHeaders(stamped);
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
        return delegate.recordMetadata();
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
