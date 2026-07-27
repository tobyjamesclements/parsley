package io.github.tobyjamesclements.parsley.kafka;

import io.github.tobyjamesclements.parsley.core.Channel;
import io.github.tobyjamesclements.parsley.core.Clock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

/**
 * The edge ops for plain Kafka clients: a running clock a producer stamps outbound records
 * with, built from records it observed ({@link #observe}) and its own acknowledged sends
 * ({@link #recordProduced}).
 *
 * <p>A sequential producer that waits for each send's acknowledgement (or uses
 * {@code producer.flush()} between causally ordered sends) has its own prior sends claimed by
 * the time it stamps the next one. Not thread-safe; one instance per producing thread.
 */
public final class EdgeClock {

    private final TopicIds topicIds;
    private final Clock clock = new Clock();

    public EdgeClock(TopicIds topicIds) {
        this.topicIds = topicIds;
    }

    /** Folds one consumed record: its own coordinate and any clock it carries. */
    public void observe(ConsumerRecord<?, ?> record) {
        Clock carried = CausalHeaders.read(record.headers());
        if (carried != null) clock.mergeMax(carried);
        clock.advanceTo(channel(record.topic(), record.partition()), record.offset());
    }

    /** Folds one of this producer's own acknowledged sends. */
    public void recordProduced(RecordMetadata metadata) {
        if (metadata.hasOffset()) {
            clock.advanceTo(channel(metadata.topic(), metadata.partition()), metadata.offset());
        }
    }

    /** Stamps the current clock onto an outbound record's headers. */
    public void stamp(Headers headers) {
        CausalHeaders.write(headers, clock.copy());
    }

    /** A snapshot of the current clock (for inspection or persistence by the caller). */
    public Clock snapshot() {
        return clock.copy();
    }

    private Channel channel(String topic, int partition) {
        return new Channel(topicIds.resolve(topic).id(), partition);
    }
}
