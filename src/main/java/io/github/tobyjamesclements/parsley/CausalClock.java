package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

/**
 * Tracks a plain Kafka client's causal past, so the client can stamp the records it produces.
 *
 * <p>A running view built from the records the client observed and its own acknowledged sends.
 *
 * <p>{@link #observe} folds a consumed record, its coordinate and any clock it carries.
 * {@link #recordProduced} folds one of this producer's own acknowledged sends. {@link #stamp}
 * writes the current clock onto an outbound record's headers.
 *
 * <p>A sequential producer has its own prior sends claimed by the time it stamps the next one,
 * provided it waits for each send's acknowledgement or calls {@code producer.flush()} between
 * causally ordered sends.
 *
 * <p>A producer that stamps nothing claims nothing. Not thread-safe. Use one instance per
 * producing thread.
 */
public final class CausalClock {

    private final TopicIds topicIds;
    private final VectorClock clock = new VectorClock();

    /**
     * Creates a clock that resolves topic identity through {@code admin}.
     *
     * <p>The admin client is owned and closed by the caller. It is consulted lazily, and each
     * topic name is resolved once and cached.
     *
     * @param admin resolves topic names to the stable UUIDs the clock keys channels by
     */
    public CausalClock(Admin admin) {
        this.topicIds = TopicIds.fromAdmin(admin);
    }

    /**
     * Creates a clock resolving through a given resolver, for tests and the simulator.
     *
     * @param topicIds resolves topic names to the stable UUIDs the clock keys channels by
     */
    CausalClock(TopicIds topicIds) {
        this.topicIds = topicIds;
    }

    /**
     * Folds one consumed record, its own coordinate and any clock it carries.
     *
     * @param record the consumed record
     */
    public void observe(ConsumerRecord<?, ?> record) {
        VectorClock carried = CausalHeaders.read(record.headers());
        if (carried != null) clock.mergeMax(carried);
        clock.advanceTo(channel(record.topic(), record.partition()), record.offset());
    }

    /**
     * Folds one of this producer's own acknowledged sends. A send with no offset is ignored.
     *
     * @param metadata the acknowledged send's metadata
     */
    public void recordProduced(RecordMetadata metadata) {
        if (metadata.hasOffset()) {
            clock.advanceTo(channel(metadata.topic(), metadata.partition()), metadata.offset());
        }
    }

    /**
     * Stamps the current clock onto an outbound record's headers.
     *
     * @param headers the outbound record's headers, written in place
     */
    public void stamp(Headers headers) {
        CausalHeaders.write(headers, clock.copy());
    }

    private Channel channel(String topic, int partition) {
        return new Channel(topicIds.resolve(topic).id(), partition);
    }
}
