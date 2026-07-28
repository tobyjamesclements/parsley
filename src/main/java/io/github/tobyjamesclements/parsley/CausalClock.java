package io.github.tobyjamesclements.parsley;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

/**
 * The clock a plain Kafka client stamps records with: a running view of the client's causal
 * past, built from the records it observed and its own acknowledged sends.
 *
 * <p>Usage is three verbs. {@link #observe} folds a consumed record (its coordinate and any
 * clock it carries); {@link #recordProduced} folds one of this producer's own acknowledged
 * sends; {@link #stamp} writes the current clock onto an outbound record's headers. A
 * sequential producer that waits for each send's acknowledgement (or calls
 * {@code producer.flush()} between causally ordered sends) has its own prior sends claimed by
 * the time it stamps the next one.
 *
 * <p>A producer that stamps nothing claims nothing — safe, and the reason adoption needs no
 * flag day. Not thread-safe; one instance per producing thread.
 */
public final class CausalClock {

    private final TopicIds topicIds;
    private final VectorClock clock = new VectorClock();

    /**
     * @param admin resolves topic identity (names to stable UUIDs); owned and closed by the
     *     caller, consulted lazily and cached per name
     */
    public CausalClock(Admin admin) {
        this.topicIds = TopicIds.fromAdmin(admin);
    }

    CausalClock(TopicIds topicIds) {
        this.topicIds = topicIds;
    }

    /** Folds one consumed record: its own coordinate and any clock it carries. */
    public void observe(ConsumerRecord<?, ?> record) {
        VectorClock carried = CausalHeaders.read(record.headers());
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

    private Channel channel(String topic, int partition) {
        return new Channel(topicIds.resolve(topic).id(), partition);
    }
}
