package io.github.tobyjamesclements.parsley.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The cluster-facing helpers the integration suites share. One copy, so a tightening of
 * the held-premise check or the wait discipline reaches every suite that asserts it.
 */
final class ClusterTestSupport {

    private ClusterTestSupport() {
    }

    static void produce(String bootstrapServers, String topic, String key, String value,
                        RecordHeader... headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            for (RecordHeader header : headers) {
                record.headers().add(header);
            }
            producer.send(record);
            producer.flush();
        }
    }

    static UUID topicId(Admin admin, String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
        return new UUID(description.topicId().getMostSignificantBits(),
                description.topicId().getLeastSignificantBits());
    }

    static void await(String what, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted awaiting " + what);
            }
        }
        throw new AssertionError("timed out awaiting " + what);
    }

    /**
     * Establishes the held premise by evidence rather than elapsed time: the process's
     * committed read position on the effect's topic reaches past it — so it was fed and its
     * step committed — while nothing has been delivered. A fixed sleep would let a slow
     * runner pass the emptiness assertion without the effect ever having been fed.
     */
    static void awaitFedAndHeld(Admin admin, String groupId, String topic,
                                ConcurrentLinkedQueue<String> delivered) {
        await("the effect to be fed and committed", () -> {
            try {
                var committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                        .get(30, TimeUnit.SECONDS);
                var offset = committed.get(new TopicPartition(topic, 0));
                return offset != null && offset.offset() >= 1;
            } catch (Exception e) {
                return false; // transient admin failure: not evidence either way, poll again
            }
        }, Duration.ofSeconds(60));
        assertEquals(List.of(), List.copyOf(delivered), "the effect must be held while its cause is missing");
    }
}
