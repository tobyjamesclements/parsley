package io.parsley.kafka;

import io.parsley.VectorClockSerialiser;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Compact binary {@link VectorClockSerialiser} for {@link KafkaVectorClock}.
 *
 * <h2>Wire format</h2>
 * <pre>
 * [int   count]
 * for each entry:
 *   [short topicLen] [byte[] topic (UTF-8)] [int partition] [long offset]
 * </pre>
 *
 * <p>Registered automatically via {@link java.util.ServiceLoader}.
 */
public final class KafkaVectorClockSerialiser implements VectorClockSerialiser<KafkaVectorClock> {

    @Override
    public byte[] serialise(KafkaVectorClock clock) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            Map<TopicPartition, Long> positions = clock.positions();
            dos.writeInt(positions.size());
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                byte[] topicBytes = entry.getKey().topic().getBytes(StandardCharsets.UTF_8);
                dos.writeShort(topicBytes.length);
                dos.write(topicBytes);
                dos.writeInt(entry.getKey().partition());
                dos.writeLong(entry.getValue());
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Serialisation failed", e);
        }
    }

    @Override
    public KafkaVectorClock deserialise(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int count = dis.readInt();
            Map<TopicPartition, Long> positions = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                int topicLen = dis.readUnsignedShort();
                byte[] topicBytes = new byte[topicLen];
                dis.readFully(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                int partition = dis.readInt();
                long offset = dis.readLong();
                positions.put(new TopicPartition(topic, partition), offset);
            }
            return new KafkaVectorClock(positions);
        } catch (IOException e) {
            throw new IllegalStateException("Deserialisation failed", e);
        }
    }
}
