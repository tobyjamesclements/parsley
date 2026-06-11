package io.parsley.kafka.internal;

import io.parsley.VectorClock;
import io.parsley.VectorClockSerialiser;
import io.parsley.kafka.KafkaVectorClock;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
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
public final class KafkaVectorClockSerialiser implements VectorClockSerialiser {

    @Override
    public byte[] serialise(VectorClock clock) {
        KafkaVectorClock kvc = (KafkaVectorClock) clock;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            Map<TopicPartition, Long> positions = kvc.positions();
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
    public VectorClock deserialise(byte[] bytes) {
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
