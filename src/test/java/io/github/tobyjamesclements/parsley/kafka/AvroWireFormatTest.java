package io.github.tobyjamesclements.parsley.kafka;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import io.github.tobyjamesclements.parsley.api.Channel;
import io.github.tobyjamesclements.parsley.api.Effects;
import io.github.tobyjamesclements.parsley.api.ProcessDefinition;
import io.github.tobyjamesclements.parsley.core.CausesCodec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * SPEC Safety 5 names Avro with the Confluent Schema Registry wire format explicitly: a reader with the
 * application's codecs alone must decode parsley-sent messages. This test runs an Avro serde producing exactly that
 * wire format — magic byte 0x0, schema id int32, Avro binary body — through a parsley process and asserts the output
 * value is byte-for-byte what the serde produces, decodable with the serde alone, headers ignored.
 */
class AvroWireFormatTest {

    private static final Schema SCHEMA = SchemaBuilder.record("Order").fields()
            .requiredString("item")
            .requiredInt("qty")
            .endRecord();
    private static final int SCHEMA_ID = 7;

    /** The Confluent wire format: 1 magic byte (0), 4-byte big-endian schema id, then Avro binary encoding. */
    static final class SchemaRegistryFormatSerde implements Serde<GenericRecord> {
        @Override
        public Serializer<GenericRecord> serializer() {
            return (topic, record) -> {
                if (record == null) {
                    return null;
                }
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    out.write(0);
                    out.write(ByteBuffer.allocate(4).putInt(SCHEMA_ID).array());
                    var encoder = EncoderFactory.get().binaryEncoder(out, null);
                    new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, encoder);
                    encoder.flush();
                    return out.toByteArray();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return (topic, bytes) -> {
                if (bytes == null) {
                    return null;
                }
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(bytes);
                    if (buffer.get() != 0) {
                        throw new IllegalArgumentException("not schema-registry wire format");
                    }
                    int schemaId = buffer.getInt();
                    assertEquals(SCHEMA_ID, schemaId);
                    var decoder = DecoderFactory.get().binaryDecoder(
                            bytes, buffer.position(), buffer.remaining(), null);
                    return new GenericDatumReader<GenericRecord>(SCHEMA).read(null, decoder);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
        }
    }

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void avroSchemaRegistryFormatPassesThroughByteExact() {
        Serde<GenericRecord> avro = new SchemaRegistryFormatSerde();
        Channel<String, GenericRecord> in = Channel.of("avro-in", Serdes.String(), avro);
        Channel<String, GenericRecord> out = Channel.of("avro-out", Serdes.String(), avro);
        ProcessDefinition definition = ProcessDefinition.named("avro")
                .receives(in, (delivery, state) -> {
                    GenericRecord doubled = new GenericData.Record(SCHEMA);
                    doubled.put("item", delivery.value().get("item"));
                    doubled.put("qty", (int) delivery.value().get("qty") * 2);
                    return Effects.builder().send(out, delivery.key(), doubled).build();
                })
                .sends(out)
                .build();

        Map<String, TopicInfo> topics = Map.of(
                "avro-in", new TopicInfo(new UUID(7, 1), 1),
                "avro-out", new TopicInfo(new UUID(7, 2), 1));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(ProcessTopology.build(
                definition, topics, (received, hints, frontier) -> io.github.tobyjamesclements.parsley.core.PositionFacts.EMPTY,
                Duration.ofSeconds(1)), props);

        GenericRecord order = new GenericData.Record(SCHEMA);
        order.put("item", "widget");
        order.put("qty", 3);
        byte[] valueIn = avro.serializer().serialize("avro-in", order);
        driver.createInputTopic("avro-in", new ByteArraySerializer(), new ByteArraySerializer())
                .pipeInput(new TestRecord<>("k".getBytes(), valueIn));

        TestRecord<byte[], byte[]> record = driver.createOutputTopic(
                "avro-out", new ByteArrayDeserializer(), new ByteArrayDeserializer()).readRecord();

        // The wire value is exactly what the application's serde produces for the emitted record: a reader with the
        // serde alone — no knowledge of parsley — decodes it; the causal metadata rides in a header it never reads.
        GenericRecord expected = new GenericData.Record(SCHEMA);
        expected.put("item", "widget");
        expected.put("qty", 6);
        assertArrayEquals(avro.serializer().serialize("avro-out", expected), record.value());
        GenericRecord decoded = avro.deserializer().deserialize("avro-out", record.value());
        assertEquals(6, decoded.get("qty"));
        assertNotNull(record.headers().lastHeader(CausesCodec.HEADER_KEY));
    }
}
