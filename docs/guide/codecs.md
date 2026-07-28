# Codecs and Avro

A `Codec<T>` is two pure functions between a value and its bytes, and it is the only
serialization vocabulary in the functional core: user logic sees decoded values, and Kafka
serdes stay at the imperative edge, bridged in through `Codec.fromSerde`. Serialization runs
exactly once on each side, inside the stage. Inbound bytes are held verbatim while a record
is gated and decoded with the source topic's codecs at delivery; emissions are encoded with
their topic's codecs at the stamping site, so the clock travels with the exact bytes it
claims.

## The codec contract

- **Never null.** Null keys and null values pass through the runtime untouched — a tombstone
  stays a tombstone — so `encode` and `decode` may assume their argument is present and need
  no null branch.
- **Total on real data, and inverse.** `decode` must accept every byte sequence `encode`
  produces (and every byte sequence already on the topic), and `decode(encode(v))` must
  equal `v` under the type's equality, which is what makes pure logic testable with plain
  values.
- **Failure is loud.** A codec that cannot decode should throw, not guess: an exception from
  `decode` or `encode` fails the task, the transaction aborts, and the retry refetches. A
  lenient codec that returns a placeholder converts a wiring bug into silent data corruption.

## Built-in codecs and writing your own

`Codec.utf8()`, `Codec.int64()`, and `Codec.bytes()` cover strings, longs, and raw bytes.
Everything else is `Codec.of(encoder, decoder)`. A fixed-layout value needs a dozen lines:

```java
record Reading(long sensor, double value) {}

Codec<Reading> readingCodec = Codec.of(
        r -> ByteBuffer.allocate(16).putLong(r.sensor()).putDouble(r.value()).array(),
        b -> {
            ByteBuffer buf = ByteBuffer.wrap(b);
            return new Reading(buf.getLong(), buf.getDouble());
        });
```

For JSON, wrap a mapper and rethrow its checked exceptions unchecked — the runtime treats
any codec exception as a task failure, which is the behaviour you want:

```java
static <T> Codec<T> json(ObjectMapper mapper, Class<T> type) {
    return Codec.of(
            v -> {
                try {
                    return mapper.writeValueAsBytes(v);
                } catch (JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            },
            b -> {
                try {
                    return mapper.readValue(b, type);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
}
```

## Key codecs are identity

A key's encoded bytes are load-bearing three times over. They choose the partition (the
producer-default murmur2 hash of the encoded key), they key the stage's per-key fold state,
and they are the unit of the co-partitioning agreement across topics and producers. A key
codec must therefore be **canonical and stable**: the same logical key produces the same
bytes on every producer, in every version, forever. `Codec.utf8()`, `Codec.int64()`, and
`Codec.bytes()` all qualify.

Registry-framed encodings do not: an Avro, JSON Schema, or Protobuf serializer prefixes the
payload with a schema id, so re-registering the key schema changes the bytes of every key —
which re-partitions the topic and orphans existing fold state. Keep registry-framed codecs
on values, and give keys a primitive or hand-rolled canonical codec.

## Bridging Kafka serdes

`Codec.fromSerde(serde, topic)` turns any configured `Serde<T>` into a codec. The topic name
is fixed at bridge time because serdes take it per call — schema-registry serdes derive
their subject from it — so bridge one codec per topic rather than sharing one across topics.
The caller configures the serde before bridging and owns its lifecycle. The bridged codec
inherits the codec contract's null rule: the serde's null path is never exercised, because
nulls do not reach codecs.

## Avro

Avro arrives through the serde bridge. Add Confluent's serde artifact (it lives in the
Confluent Maven repository, not Central):

```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-streams-avro-serde</artifactId>
    <version>8.0.0</version>
</dependency>
```

Configure a serde per value type, bridge it per topic, and declare the topic once:

```java
SpecificAvroSerde<Reading> readingSerde = new SpecificAvroSerde<>();
readingSerde.configure(Map.of("schema.registry.url", "http://registry:8081"), false);

Topic<String, Reading> c1 = Topic.of("c1", Codec.utf8(), Codec.fromSerde(readingSerde, "c1"));
```

The `false` in `configure` marks it a value serde, and the bridge's topic name gives the
registry subject (`c1-value` under the default naming strategy). `GenericAvroSerde` works
identically for `GenericRecord` values, as do Confluent's JSON Schema and Protobuf serdes —
the bridge does not care which framing sits behind the serde.

What Avro-specific behaviour to expect at runtime:

- **The registry is a runtime dependency of both codec sites.** Encoding at the stamping
  site registers or looks up the writer schema; decoding at delivery fetches the writer
  schema by the id in the frame. A registry outage therefore fails the task rather than
  producing or accepting unreadable bytes, and the transaction's retry refetches.
- **Held records and schema evolution compose cleanly.** A record can wait in the hold queue
  for its causes; it waits as the original bytes and is decoded only at delivery. The frame
  carries the writer-schema id, so a record written under an older schema decodes under
  whatever compatible reader schema is in force when it is finally released. Avro's usual
  compatibility rules apply unchanged; Parsley adds no constraint of its own.
- **Keep Avro off keys.** The schema-id frame makes registry-encoded keys non-canonical —
  see [key codecs are identity](#key-codecs-are-identity).
