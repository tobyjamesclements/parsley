package io.github.tobyjamesclements.parsley;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Exports the codec's test vectors for the decoder written in {@code parsley-jepsen} from
 * {@code docs/wire-format.md} alone: encodings the codec produces with the frontier each
 * decodes to, and every catalogued malformation with the diagnosis family it must draw.
 * The valid vectors are the frozen golden bytes, the varint spelling and the round-trip
 * shapes {@code CausesCodecTest} pins; the malformed ones are
 * {@link CausesMalformationVectors#all()}.
 */
final class JepsenCodecVectors {
    private JepsenCodecVectors() {
    }

    static Map<Object, Object> vectors() {
        List<Object> valid = new ArrayList<>();
        valid.add(valid("empty frontier", Causes.none()));
        UUID low = new UUID(0x0102030405060708L, 0x090A0B0C0D0E0F10L);
        UUID highBit = new UUID(0xF102030405060708L, 0x090A0B0C0D0E0F10L);
        valid.add(valid("frozen golden bytes: two partitions in one group, high-bit topic last",
                Causes.of(Map.of(new Channel(low, 2), 41L, new Channel(low, 5), 7L, new Channel(highBit, 0), 9L))));
        valid.add(valid("varint partition 300 spelled AC 02", Causes.of(Map.of(new Channel(new UUID(1, 1), 300), 7L))));
        valid.add(valid("three channels over two topics",
                Causes.of(Map.of(new Channel(new UUID(1, 1), 0), 41L, new Channel(new UUID(1, 2), 3), 7L,
                        new Channel(new UUID(1, 1), 6), 3L))));
        Map<Channel, Long> wide = new LinkedHashMap<>();
        for (int topic = 0; topic < 4; topic++) {
            UUID id = new UUID(0x8000000000000000L + topic * 0x1000L, 0xFFFFFFFFFFFFFFFFL - topic);
            for (int partition = 0; partition < 200; partition += 37) {
                wide.put(new Channel(id, partition), (long) partition * 1_000_003L + topic);
            }
        }
        wide.put(new Channel(new UUID(0, 1), 130), Long.MAX_VALUE - 1);
        valid.add(valid("wide frontier: high-bit topic ids, multi-byte partitions, a maximal position",
                Causes.of(wide)));
        List<Object> malformed = new ArrayList<>();
        for (CausesMalformationVectors.Vector vector : CausesMalformationVectors.all()) {
            malformed.add(JepsenEdn.map("family", vector.family(), "label", vector.label(),
                    "bytes", vector.bytes() == null ? null : HexFormat.of().formatHex(vector.bytes()),
                    "diagnosis", vector.diagnosisFragment()));
        }
        return JepsenEdn.map("header-key", CausesCodec.HEADER_KEY, "valid", valid, "malformed", malformed);
    }

    private static Map<Object, Object> valid(String label, Causes causes) {
        Map<Object, Object> decoded = new LinkedHashMap<>();
        causes.byChannel().forEach((channel, position) -> decoded.put(JepsenExport.channel(channel), position));
        return JepsenEdn.map("label", label, "bytes", HexFormat.of().formatHex(CausesCodec.encode(causes)),
                "causes", decoded);
    }

    static void writeTo(Path path) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        Files.writeString(path, JepsenEdn.write(vectors()) + "\n", StandardCharsets.UTF_8);
    }
}
