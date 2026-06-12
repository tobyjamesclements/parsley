package io.parsley;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CausalRecordTest {

    private static final TestClock POSITION = TestClock.empty().advance("input-0", 3L);

    @Test
    void headersAreDefensivelyCopied() {
        List<CausalHeader> source = new ArrayList<>();
        source.add(new CausalHeader("h1", new byte[]{1}));
        CausalRecord<String, String, TestClock> record =
                new CausalRecord<>("k", "v", 0L, source, null, POSITION, "input-0@3");

        source.add(new CausalHeader("h2", new byte[]{2}));

        assertEquals(1, record.headers().size());
        assertEquals("h1", record.headers().getFirst().key());
    }

    @Test
    void headersListIsImmutable() {
        CausalRecord<String, String, TestClock> record =
                new CausalRecord<>("k", "v", 0L, List.of(), null, POSITION, "input-0@3");

        assertThrows(UnsupportedOperationException.class,
                () -> record.headers().add(new CausalHeader("h", new byte[0])));
    }

    @Test
    void dependenciesEmptyWhenNoEncodedDependencies() {
        CausalRecord<String, String, TestClock> record =
                new CausalRecord<>("k", "v", 0L, List.of(), null, POSITION, "input-0@3");

        assertTrue(record.dependencies().isEmpty());
    }

    @Test
    void dependenciesPresentWhenEncoded() {
        byte[] encoded = {1, 2, 3};
        CausalRecord<String, String, TestClock> record =
                new CausalRecord<>("k", "v", 0L, List.of(), encoded, POSITION, "input-0@3");

        assertArrayEquals(encoded, record.dependencies().orElseThrow());
    }
}
