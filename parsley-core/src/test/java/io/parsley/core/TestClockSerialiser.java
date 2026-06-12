package io.parsley.core;

import io.parsley.VectorClockSerialiser;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Text-format serialiser for {@link TestClock}; registered via META-INF/services. */
public final class TestClockSerialiser implements VectorClockSerialiser<TestClock> {

    @Override
    public byte[] serialise(TestClock clock) {
        return clock.positions().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(";"))
                .getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TestClock deserialise(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8);
        if (text.isEmpty()) {
            return TestClock.empty();
        }
        Map<String, Long> positions = new HashMap<>();
        for (String entry : text.split(";")) {
            int eq = entry.indexOf('=');
            if (eq < 0) {
                throw new IllegalStateException("Malformed TestClock encoding: " + text);
            }
            positions.put(entry.substring(0, eq), Long.parseLong(entry.substring(eq + 1)));
        }
        return new TestClock(Map.copyOf(positions));
    }
}
