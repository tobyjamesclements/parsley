package io.parsley;

import java.util.Map;

record TestClock(Map<String, Long> positions) implements VectorClock<TestClock> {

    static TestClock empty() {
        return new TestClock(Map.of());
    }

    TestClock advance(String partition, long position) {
        return new TestClock(VectorClocks.advance(positions, partition, position));
    }

    @Override
    public boolean satisfiedBy(TestClock frontier) {
        return VectorClocks.satisfied(positions, frontier.positions());
    }

    @Override
    public TestClock merge(TestClock other) {
        return new TestClock(VectorClocks.merge(positions, other.positions()));
    }
}
